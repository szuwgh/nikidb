use crate::bucket::IBucket;
use crate::error::{NKError, NKResult};
use crate::freelist::FreeList;
use crate::node::NodeImpl;
use crate::page::{FreeListPageFlag, LeafPageFlag, Meta, MetaPageFlag, OwnerPage, Page, Pgid};
use crate::tx::{Tx, TxImpl, Txid};
use crate::{magic, version};
use lock_api::{RawMutex, RawRwLock};
use parking_lot::{Mutex, RwLock};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::prelude::FileExt;
use std::ptr::null;
use std::sync::Arc;

const MAX_MAP_SIZE: u64 = 0x0FFF_FFFF; //256TB

const MAX_MMAP_STEP: u64 = 1 << 30;

fn get_page_size() -> usize {
    page_size::get()
    // return 256;
}

#[derive(Clone)]
pub struct DB(Arc<DBImpl>);

impl DB {
    fn begin_rwtx(&self) -> Tx {
        unsafe {
            self.0.rw_lock.raw().lock();
        }
        let mut tx = Tx(Arc::new(TxImpl::build(true, self.0.clone())));
        tx.init();
        *(self.0.rwtx.try_write().unwrap()) = Some(tx.clone());
        let txs = self.0.txs.read();
        let minid = txs
            .iter()
            .map(|tx| tx.id())
            .min()
            .unwrap_or(0xFFFF_FFFF_FFFF_FFFF);
        if minid > 0 {
            self.0.freelist.try_write().unwrap().release(minid - 1);
        }
        drop(txs);
        tx
    }

    fn begin_tx(&self) -> Tx {
        unsafe {
            self.0.mmap.raw().lock_shared();
        }
        let mut tx = Tx(Arc::new(TxImpl::build(false, self.0.clone())));
        tx.init();
        self.0.txs.try_write().unwrap().push(tx.clone());
        tx
    }

    fn begin(&self, writable: bool) -> Tx {
        if writable {
            self.begin_rwtx()
        } else {
            self.begin_tx()
        }
    }

    pub fn open(db_path: &str, options: Options) -> NKResult<DB> {
        DBImpl::open(db_path, options)
    }

    pub fn update<'a>(
        &self,
        mut handler: Box<dyn FnMut(&mut Tx) -> NKResult<()> + 'a>,
    ) -> NKResult<()> {
        let mut t = self.begin(true);
        if let Err(e) = handler(&mut t) {
            t.rollback()?;
            return Err(e);
        }
        t.commit()?;
        Ok(())
    }

    pub fn view<'a>(
        &self,
        mut handler: Box<dyn FnMut(&mut Tx) -> NKResult<()> + 'a>,
    ) -> NKResult<()> {
        let mut t = self.begin(false);
        if let Err(e) = handler(&mut t) {
            t.rollback()?;
            return Err(e);
        }
        t.rollback()?;
        Ok(())
    }

    fn print(&self) {
        self.0.print();
    }
}

pub(crate) struct DBImpl {
    file: RwLock<File>,
    pub(crate) mmap: RwLock<MmapUtil>,
    page_pool: RwLock<Vec<Vec<u8>>>,
    pub(crate) freelist: RwLock<FreeList>,
    rwtx: RwLock<Option<Tx>>,
    txs: RwLock<Vec<Tx>>,
    pub(crate) rw_lock: Mutex<()>,
}

pub(crate) struct MmapUtil {
    pub(crate) page_size: usize,
    mmap: Option<memmap::Mmap>,
    meta0: *const Meta,
    meta1: *const Meta,
    db_size: u64,
}

unsafe impl Send for MmapUtil {}
unsafe impl Sync for MmapUtil {}

impl Default for MmapUtil {
    fn default() -> Self {
        Self {
            page_size: 0,
            mmap: None,
            meta0: null(),
            meta1: null(),
            db_size: 0,
        }
    }
}

impl MmapUtil {
    fn mmap_size(&self, mut size: u64) -> NKResult<u64> {
        for i in 15..=30 {
            if size <= 1 << i {
                return Ok(1 << i);
            }
        }
        if size > MAX_MAP_SIZE {
            return Err(NKError::Unexpected("mmap too large".to_string()));
        }
        let remainder = size % MAX_MMAP_STEP;
        if remainder > 0 {
            size += MAX_MAP_SIZE - remainder;
        };
        let page_size = self.page_size as u64;
        if (size % page_size) != 0 {
            size = ((size / page_size) + 1) * page_size;
        };
        if size > MAX_MAP_SIZE {
            size = MAX_MAP_SIZE
        };
        Ok(size)
    }

    pub(crate) fn set_mmap(&mut self, file: &File, mut min_size: u64) -> NKResult<()> {
        let mut mmap_opts = memmap::MmapOptions::new();

        let mut size = file.metadata().map_err(|e| NKError::DBOpenFail(e))?.len();
        if size < min_size {
            size = min_size;
        }
        min_size = self.mmap_size(size)?;
        drop(self.mmap.as_deref());
        let nmmap = unsafe {
            mmap_opts
                .offset(0)
                .len(min_size as usize)
                .map(file)
                .map_err(|e| format!("mmap failed: {}", e))?
        };
        let meta0 = self.page_in_buffer(&nmmap, 0).meta();
        let meta1 = self.page_in_buffer(&nmmap, 1).meta();
        meta0.validate()?;
        meta1.validate()?;
        self.meta0 = meta0;
        self.meta1 = meta1;
        self.mmap.replace(nmmap);
        self.db_size = min_size as u64;
        Ok(())
    }

    pub(crate) fn meta(&self) -> Meta {
        unsafe {
            let mut metaA = self.meta0;
            let mut metaB = self.meta1;
            if (*self.meta1).txid > (*self.meta0).txid {
                metaA = self.meta1;
                metaB = self.meta0;
            }
            if (*metaA).validate().is_ok() {
                return *metaA.clone();
            }
            if (*metaB).validate().is_ok() {
                return *metaB.clone();
            }
            panic!("niki.DB.meta(): invalid meta pages")
        }
    }

    pub(crate) fn page_in_buffer_mut<'a>(&self, buf: &'a mut [u8], id: u32) -> &'a mut Page {
        Page::from_buf_mut(&mut buf[(id as usize * self.page_size)..])
    }

    pub(crate) fn page_in_buffer<'a>(&self, buf: &'a [u8], id: u32) -> &'a Page {
        Page::from_buf(&buf[(id as usize * self.page_size) as usize..])
    }

    pub(crate) fn page(&self, id: Pgid) -> *const Page {
        self.page_in_buffer(&self.mmap.as_ref().unwrap(), id as u32)
    }
}

#[derive(Clone, Copy)]
pub struct Options {
    no_grow_sync: bool,

    read_only: bool,

    mmap_flags: u32,

    initial_mmap_size: u64,
}

pub static DEFAULT_OPTIONS: Options = Options {
    no_grow_sync: false,
    read_only: false,
    mmap_flags: 0,
    initial_mmap_size: 0,
};

impl DBImpl {
    pub fn open(db_path: &str, options: Options) -> NKResult<DB> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_path)
            .map_err(|e| NKError::DBOpenFail(e))?;
        let size = f.metadata().map_err(|e| NKError::DBOpenFail(e))?.len();
        let mut db = Self::new(f);
        if size == 0 {
            db.init()?;
        } else {
            let mut buf = vec![0; get_page_size()];
            db.file
                .try_read()
                .unwrap()
                .read_at(&mut buf, 0)
                .map_err(|_e| ("can't read to file", _e))?;
            let m = db.mmap.try_read().unwrap().page_in_buffer(&buf, 0).meta();
            m.validate()?;
            db.mmap.try_write().unwrap().page_size = m.page_size;
        }
        db.mmap
            .try_write()
            .unwrap()
            .set_mmap(&db.file.try_read().unwrap(), options.initial_mmap_size)?;
        db.freelist.try_write().unwrap().read(unsafe {
            &*db.mmap
                .try_read()
                .unwrap()
                .page(db.mmap.try_read().unwrap().meta().freelist)
        });
        Ok(DB(Arc::new(db)))
    }

    fn print(&self) {
        let meta = self.meta();
        let root = meta.root.root;
        let p = unsafe { &*self.page(root) };
        let mut node = NodeImpl::new().build();
        node.read(p);
        node.print(self);
    }

    fn new(file: File) -> DBImpl {
        Self {
            file: RwLock::new(file),
            mmap: RwLock::new(MmapUtil::default()),
            page_pool: RwLock::new(Vec::new()),
            freelist: RwLock::new(FreeList::default()),
            rwtx: RwLock::new(None),
            txs: RwLock::new(Vec::new()),
            rw_lock: Mutex::new(()),
        }
    }

    fn init(&mut self) -> NKResult<()> {
        let page_size = get_page_size();
        self.mmap.try_write().unwrap().page_size = page_size;
        let mut buf: Vec<u8> = vec![0; 4 * page_size];
        for i in 0..2 {
            let p = self
                .mmap
                .try_write()
                .unwrap()
                .page_in_buffer_mut(&mut buf, i);
            p.id = i as Pgid;
            p.flags = MetaPageFlag;

            let m = p.meta_mut();
            m.magic = magic;
            m.version = version;
            m.page_size = page_size;
            m.freelist = 2;
            m.root = IBucket::new(3);
            m.pgid = 4;
            m.txid = i as Txid;
            m.checksum = m.sum64();
        }

        let mut p = self
            .mmap
            .try_write()
            .unwrap()
            .page_in_buffer_mut(&mut buf, 2);
        p.id = 2;
        p.flags = FreeListPageFlag;
        p.count = 0;

        p = self
            .mmap
            .try_write()
            .unwrap()
            .page_in_buffer_mut(&mut buf, 3);
        p.id = 3;
        p.flags = LeafPageFlag;
        p.count = 0;

        self.write_at(&buf, 0)?;
        self.sync()?;

        Ok(())
    }

    pub(crate) fn write_at(&self, buf: &[u8], pos: u64) -> NKResult<()> {
        self.file
            .try_write()
            .unwrap()
            .write_at(buf, pos)
            .map_err(|_e| ("can't write to file", _e))?;
        Ok(())
    }

    pub(crate) fn sync(&self) -> NKResult<()> {
        self.file
            .try_write()
            .unwrap()
            .flush()
            .map_err(|_e| ("can't flush file", _e))?;
        Ok(())
    }

    pub(crate) fn allocate(&self, count: usize) -> NKResult<OwnerPage> {
        let mut page = if count == 1 {
            if let Some(p) = self.page_pool.try_write().unwrap().pop() {
                OwnerPage::from_vec(p)
            } else {
                OwnerPage::from_vec(vec![0u8; get_page_size()])
            }
        } else {
            OwnerPage::from_vec(vec![0u8; get_page_size() * count])
        };

        let p = page.to_page_mut();
        p.overflow = (count - 1) as u32;

        p.id = self.freelist.try_write().unwrap().allocate(count);

        if p.id != 0 {
            return Ok(page);
        }
        p.id = (*(self.rwtx.try_write().unwrap().as_ref().unwrap().0))
            .meta
            .borrow()
            .pgid;
        let minsz = (((p.id + count as Pgid + 1) as usize) * get_page_size()) as u64;
        if minsz >= self.mmap.try_read().unwrap().db_size {
            self.mmap
                .try_write()
                .unwrap()
                .set_mmap(&self.file.try_read().unwrap(), minsz)?;
        }

        (*(self.rwtx.try_write().unwrap().as_ref().unwrap().0))
            .meta
            .borrow_mut()
            .pgid += count as Pgid;

        Ok(page)
    }

    pub(crate) fn page(&self, id: Pgid) -> *const Page {
        self.mmap.try_read().unwrap().page(id)
    }

    pub(crate) fn get_page_size(&self) -> usize {
        self.mmap.try_read().unwrap().page_size
    }

    pub(crate) fn meta(&self) -> Meta {
        self.mmap.try_read().unwrap().meta()
    }

    pub(crate) fn page_in_buffer_mut<'a>(&self, buf: &'a mut [u8], id: u32) -> &'a mut Page {
        self.mmap.try_write().unwrap().page_in_buffer_mut(buf, id)
    }

    pub(crate) fn remove_tx(&self, tx: Tx) {
        let mut txs = self.txs.try_write().unwrap();
        let index = txs.iter().position(|t| Arc::ptr_eq(&tx.0, &t.0)).unwrap();
        txs.remove(index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_db_mmap() {
        let db = DBImpl::open("./test.db", DEFAULT_OPTIONS).unwrap();
        let mut tx = unsafe { (&*(db.0.mmap.try_read().unwrap().meta0)).txid };
        let mut buf = vec![0; 4096];
        let page =
            db.0.mmap
                .try_write()
                .unwrap()
                .page_in_buffer_mut(&mut buf, 0);
        let meta = page.meta_mut();
        meta.txid = 2;
        db.0.write_at(&buf, 0).unwrap();
        db.0.sync().unwrap();
        tx = unsafe { (&*(db.0.mmap.try_read().unwrap().meta0)).txid };
    }

    #[test]
    fn test_tx_create_bucket() {
        let mut db = DBImpl::open("./test.db", DEFAULT_OPTIONS).unwrap();
        let mut tx1 = db.begin_rwtx();
        tx1.create_bucket("888".as_bytes()).unwrap();
        tx1.commit();
        db.print();
        let mut tx2 = db.begin_rwtx();
        let b = tx2.bucket("888".as_bytes()).unwrap();
        b.put(b"001", b"aaa");
        b.put(b"002", b"bbb");
        b.put(b"003", b"ccc");
        b.put(b"004", b"ddd");
        tx2.commit();
        db.print();
        println!("------------------");
        let mut tx4 = db.begin_rwtx();
        let b = tx4.bucket("888".as_bytes()).unwrap();
        let v1 = b.get(b"001");
        println!("{:?}", v1);
        let v2 = b.get(b"004");
        println!("{:?}", v2);
        println!("------------------");
        let mut tx3 = db.begin_rwtx();
        let b = tx3.bucket("888".as_bytes()).unwrap();
        b.put(b"005", b"aaa");
        b.put(b"006", b"bbb");
        b.put(b"007", b"ccc");
        b.put(b"008", b"ddd");
        tx3.rollback();
        db.print();
    }

    #[test]
    fn test_tx_delete() {
        let mut db = DBImpl::open("./test.db", DEFAULT_OPTIONS).unwrap();
        let mut tx1 = db.begin_rwtx();
        tx1.create_bucket("888".as_bytes()).unwrap();
        tx1.commit();
        db.print();
        let mut tx2 = db.begin_rwtx();
        let b = tx2.bucket("888".as_bytes()).unwrap();
        b.put(b"001", b"aaa");
        b.put(b"002", b"bbb");
        b.put(b"003", b"ccc");
        b.put(b"004", b"ddd");
        tx2.commit();
        db.print();

        let mut tx3 = db.begin_rwtx();
        let b = tx3.bucket("888".as_bytes()).unwrap();
        b.delete(b"001");
        tx3.commit();
        db.print();
    }

    #[test]
    fn test_db_print() {
        let mut db = DBImpl::open("./test.db", DEFAULT_OPTIONS).unwrap();
        db.print();
    }
}
