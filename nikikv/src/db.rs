use crate::bucket::IBucket;
use crate::error::{NKError, NKResult};
use crate::freelist::FreeList;
use crate::page::{
    self, FreeListPageFlag, LeafPageFlag, Meta, MetaPageFlag, OwnerPage, Page, Pgid,
};
use crate::tx::{Tx, TxImpl, Txid};
use crate::{magic, version};
use page_size;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::prelude::FileExt;
use std::ptr::null;
use std::rc::Rc;
use std::sync::Arc;

const MAX_MAP_SIZE: u64 = 0x0FFF_FFFF; //256TB

const MAX_MMAP_STEP: u64 = 1 << 30;

fn get_page_size() -> usize {
    page_size::get()
}

pub struct DB(Arc<RefCell<DBImpl>>);

impl DB {
    fn begin(&self) -> Tx {
        let mut tx = Tx(Arc::new(TxImpl::build(self.0.clone())));
        tx.init();
        tx
    }
}

pub(crate) struct DBImpl {
    file: File,
    page_size: u32,
    mmap: Option<memmap::Mmap>,
    meta0: *const Meta,
    meta1: *const Meta,
    page_pool: Vec<Vec<u8>>,
    freelist: FreeList,
    db_size: usize,
    rwtx: Option<Tx>,
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
            println!("init db");
            db.init()?;
        } else {
            let mut buf = vec![0; 0x1000];
            db.file
                .read_at(&mut buf, 0)
                .map_err(|_e| ("can't read to file", _e))?;
            let m = db.page_in_buffer(&buf, 0).meta();
            m.validate()?;
            db.page_size = m.page_size;
            println!("read:checksum {}", m.checksum);
        }
        db.set_mmap(options.initial_mmap_size)?;
        db.freelist.read(unsafe { &*db.page(db.meta().freelist) });
        Ok(DB(Arc::new(RefCell::new(db))))
    }

    fn new(file: File) -> DBImpl {
        Self {
            file: file,
            page_size: 0,
            mmap: None,
            meta0: null(),
            meta1: null(),
            page_pool: Vec::new(),
            freelist: FreeList::default(),
            db_size: 0,
        }
    }

    fn init(&mut self) -> NKResult<()> {
        self.page_size = get_page_size() as u32;
        let mut buf: Vec<u8> = vec![0; 4 * self.page_size as usize];
        for i in 0..2 {
            let p = self.page_in_buffer_mut(&mut buf, i);
            p.id = i as Pgid;
            p.flags = MetaPageFlag;

            let m = p.meta_mut();
            m.magic = magic;
            m.version = version;
            m.page_size = self.page_size;
            m.freelist = 2;
            m.root = IBucket::new(3);
            m.pgid = 4;
            m.txid = i as Txid;
            m.checksum = m.sum64();
        }

        // write an empty freelist at page 3
        let mut p = self.page_in_buffer_mut(&mut buf, 2);
        p.id = 2;
        p.flags = FreeListPageFlag;
        p.count = 0;

        p = self.page_in_buffer_mut(&mut buf, 3);
        p.id = 3;
        p.flags = LeafPageFlag;
        p.count = 0;

        self.write_at(&mut buf, 0)?;
        self.sync()?;

        Ok(())
    }

    fn write_at(&mut self, buf: &mut [u8], pos: u64) -> NKResult<()> {
        self.file
            .write_at(buf, pos)
            .map_err(|_e| ("can't write to file", _e))?;
        Ok(())
    }

    fn sync(&mut self) -> NKResult<()> {
        self.file.flush().map_err(|_e| ("can't flush file", _e))?;
        Ok(())
    }

    fn page_in_buffer_mut<'a>(&mut self, buf: &'a mut [u8], id: u32) -> &'a mut Page {
        Page::from_buf_mut(&mut buf[(id * self.page_size) as usize..])
    }

    fn page_in_buffer<'a>(&self, buf: &'a [u8], id: u32) -> &'a Page {
        Page::from_buf(&buf[(id * self.page_size) as usize..])
    }

    pub(crate) fn page(&self, id: Pgid) -> *const Page {
        self.page_in_buffer(&self.mmap.as_ref().unwrap(), id as u32)
    }

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
        // If we've exceeded the max size then only grow up to the max size.
        if size > MAX_MAP_SIZE {
            size = MAX_MAP_SIZE
        };
        Ok(size)
    }

    pub(crate) fn allocate(&mut self, count: usize) -> NKResult<OwnerPage> {
        //
        let mut page = if count == 1 {
            if let Some(p) = self.page_pool.pop() {
                OwnerPage::from_vec(p)
            } else {
                OwnerPage::from_vec(vec![0u8; get_page_size()])
            }
        } else {
            OwnerPage::from_vec(vec![0u8; get_page_size() * count])
        };

        let p = page.to_page();
        p.overflow = (count - 1) as u32;

        p.id = self.freelist.allocate(count);
        if p.id != 0 {
            return Ok(page);
        }

        let minsz = ((p.id + count as Pgid + 1) as usize) * get_page_size();
        if minsz >= self.db_size {
            self.set_mmap(minsz as u64)?;
        }

        Ok(page)
    }

    pub(crate) fn set_mmap(&mut self, mut min_size: u64) -> NKResult<()> {
        let mut mmap_opts = memmap::MmapOptions::new();

        let mut size = self
            .file
            .metadata()
            .map_err(|e| NKError::DBOpenFail(e))?
            .len();
        println!("size:{}", size);
        if size < min_size {
            size = min_size;
        }
        min_size = self.mmap_size(size)?;
        println!("min_size:{}", min_size);
        drop(self.mmap.as_deref());
        let nmmap = unsafe {
            mmap_opts
                .offset(0)
                .len(min_size as usize)
                .map(&self.file)
                .map_err(|e| format!("mmap failed: {}", e))?
        };
        let meta0 = self.page_in_buffer(&nmmap, 0).meta();
        let meta1 = self.page_in_buffer(&nmmap, 1).meta();
        meta0.validate()?;
        meta1.validate()?;
        self.meta0 = meta0;
        self.meta1 = meta1;
        self.mmap = Some(nmmap);
        self.db_size = min_size as usize;
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

    pub fn update(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_db_open() {
        //
    }

    #[test]
    fn test_db_mmap() {
        let db = DBImpl::open("./test.db", DEFAULT_OPTIONS).unwrap();
        (*db.0).borrow_mut().set_mmap2(32769);
    }

    #[test]
    fn test_tx_create_bucket() {
        let db = DBImpl::open("./test.db", DEFAULT_OPTIONS).unwrap();
        let mut tx = db.begin();
        tx.create_bucket("aaa".as_bytes());
    }
}
