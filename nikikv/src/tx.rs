use crate::bucket::Bucket;
use crate::db::DBImpl;
use crate::error::{NKError, NKResult};
use crate::page::{Meta, OwnerPage, Page, Pgid};

use lock_api::{RawMutex, RawRwLock};

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};
pub(crate) type Txid = u64;

pub(crate) struct Tx(pub(crate) Arc<TxImpl>);

unsafe impl Sync for Tx {}
unsafe impl Send for Tx {}

impl Tx {
    pub(crate) fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }

    pub(crate) fn init(&mut self) {
        let r = self.0.clone();
        r.root.borrow_mut().weak_tx = Arc::downgrade(&self.0);
        if r.writable {
            r.meta.borrow_mut().txid += 1;
        }
    }

    pub(crate) fn create_bucket(&mut self, name: &[u8]) -> NKResult<&mut Bucket> {
        self.0
            .root
            .borrow_mut()
            .create_bucket(name)
            .map(|m| unsafe { &mut *m })
    }

    pub(crate) fn bucket(&mut self, name: &[u8]) -> NKResult<&mut Bucket> {
        self.0
            .root
            .borrow_mut()
            .bucket(name)
            .map(|m| unsafe { &mut *m })
    }

    pub(crate) fn id(&self) -> Txid {
        self.0.meta.borrow().txid
    }

    fn tx(&self) -> Arc<TxImpl> {
        self.0.clone()
    }

    pub fn rollback(&mut self) -> NKResult<()> {
        self._rollback()
    }

    pub(crate) fn _rollback(&mut self) -> NKResult<()> {
        let db = self.0.db();
        if self.0.writable {
            db.freelist
                .try_write()
                .unwrap()
                .rollback(self.tx().meta.borrow().txid)?;
            let free_page = db.page(db.meta().freelist);
            db.freelist
                .try_write()
                .unwrap()
                .reload(unsafe { &*free_page })?;
        }
        self.close();
        Ok(())
    }

    pub fn commit(&mut self) -> NKResult<()> {
        let tx = self.tx();
        let db = tx.db();

        tx.root
            .borrow_mut()
            .rebalance(db.get_page_size() as usize)?;
        if let Err(e) = tx.root.borrow_mut().spill(self.0.clone()) {
            self._rollback()?;
            return Err(e);
        }
        //回收旧的freelist列表

        db.freelist
            .try_write()
            .unwrap()
            .free(tx.meta.borrow().txid, unsafe {
                &*db.page(tx.meta.borrow().freelist)
            });

        let size = db.freelist.try_read().unwrap().size();
        let mut p = match db.allocate(size / db.get_page_size() as usize + 1) {
            Ok(_p) => _p,
            Err(e) => {
                self._rollback()?;
                return Err(e);
            }
        };

        let page = p.to_page_mut();
        db.freelist.try_write().unwrap().write(page);

        tx.meta.borrow_mut().freelist = page.id;
        tx.pages.borrow_mut().insert(page.id, p);

        tx.meta.borrow_mut().root.root = tx.root.borrow().ibucket.root;
        println!("meta root pgid:{}", tx.root.borrow().ibucket.root);
        //write dirty page
        if let Err(e) = tx.write() {
            self._rollback()?;
            return Err(e);
        }

        //write meta
        if let Err(e) = tx.write_meta() {
            self._rollback()?;
            return Err(e);
        }

        self.close();

        Ok(())
    }

    pub(crate) fn close(&self) {
        if !self.0.writable {
            self.0.db().remove_tx(self.clone());
            unsafe {
                self.0.db().mmap.raw().unlock_shared();
            }
        } else {
            unsafe {
                self.0.db().rw_lock.raw().unlock();
            }
        }
    }
}

pub(crate) struct TxImpl {
    pub(crate) writable: bool,
    dbImpl: Arc<DBImpl>,
    pub(crate) root: RefCell<Bucket>,
    pub(crate) meta: RefCell<Meta>,
    pub(crate) pages: RefCell<HashMap<Pgid, OwnerPage>>,
}

impl TxImpl {
    pub(crate) fn build(writable: bool, db: Arc<DBImpl>) -> TxImpl {
        let tx = Self {
            writable: writable,
            dbImpl: db.clone(),
            root: RefCell::new(Bucket::new(0, Weak::new())),
            meta: RefCell::new(db.meta()),
            pages: RefCell::new(HashMap::new()),
        };
        tx.root.borrow_mut().ibucket = tx.meta.borrow().root.clone();
        tx
    }

    pub(crate) fn db(&self) -> Arc<DBImpl> {
        self.dbImpl.clone()
    }

    pub(crate) fn write(&self) -> NKResult<()> {
        let mut pages = self
            .pages
            .borrow_mut()
            .drain()
            .map(|(k, v)| (k, v))
            .collect::<Vec<(u64, OwnerPage)>>();
        pages.sort_by(|a, b| a.0.cmp(&b.0));

        for p in pages.iter() {
            let page = p.1.to_page();
            let page_size = self.dbImpl.get_page_size();
            let offset = page.id * page_size as u64;
            println!("write page id:{}", page.id);
            self.db().write_at(&p.1.value, offset)?;
        }
        self.db().sync()?;
        Ok(())
    }

    pub(crate) fn write_meta(&self) -> NKResult<()> {
        let page_size = self.db().get_page_size();
        let mut buf = vec![0u8; page_size as usize];
        let id = {
            let p = self.db().page_in_buffer_mut(&mut buf, 0);
            self.meta.borrow_mut().write(p);
            p.id
        };
        println!("meta id:{}, meta page size{}", id, page_size);
        self.db().write_at(&buf, id * page_size as u64)?;

        self.db().sync()?;
        Ok(())
    }
}
