use crate::bucket::Bucket;
use crate::db::DBImpl;
use crate::error::{NKError, NKResult};
use crate::page::{Meta, OwnerPage, Page, Pgid};
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ptr::null;
use std::sync::{Arc, RwLock, Weak};

pub(crate) type Txid = u64;

pub(crate) struct Tx(pub(crate) Arc<TxImpl>);

// unsafe impl Sync for Tx {}
// unsafe impl Send for Tx {}

impl Tx {
    pub(crate) fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }

    pub(crate) fn init(&mut self) {
        let r = self.0.clone();
        r.root.borrow_mut().weak_tx = Arc::downgrade(&self.0);
    }

    pub(crate) fn create_bucket(&mut self, name: &[u8]) {
        self.0.root.borrow_mut().create_bucket(name);
    }

    fn tx(&self) -> Arc<TxImpl> {
        self.0.clone()
    }

    pub(crate) fn commit(&mut self) -> NKResult<()> {
        self.tx().root.borrow_mut().spill(self.0.clone());
        Ok(())
    }
    // pub(crate) fn allocate(&self, count: usize) -> NKResult<OwnerPage> {
    //     let mut page = (*(self.0.db())).borrow_mut().allocate(count)?;
    //     Ok(page)
    // }

    // pub(crate) fn page(&self, id: Pgid) -> *const Page {
    //     let db = (*(self.0.db())).borrow();
    //     let p = db.page(id);
    //     unsafe { &*p }
    // }
}

pub(crate) struct TxImpl {
    dbImpl: RefCell<Arc<DBImpl>>,
    pub(crate) root: RefCell<Bucket>,
    pub(crate) meta: RefCell<Meta>,
    pub(crate) pages: RefCell<HashMap<Pgid, OwnerPage>>,
}

impl TxImpl {
    pub(crate) fn build(db: Arc<DBImpl>) -> TxImpl {
        let tx = Self {
            dbImpl: RefCell::new(db.clone()),
            root: RefCell::new(Bucket::new(0, Weak::new())),
            meta: RefCell::new(db.meta()),
            pages: RefCell::new(HashMap::new()),
        };
        tx.root.borrow_mut().ibucket = tx.meta.borrow().root.clone();
        tx
    }

    pub(crate) fn db(&self) -> Arc<DBImpl> {
        self.dbImpl.borrow().clone()
    }
}
