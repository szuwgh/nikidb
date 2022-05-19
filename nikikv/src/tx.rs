use crate::bucket::Bucket;
use crate::db::DBImpl;
use crate::page::Meta;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::ptr::null;
use std::sync::{Arc, RwLock, Weak};

pub(crate) type Txid = u64;

pub(crate) struct Tx(pub(crate) Arc<TxImpl>);

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
}

pub(crate) struct TxImpl {
    dbImpl: Arc<DBImpl>,
    pub(crate) root: RefCell<Bucket>,
    pub(crate) meta: Meta,
}

impl TxImpl {
    pub(crate) fn build(db: Arc<DBImpl>) -> TxImpl {
        let tx = Self {
            dbImpl: db.clone(),
            root: RefCell::new(Bucket::new(0, false, Weak::new())),
            meta: db.meta(),
        };
        tx.root.borrow_mut().ibucket = tx.meta.root.clone();
        tx
    }

    pub(crate) fn db(&self) -> Arc<DBImpl> {
        self.dbImpl.clone()
    }
}
