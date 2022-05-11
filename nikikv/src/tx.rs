use crate::bucket::Bucket;
use crate::db::DBImpl;
use std::cell::RefCell;
use std::sync::{Arc, RwLock, Weak};

pub(crate) struct Tx(pub(crate) Arc<TxImpl>);

impl Tx {
    pub(crate) fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }

    pub(crate) fn init(&mut self) {
        let r = self.0.clone();
        let mut bucket = r.root.borrow_mut();
        bucket.weak_tx = Arc::downgrade(&self.0);
    }
}

pub(crate) struct TxImpl {
    dbImpl: Arc<DBImpl>,
    pub(crate) root: RefCell<Bucket>,
}

impl TxImpl {
    pub(crate) fn build(db: Arc<DBImpl>) -> TxImpl {
        let tx = Self {
            dbImpl: db.clone(),
            root: RefCell::new(Bucket::new(0, false, Weak::new())),
        };
        tx
    }

    pub(crate) fn db(&self) -> Arc<DBImpl> {
        self.dbImpl.clone()
    }
}
