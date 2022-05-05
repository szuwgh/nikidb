use crate::bucket::Bucket;
use crate::db::DB;
use std::rc::{Rc, Weak};
pub struct Tx {
    db: Arc<DB>,
    root: Bucket,
}

impl Tx {
    pub fn build(db: &DB) -> Tx {
        let tx = Self {
            db: db,
            root: Bucket::new(0, Weak::new()),
        };
        tx
    }
}
