use crate::bucket::Bucket;
use crate::db::DB;
use std::rc::{Rc, Weak};
pub struct Tx<'t> {
    db: &'t DB,
    root: Bucket,
}

impl<'t> Tx<'t> {
    pub fn build(db: &DB) -> Tx {
        let tx = Self {
            db: db,
            root: Bucket::new(0, Weak::new()),
        };
        tx
    }
}
