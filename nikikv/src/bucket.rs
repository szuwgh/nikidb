use std::collections::HashMap;

use crate::cursor::Cursor;
use crate::page::{Node, Pgid};

use crate::error::NKResult;

pub struct Bucket {
    bucket: IBucket,
    nodes: HashMap<Pgid, Node>, //tx: Tx,
}

impl Bucket {
    pub fn new(root: Pgid) {}

    pub fn create_bucket(&mut self) {
        let mut c = self.cursor();
    }

    fn cursor(&mut self) -> Cursor {
        Cursor { bucket: self }
    }

    pub fn put(key: &[u8], value: &[u8]) {}

    pub fn get(key: &[u8]) {}

    pub fn page_node(&self, id: Pgid) -> NKResult<()> {}

    pub fn value() {}
}

pub struct IBucket {
    root: Pgid,
    sequence: u64,
}

impl IBucket {
    pub fn new(root: Pgid) -> IBucket {
        Self {
            root: root,
            sequence: 0,
        }
    }
}
