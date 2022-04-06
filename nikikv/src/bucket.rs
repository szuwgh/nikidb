use crate::page::Pgid;

pub struct Bucket {
    root: Pgid,
}

impl Bucket {
    pub fn new(root: Pgid) -> Bucket {
        Self { root: root }
    }
}
