use std::borrow::BorrowMut;

use crate::bucket::{Bucket, PageNode};
use crate::error::NKResult;
use crate::page::{Page, PageFlag, Pgid};

pub(crate) struct Cursor<'a> {
    pub(crate) bucket: &'a Bucket,
    stack: Vec<ElemRef>,
}

#[derive(Clone)]
pub(crate) struct ElemRef {
    page_node: PageNode,
    index: i32,
}

impl ElemRef {
    fn is_leaf(&self) -> bool {
        match &self.page_node {
            PageNode::Node(n) => n.is_leaf,
            PageNode::Page(p) => unsafe { (*(*p)).flags == PageFlag::LeafPageFlag },
        }
    }
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(bucket: &'a Bucket) -> Cursor<'a> {
        Self {
            bucket: bucket,
            stack: Vec::new(),
        }
    }

    fn first(&mut self) {}

    fn last(&mut self) {}

    fn next(&mut self) {}

    fn prev(&mut self) {}

    fn delete(&mut self) {}

    pub fn seek(&mut self) {}

    fn search(&mut self, key: &[u8], id: Pgid) -> NKResult<()> {
        let page_node = self.bucket.page_node(id)?;
        let elem_ref = ElemRef {
            page_node: page_node,
            index: 0,
        };
        self.stack.push(elem_ref.clone());

        Ok(())
    }
}
