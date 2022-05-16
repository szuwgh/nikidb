use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Index;
use std::ptr::null;

use crate::cursor::Cursor;
use crate::error::{NKError, NKResult};
use crate::node::{Node, NodeImpl};
use crate::page::{BucketLeafFlag, Page, Pgid};
use crate::tx::TxImpl;
use std::mem::size_of;
use std::rc::Rc;
use std::sync::{Arc, Weak};
pub(crate) const BucketHeaderSize: usize = size_of::<IBucket>();

pub(crate) struct Bucket {
    pub(crate) ibucket: IBucket,
    nodes: HashMap<Pgid, Node>, //tx: Tx,
    pub(crate) weak_tx: Weak<TxImpl>,
    rootNode: Option<Node>,
    page: *const Page,
}

#[derive(Clone)]
pub(crate) enum PageNode {
    Page(*const Page),
    Node(Node),
}

impl From<Node> for PageNode {
    fn from(n: Node) -> Self {
        PageNode::Node(n)
    }
}

impl Bucket {
    pub(crate) fn new(root: Pgid, is_leaf: bool, tx: Weak<TxImpl>) -> Bucket {
        Self {
            ibucket: IBucket {
                root: root,
                sequence: 0,
            },
            nodes: HashMap::new(),
            weak_tx: tx,
            rootNode: None, //NodeImpl::new(is_leaf),
            page: null(),
        }
    }

    pub(crate) fn create_bucket(&mut self, key: &[u8]) -> NKResult<Bucket> {
        let mut c = self.cursor();
        let item = c.seek(key)?;
        if item.key().eq(&Some(key)) {
            if item.flags() & BucketLeafFlag != 0 {
                return Err(NKError::ErrBucketExists(
                    String::from_utf8_lossy(key).into(),
                ));
            }
        }

        let bucket = Bucket::new(0, true, self.weak_tx.clone());
        let value = bucket.write();

        Ok(bucket)
    }

    fn cursor(&mut self) -> Cursor {
        Cursor::new(self)
        //    let item =
        // Cursor { bucket: self }
    }

    pub(crate) fn put(key: &[u8], value: &[u8]) {}

    pub(crate) fn get(key: &[u8]) {}

    pub(crate) fn page_node(&self, id: Pgid) -> NKResult<PageNode> {
        if let Some(node) = self.nodes.get(&id) {
            return Ok(PageNode::Node(node.clone()));
        }
        let page = self.tx().unwrap().db().page(id);
        Ok(PageNode::Page(page))
    }

    pub(crate) fn tx(&self) -> Option<Arc<TxImpl>> {
        self.weak_tx.upgrade()
    }

    pub(crate) fn write(&self) -> Vec<u8> {
        let n = self.rootNode.as_ref().unwrap().borrow();
        let size = n.size();
        let mut value = vec![0u8; BucketHeaderSize + size];

        let bucket = value.as_ptr() as *mut IBucket;
        unsafe { *bucket = *&self.ibucket }
        let p = Page::from_buf_mut(&mut value[BucketHeaderSize..]);
        n.write(p);
        value
    }

    pub(crate) fn node(&mut self, pgid: Pgid, parent: Weak<NodeImpl>) -> Node {
        if let Some(node) = self.nodes.get(&pgid) {
            return node.clone();
        }

        let mut n = NodeImpl::new(self).parent(parent.clone()).build();
        if self.page.is_null() {
            unsafe {
                let p = &self.tx().unwrap().db().page(pgid);
                n.borrow_mut().read(unsafe { &**p });
            }
        }

        self.nodes.insert(pgid, n.clone());
        n
    }
}

#[derive(Clone, Copy)]
pub(crate) struct IBucket {
    pub(crate) root: Pgid,
    sequence: u64,
}

impl IBucket {
    pub(crate) fn new(root: Pgid) -> IBucket {
        Self {
            root: root,
            sequence: 0,
        }
    }
}
