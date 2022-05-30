use crate::cursor::Cursor;
use crate::error::{NKError, NKResult};
use crate::node::{Node, NodeImpl};
use crate::page::{BucketLeafFlag, OwnerPage, Page, Pgid};
use crate::tx::TxImpl;
use crate::u8_to_struct_mut;
use std::borrow::BorrowMut;
use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::mem::size_of;
use std::ops::Index;
use std::ptr::{null, null_mut};
use std::rc::{Rc, Weak};
use std::sync::{Arc, Weak as ArcWeak};

pub(crate) const BucketHeaderSize: usize = size_of::<IBucket>();

const MAX_KEY_SIZE: usize = 32768;

const MAX_VALUE_SIZE: usize = (1 << 31) - 2;

pub(crate) struct Bucket {
    pub(crate) ibucket: IBucket,
    nodes: HashMap<Pgid, Node>,
    pub(crate) weak_tx: ArcWeak<TxImpl>,
    root_node: Option<Node>,
    page: Option<OwnerPage>, // inline page
    buckets: HashMap<Vec<u8>, Rc<RefCell<Bucket>>>,
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
    pub(crate) fn new(root: Pgid, tx: ArcWeak<TxImpl>) -> Bucket {
        Self {
            ibucket: IBucket {
                root: root,
                sequence: 0,
            },
            nodes: HashMap::new(),
            weak_tx: tx,
            root_node: Some(NodeImpl::new(null_mut()).leaf(true).build()),
            page: None,
            buckets: HashMap::new(),
        }
    }

    pub(crate) fn bucket(&mut self, key: &[u8]) -> NKResult<Rc<RefCell<Bucket>>> {
        if let Some(bucket) = self.buckets.get_mut(key) {
            return Ok(bucket.clone());
        }
        let item = {
            let mut c = self.cursor();
            c.seek_item(key)?
        };

        if !key.eq(item.0.unwrap()) || (item.2 & BucketLeafFlag) == 0 {
            return Err(NKError::ErrBucketNotFound);
        }

        let value = item.1.unwrap().to_vec();
        let child = self.open_bucket(value)?;
        self.buckets.insert(key.to_vec(), child.clone());
        Ok(child.clone())
    }

    fn open_bucket(&mut self, value: Vec<u8>) -> NKResult<Rc<RefCell<Bucket>>> {
        let mut child = Bucket::new(0, self.weak_tx.clone());
        let ibucket = crate::u8_to_struct::<IBucket>(value.as_slice());
        child.ibucket = ibucket.clone();
        if child.ibucket.root == 0 {
            //inline page
            let page = &value[BucketHeaderSize..];
            child.page = Some(OwnerPage::from_vec(page.to_vec()));
        }
        Ok(Rc::new(RefCell::new(child)))
    }

    pub(crate) fn create_bucket(&mut self, key: &[u8]) -> NKResult<Rc<RefCell<Bucket>>> {
        let tx_clone = self.weak_tx.clone();
        let mut c = self.cursor();
        let item = c.seek(key)?;
        if item.key().eq(&Some(key)) {
            if item.flags() & BucketLeafFlag != 0 {
                return Err(NKError::ErrBucketExists(
                    String::from_utf8_lossy(key).into(),
                ));
            }
        }
        let mut bucket = Bucket::new(0, tx_clone); // root == 0 is inline bucket
        let value = bucket.write();

        (*c.node()?)
            .borrow_mut()
            .put(self, key, key, value.as_slice(), 0, BucketLeafFlag);

        self.bucket(key)
    }

    fn cursor(&mut self) -> Cursor {
        Cursor::new(self)
    }

    pub(crate) fn put(&mut self, key: &[u8], value: &[u8]) -> NKResult<()> {
        if key.len() == 0 {
            return Err(NKError::ErrKeyRequired);
        } else if key.len() > MAX_KEY_SIZE {
            return Err(NKError::ErrKeyTooLarge);
        } else if value.len() > MAX_VALUE_SIZE {
            return Err(NKError::ErrValueTooLarge);
        }

        let mut c = self.cursor();
        let item = c.seek(key)?;

        if Some(key) == item.0 && (item.2 & BucketLeafFlag) == 1 {
            return Err(NKError::ErrBucketNotFound);
        }
        (*c.node()?).borrow_mut().put(self, key, key, value, 0, 0);
        Ok(())
    }

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
        let n = self.root_node.as_ref().unwrap().borrow();
        let size = n.size();
        let mut value = vec![0u8; BucketHeaderSize + size];

        let bucket = value.as_ptr() as *mut IBucket;
        unsafe {
            *bucket = *&self.ibucket;
        }

        let p = Page::from_buf_mut(&mut value[BucketHeaderSize..]);
        n.write(p);
        value
    }

    pub(crate) fn node(&mut self, pgid: Pgid, parent: Weak<RefCell<NodeImpl>>) -> Node {
        if let Some(node) = self.nodes.get(&pgid) {
            return node.clone();
        }
        let n = NodeImpl::new(self).parent(parent.clone()).build();
        if self.page.is_none() {
            let p = self.tx().unwrap().db().page(pgid);
            (*n).borrow_mut().read(unsafe { &*p });
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
