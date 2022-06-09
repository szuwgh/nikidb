use crate::cursor::Cursor;
use crate::error::{NKError, NKResult};
use crate::node::{Node, NodeImpl};
use crate::page::{BucketLeafFlag, OwnerPage, Page, Pgid};
use crate::tx::TxImpl;
use crate::u8_to_struct_mut;
use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::mem::size_of;
use std::ptr::{null, null_mut};
use std::rc::{Rc, Weak};
use std::sync::{Arc, Weak as ArcWeak};

pub(crate) const BucketHeaderSize: usize = size_of::<IBucket>();

const MAX_KEY_SIZE: usize = 32768;

const MAX_VALUE_SIZE: usize = (1 << 31) - 2;

const MIN_FILL_PERCENT: f64 = 0.1;
const MAX_FILL_PERCENT: f64 = 1.0;

const DEFAULT_FILL_PERCENT: f64 = 1.0;

pub(crate) struct Bucket {
    pub(crate) ibucket: IBucket,
    nodes: HashMap<Pgid, Node>,
    pub(crate) weak_tx: ArcWeak<TxImpl>,
    root_node: Option<Node>,
    page: Option<OwnerPage>, // inline page
    buckets: HashMap<Vec<u8>, Bucket>,

    pub(crate) fill_percent: f64,
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
            root_node: None, // Some(NodeImpl::new().leaf(true).build()),
            page: None,
            buckets: HashMap::new(),
            fill_percent: DEFAULT_FILL_PERCENT,
        }
    }

    pub(crate) fn bucket(&mut self, key: &[u8]) -> NKResult<*mut Bucket> {
        if let Some(bucket) = self.buckets.get_mut(key) {
            return Ok(bucket);
        }
        let value = {
            let mut c = self.cursor();
            let item = c.seek_item(key)?;
            if !key.eq(item.0.unwrap()) || (item.2 & BucketLeafFlag) == 0 {
                return Err(NKError::ErrBucketNotFound);
            }
            item.1.unwrap().to_vec()
        };
        let child = self.open_bucket(value)?;
        self.buckets.insert(key.to_vec(), child);
        if let Some(bucket) = self.buckets.get_mut(key) {
            return Ok(bucket);
        }

        return Err(NKError::ErrBucketNotFound);
    }

    fn open_bucket(&self, value: Vec<u8>) -> NKResult<Bucket> {
        let mut child = Bucket::new(0, self.weak_tx.clone());
        let ibucket = crate::u8_to_struct::<IBucket>(value.as_slice());
        child.ibucket = ibucket.clone();
        if child.ibucket.root == 0 {
            //inline page
            let page = &value[BucketHeaderSize..];
            child.page = Some(OwnerPage::from_vec(page.to_vec()));
        }
        Ok(child)
    }

    pub(crate) fn create_bucket(&mut self, key: &[u8]) -> NKResult<*mut Bucket> {
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
        bucket.root_node = Some(NodeImpl::new().leaf(true).build());
        let value = bucket.write();

        c.node()?.put(key, key, value.as_slice(), 0, BucketLeafFlag);

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
            return Err(NKError::IncompatibleValue);
        }
        c.node()?.put(key, key, value, 0, 0);
        Ok(())
    }

    pub(crate) fn get(&mut self, key: &[u8]) -> Option<&[u8]> {
        let mut c = self.cursor();
        let item = c.seek(key).unwrap();
        if Some(key) == item.0 && (item.2 & BucketLeafFlag) == 1 {
            return None;
        }
        item.1
    }

    pub(crate) fn page_node(&self, id: Pgid) -> NKResult<PageNode> {
        // inline page
        if self.ibucket.root == 0 {
            if id != 0 {
                panic!("inline bucket non-zero page access(2): {} != 0", id)
            }
            if let Some(n) = &self.root_node {
                return Ok(PageNode::Node(n.clone()));
            }
            if let Some(p) = &self.page {
                return Ok(PageNode::Page(p.to_page()));
            }
        }
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
        let n = self.root_node.as_ref().unwrap();
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

    pub(crate) fn node(&mut self, pgid: Pgid, parent: Option<Weak<RefCell<NodeImpl>>>) -> Node {
        if let Some(node) = self.nodes.get(&pgid) {
            return node.clone();
        }

        let mut n = if let Some(p) = parent {
            let parent_node = p.upgrade().unwrap();
            let n = NodeImpl::new().parent(p.clone()).build();
            (*parent_node).borrow_mut().children.push(n.clone());
            n
        } else {
            let n = NodeImpl::new().build();
            self.root_node = Some(n.clone());
            n
        };

        if self.page.is_none() {
            let p = self.tx().unwrap().db().page(pgid);
            n.read(unsafe { &*p });
        }
        self.nodes.insert(pgid, n.clone());
        n
    }

    fn inline_able(&self) {}

    pub(crate) fn spill(&mut self, atx: Arc<TxImpl>) -> NKResult<()> {
        for b in self.buckets.values() {
            // b.in
        }

        let mut root = self.root_node.as_ref().unwrap().clone();
        root.spill(atx, &self)?;
        let root_node = root.root(root.clone());
        self.ibucket.root = root_node.node().pgid;
        self.root_node = Some(root_node);
        Ok(())
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
