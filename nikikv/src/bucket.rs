use crate::cursor::Cursor;
use crate::error::{NKError, NKResult};
use crate::node::{Node, NodeImpl};
use crate::page::{BranchPageFlag, BucketLeafFlag, LeafPageElementSize, OwnerPage, Page, Pgid};
use crate::tx::TxImpl;

use std::cell::RefCell;
use std::collections::HashMap;
use std::mem::size_of;

use std::rc::Weak;
use std::sync::{Arc, Weak as ArcWeak};

pub(crate) const BucketHeaderSize: usize = size_of::<IBucket>();

const MAX_KEY_SIZE: usize = 32768;

const MAX_VALUE_SIZE: usize = (1 << 31) - 2;

pub(crate) const MIN_FILL_PERCENT: f64 = 0.1;

pub(crate) const MAX_FILL_PERCENT: f64 = 1.0;

const DEFAULT_FILL_PERCENT: f64 = 0.5;

pub(crate) struct Bucket {
    pub(crate) ibucket: IBucket,
    pub(crate) nodes: RefCell<HashMap<Pgid, Node>>,
    pub(crate) weak_tx: ArcWeak<TxImpl>,
    root_node: Option<Node>,
    page: Option<OwnerPage>, // inline page
    buckets: RefCell<HashMap<Vec<u8>, Bucket>>,

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
            nodes: RefCell::new(HashMap::new()),
            weak_tx: tx,
            root_node: None,
            page: None,
            buckets: RefCell::new(HashMap::new()),
            fill_percent: DEFAULT_FILL_PERCENT,
        }
    }

    pub(crate) fn bucket(&mut self, key: &[u8]) -> NKResult<*mut Bucket> {
        if let Some(bucket) = self.buckets.borrow_mut().get_mut(key) {
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
        self.buckets.borrow_mut().insert(key.to_vec(), child);
        if let Some(bucket) = self.buckets.borrow_mut().get_mut(key) {
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

    pub(crate) fn delete(&mut self, key: &[u8]) -> NKResult<()> {
        let mut c = self.cursor();
        let item = c.seek(key)?;
        if item.flags() & BucketLeafFlag != 0 {
            return Err(NKError::IncompatibleValue);
        }
        c.node()?.del(key);
        Ok(())
    }

    fn free(&mut self) -> NKResult<()> {
        if self.ibucket.root == 0 {
            return Ok(());
        }
        let tx = self.tx().unwrap();
        self.for_each_page_node(|page_node, _, bucket| match page_node {
            PageNode::Page(p) => tx
                .db()
                .freelist
                .try_write()
                .unwrap()
                .free(tx.meta.borrow().txid, unsafe { &**p }),
            PageNode::Node(n) => {
                n.free(bucket);
            }
        })?;
        self.ibucket.root = 0;
        Ok(())
    }

    fn for_each_page_node<F>(&mut self, mut f: F) -> NKResult<()>
    where
        F: FnMut(&mut PageNode, i32, &Bucket),
    {
        match &self.page {
            None => self._for_each_page_node(self.ibucket.root, 0, &mut f),
            Some(p) => {
                f(&mut PageNode::Page(p.to_page()), 0, self);
                Ok(())
            }
        }
    }

    fn _for_each_page_node<F>(&mut self, pgid: Pgid, depth: i32, f: &mut F) -> NKResult<()>
    where
        F: FnMut(&mut PageNode, i32, &Bucket),
    {
        let mut page_node = self.page_node(pgid)?;
        f(&mut page_node, depth, self);
        match &page_node {
            PageNode::Page(p) => {
                let page = unsafe { &**p };
                if page.flags & BranchPageFlag != 0 {
                    for i in 0..page.count as usize {
                        let elem = page.branch_page_element(i);
                        self._for_each_page_node(elem.pgid, depth + 1, f)?;
                    }
                }
            }
            PageNode::Node(n) => {
                let node = n.node();
                if !node.is_leaf {
                    for inode in &node.inodes {
                        self._for_each_page_node(inode.pgid, depth + 1, f)?;
                    }
                }
            }
        }
        Ok(())
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
        if let Some(node) = self.nodes.borrow().get(&id) {
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
        if let Some(node) = self.nodes.borrow().get(&pgid) {
            return node.clone();
        }

        let mut n = if let Some(p) = parent {
            let n = NodeImpl::new().parent(p.clone()).build();
            let parent_node = p.upgrade().map(Node).unwrap();
            parent_node.node_mut().children.push(n.clone());
            n
        } else {
            let n = NodeImpl::new().build();
            self.root_node.replace(n.clone());
            n
        };

        let page = if let Some(p) = &self.page {
            p.to_page()
        } else {
            let p = self.tx().unwrap().db().page(pgid);
            unsafe { &*p }
        };
        n.read(page);
        self.nodes.borrow_mut().insert(pgid, n.clone());
        n
    }

    pub(crate) fn rebalance(&mut self, page_size: usize) -> NKResult<()> {
        let nodes = self.nodes.clone();
        for n in nodes.borrow_mut().values_mut() {
            n.rebalance(page_size, self)?;
        }

        for b in self.buckets.borrow_mut().values_mut() {
            b.rebalance(page_size)?;
        }
        Ok(())
    }

    pub(crate) fn max_inline_bucket_size(&self) -> usize {
        self.tx().unwrap().db().get_page_size() / 4
    }

    fn inline_able(&self) -> bool {
        if let Some(n) = &self.root_node {
            if !n.node().is_leaf {
                return false;
            }
            let mut size = Page::header_size();
            for inode in n.node().inodes.iter() {
                size += LeafPageElementSize + inode.key.len() + inode.value.len();
                if inode.flags & BucketLeafFlag != 0 {
                    return false;
                } else if size > self.max_inline_bucket_size() {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    pub(crate) fn spill(&mut self, atx: Arc<TxImpl>) -> NKResult<()> {
        let root_bucket = unsafe { &mut *(self as *mut Self) };
        for (name, child) in self.buckets.borrow_mut().iter_mut() {
            // b.in
            let value = if child.inline_able() {
                println!("bucket is inline_able");
                child.free()?;
                child.write()
            } else {
                println!("bucket is no inline_able, to spill");
                child.spill(atx.clone())?;
                let value = vec![0u8; BucketHeaderSize];
                let bucket = value.as_ptr() as *mut IBucket;
                unsafe {
                    *bucket = *&child.ibucket;
                }
                value
            };
            if child.root_node.is_none() {
                continue;
            }
            let mut c = root_bucket.cursor();
            let item = c.seek(name)?;
            if let Some(k) = item.0 {
                if k != name {
                    panic!("misplaced bucket header: {:?} -> {:?}", k, name);
                }
            }
            if item.flags() & BucketLeafFlag == 0 {
                panic!("unexpected bucket header flag: {}", item.flags());
            }
            c.node()?
                .put(name, name, value.as_slice(), 0, BucketLeafFlag);
        }

        if let Some(n) = &self.root_node {
            let mut root = n.clone();
            let root_node = root.spill(atx, &self)?;
            self.ibucket.root = root_node.node().pgid;
            self.root_node.replace(root_node);
        }

        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(crate) struct IBucket {
    pub(crate) root: Pgid,
    pub(crate) sequence: u64,
}

impl IBucket {
    pub(crate) fn new(root: Pgid) -> IBucket {
        Self {
            root: root,
            sequence: 0,
        }
    }
}
