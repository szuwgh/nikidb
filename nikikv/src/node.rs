use crate::bucket::{Bucket, IBucket};
use crate::page::{
    BranchPageElementSize, BranchPageFlag, BucketLeafFlag, FreeListPageFlag, LeafPageElementSize,
    LeafPageFlag, MetaPageFlag, Page, Pgid,
};
use crate::tx::{Tx, TxImpl};
use crate::{error::NKError, error::NKResult};
use crate::{magic, version};
use fnv::FnvHasher;
use memoffset::offset_of;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::Sub;
use std::ptr::{null, null_mut};
use std::rc::Rc;
use std::rc::Weak;
use std::sync::{Arc, Weak as ArcWeak};

pub(crate) type Node = Rc<RefCell<NodeImpl>>;

// fn return_node() -> Node {
//     RefCell::new(NodeImpl::new(false))
// }

#[derive(Clone, Debug)]
pub(crate) struct NodeImpl {
    // pub(crate) bucket: *mut Bucket,
    pub(crate) is_leaf: bool,
    pub(crate) inodes: Vec<INode>,
    pub(crate) parent: Option<Weak<RefCell<NodeImpl>>>,
    unbalanced: bool,
    spilled: bool,
    pub(crate) pgid: Pgid,
    pub(crate) children: Vec<Node>,
    key: Option<Vec<u8>>,
}

impl NodeImpl {
    pub(crate) fn new(bucket: *mut Bucket) -> NodeImpl {
        Self {
            //  bucket: bucket,
            is_leaf: false,
            inodes: Vec::new(),
            parent: None,
            unbalanced: false,
            spilled: false,
            pgid: 0,
            children: Vec::new(),
            key: None,
        }
    }

    pub fn leaf(mut self, is_leaf: bool) -> NodeImpl {
        self.is_leaf = is_leaf;
        self
    }

    pub fn parent(mut self, parent: Weak<RefCell<NodeImpl>>) -> NodeImpl {
        self.parent = Some(parent);
        self
    }

    pub(crate) fn build(self) -> Node {
        Rc::new(RefCell::new(self))
    }

    pub(crate) fn child_at(
        &self,
        bucket: &mut Bucket,
        index: usize,
        parent: Option<Weak<RefCell<NodeImpl>>>,
    ) -> Node {
        if self.is_leaf {
            panic!("invalid childAt{} on a leaf node", index);
        }
        bucket.node(self.inodes[index].pgid, parent)
    }

    pub(crate) fn size(&self) -> usize {
        let mut sz = Page::header_size();
        let elsz = self.page_element_size();
        for i in 0..self.inodes.len() {
            let item = self.inodes.get(i).unwrap();
            sz += elsz + item.key.len() + item.value.len();
        }
        sz
    }

    fn page_element_size(&self) -> usize {
        if self.is_leaf {
            return LeafPageElementSize;
        }
        BranchPageElementSize
    }

    pub(crate) fn read(&mut self, p: &Page) {
        self.pgid = p.id;
        self.is_leaf = (p.flags & LeafPageFlag) != 0;
        let count = p.count as usize;
        self.inodes = Vec::with_capacity(count);
        for i in 0..count {
            let mut inode = INode::new();
            if self.is_leaf {
                let elem = p.leaf_page_element(i);
                inode.flags = elem.flags;
                inode.key = elem.key().to_vec();
                inode.value = elem.value().to_vec();
            } else {
                let elem = p.branch_page_element(i);
                inode.pgid = elem.pgid;
                inode.key = elem.key().to_vec();
            }
            assert!(inode.key.len() > 0, "read: zero-length inode key");
        }

        if self.inodes.len() > 0 {
            self.key = Some(self.inodes.first().unwrap().key.clone());
        } else {
            self.key = None
        }
    }

    pub(crate) fn put(
        &mut self,
        old_key: &[u8],
        new_key: &[u8],
        value: &[u8],
        pgid: Pgid,
        flags: u32,
    ) {
        // if pgid > bucket.tx().unwrap().meta.borrow().pgid {
        //     panic!(
        //         "pgid {} above high water mark {}",
        //         pgid,
        //         bucket.tx().unwrap().meta.borrow().pgid,
        //     )
        // } else
        if old_key.len() <= 0 {
            panic!("put: zero-length old key")
        } else if new_key.len() <= 0 {
            panic!("put: zero-length new key")
        }
        let (exact, index) = match self
            .inodes
            .binary_search_by(|inode| inode.key.as_slice().cmp(old_key))
        {
            Ok(v) => (true, v),
            Err(e) => (false, e),
        };
        if !exact {
            self.inodes.insert(index, INode::new());
        }
        let inode = self.inodes.get_mut(index).unwrap();
        inode.flags = flags;
        inode.key = new_key.to_vec();
        inode.value = value.to_vec();
        inode.pgid = pgid;
        assert!(inode.key.len() > 0, "put: zero-length inode key")
    }

    pub(crate) fn write(&self, p: &mut Page) {
        if self.is_leaf {
            p.flags = LeafPageFlag;
        } else {
            p.flags = BranchPageFlag;
        }
        if self.inodes.len() > 0xFFF {
            panic!("inode overflow: {} (pgid={})", self.inodes.len(), p.id);
        }
        p.count = self.inodes.len() as u16;
        if p.count == 0 {
            return;
        }

        let mut buf_ptr = unsafe {
            p.data_ptr_mut()
                .add(self.page_element_size() * self.inodes.len())
        };

        for (i, item) in self.inodes.iter().enumerate() {
            assert!(item.key.len() > 0, "write: zero-length inode key");
            if self.is_leaf {
                let elem = p.leaf_page_element_mut(i);
                elem.pos = unsafe { buf_ptr.sub(elem.as_ptr() as usize) } as u32;
                elem.flags = item.flags as u32;
                elem.ksize = item.key.len() as u32;
                elem.vsize = item.value.len() as u32;
            } else {
                let elem = p.branch_page_element_mut(i);
                elem.pos = unsafe { buf_ptr.sub(elem.as_ptr() as usize) } as u32;
                elem.ksize = unsafe { buf_ptr.sub(elem.as_ptr() as usize) } as u32;
                elem.pgid = item.pgid;
                assert!(elem.pgid != p.id, "write: circular dependency occurred");
            }
            let (klen, vlen) = (item.key.len(), item.value.len());
            unsafe {
                std::ptr::copy_nonoverlapping(item.key.as_ptr(), buf_ptr, klen);
                buf_ptr = buf_ptr.add(klen);
                std::ptr::copy_nonoverlapping(item.value.as_ptr(), buf_ptr, vlen);
                buf_ptr = buf_ptr.add(vlen);
            }
        }
    }

    fn split() {}

    //node spill
    pub(super) fn spill(&mut self, atx: Arc<TxImpl>) -> NKResult<()> {
        if self.spilled {
            return Ok(());
        }
        self.children.sort_by(|a, b| {
            (**a).borrow().inodes[0]
                .key
                .cmp(&(**b).borrow().inodes[0].key)
        });
        for child in self.children.clone() {
            (*child).borrow_mut().spill(atx.clone())?;
        }
        let mut tx = atx.clone();
        let mut db = tx.db();

        if self.pgid > 0 {
            db.freelist
                .borrow_mut()
                .free(tx.meta.borrow().txid, unsafe { &*db.page(self.pgid) });
            self.pgid = 0;
        }

        let mut p = db.allocate(self.size() / db.get_page_size() as usize + 1)?;
        let page = p.to_page();
        if page.id >= tx.meta.borrow().pgid {
            panic!(
                "pgid {} above high water mark{}",
                page.id,
                tx.meta.borrow().pgid
            );
        }
        self.pgid = page.id;
        self.write(page);
        tx.pages.borrow_mut().insert(self.pgid, p);
        self.spilled = true;

        if let Some(parent) = &mut self.parent {
            let mut parent_node = parent.upgrade().unwrap();
            if let Some(key) = &self.key {
                (*parent_node)
                    .borrow_mut()
                    .put(key, key, &vec![], self.pgid, 0);
            } else {
                let inode = self.inodes.first().unwrap();
                (*parent_node)
                    .borrow_mut()
                    .put(&inode.key, &inode.key, &vec![], self.pgid, 0);
                self.key = Some(inode.key.clone());
            }
            self.children.clear();
            return (*parent_node).borrow_mut().spill(atx.clone());
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct INode {
    pub(crate) flags: u32,
    pub(crate) pgid: Pgid,
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

impl INode {
    fn new() -> INode {
        Self {
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ptr::null_mut;

    use super::*;
    #[test]
    fn test_node_new() {
        let n = NodeImpl::new(null_mut()).build();
    }
}
