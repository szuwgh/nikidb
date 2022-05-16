use crate::bucket::IBucket;
use crate::page::{
    BranchPageElementSize, BranchPageFlag, BucketLeafFlag, FreeListPageFlag, LeafPageElementSize,
    LeafPageFlag, MetaPageFlag, Page, Pgid,
};
use crate::{error::NKError, error::NKResult};
use crate::{magic, version};
use fnv::FnvHasher;
use memoffset::offset_of;
use std::cell::RefCell;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::Sub;
use std::rc::Rc;
use std::sync::{Arc, Weak};

pub(crate) type Node = RefCell<NodeImpl>;

fn return_node() -> Node {
    RefCell::new(NodeImpl::new(false))
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NodeImpl {
    pub bucket: Weak<Bucket>,
    pub(crate) is_leaf: bool,
    pub(crate) inodes: Vec<INode>,
    pub(crate) parent: Weak<NodeImpl>,
    unbalanced: bool,
    spilled: bool,
    pgid: Pgid,
    children: Vec<Node>,
}

impl NodeImpl {
    pub(crate) fn new(is_leaf: bool) -> NodeImpl {
        Self {
            //inodes: Vec::new(),
            ..Default::default()
        }
    }

    pub fn parent(mut self, parent: Weak<NodeImpl>) -> NodeImpl {
        self.parent = parent;
        self
    }

    pub(crate) fn build(self) -> Node {
        RefCell::new(self)
    }

    pub(crate) fn child_at(&self, index: usize, parent: Weak<NodeImpl>) -> Node {
        if self.is_leaf {
            panic!("invalid childAt{} on a leaf node", index);
        }
        self.b
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

    pub(crate) fn read(&mut self, p: &Page) -> NKResult<()> {
        self.pgid = p.id;
        self.is_leaf = ((p.flags & LeafPageFlag) != 0);
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
            }
        }
        Ok(())
    }

    pub(crate) fn write(&self, p: &mut Page) -> NKResult<()> {
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
            return Ok(());
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
