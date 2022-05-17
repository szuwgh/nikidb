use crate::bucket::{Bucket, IBucket};
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
use std::ptr::null;
use std::rc::Rc;
use std::rc::Weak;

pub(crate) type Node = Rc<RefCell<NodeImpl>>;

// fn return_node() -> Node {
//     RefCell::new(NodeImpl::new(false))
// }

#[derive(Clone, Debug)]
pub(crate) struct NodeImpl {
    pub(crate) bucket: *const Bucket,
    pub(crate) is_leaf: bool,
    pub(crate) inodes: Vec<INode>,
    pub(crate) parent: Weak<RefCell<NodeImpl>>,
    unbalanced: bool,
    spilled: bool,
    pgid: Pgid,
    children: Vec<Node>,
}

impl NodeImpl {
    pub(crate) fn new(bucket: *const Bucket) -> NodeImpl {
        Self {
            bucket: bucket,
            is_leaf: false,
            inodes: Vec::new(),
            parent: Weak::new(),
            unbalanced: false,
            spilled: false,
            pgid: 0,
            children: Vec::new(),
        }
    }

    pub fn parent(mut self, parent: Weak<RefCell<NodeImpl>>) -> NodeImpl {
        self.parent = parent;
        self
    }

    pub(crate) fn build(self) -> Node {
        Rc::new(RefCell::new(self))
    }

    pub(crate) fn child_at(&self, index: usize, parent: Weak<RefCell<NodeImpl>>) -> Node {
        if self.is_leaf {
            panic!("invalid childAt{} on a leaf node", index);
        }
        self.bucket_mut().node(self.inodes[index].pgid, parent)
    }

    fn bucket_mut(&self) -> &mut Bucket {
        unsafe { &mut *(self.bucket as *mut Bucket) }
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
    }

    pub(super) fn bucket(&self) -> &Bucket {
        assert!(!self.bucket.is_null());
        unsafe { &*self.bucket }
    }

    pub(crate) fn put(
        &mut self,
        old_key: &[u8],
        new_key: &[u8],
        value: &[u8],
        pgid: Pgid,
        flags: u32,
    ) {
        if pgid > self.bucket().tx().unwrap().meta.pgid {
            panic!(
                "pgid {} above high water mark {}",
                pgid,
                self.bucket().tx().unwrap().meta.pgid,
            )
        }else if old_key.len()
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
