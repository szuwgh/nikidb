use crate::bucket::IBucket;
use crate::page::{
    BranchPageElementSize, BucketLeafFlag, LeafPageElementSize, Page, PageFlag, Pgid,
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

pub(crate) type Node = Rc<NodeImpl>;

fn return_node() -> Node {
    Rc::new(NodeImpl::new(false))
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NodeImpl {
    pub(crate) is_leaf: bool,
    pub(crate) inodes: Vec<INode>,
    pub(crate) parent: Weak<NodeImpl>,
}

impl NodeImpl {
    pub(crate) fn new(is_leaf: bool) -> NodeImpl {
        Self {
            is_leaf: false,
            inodes: Vec::new(),
            ..Default::default()
        }
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

    pub(crate) fn write_to(&self, p: &mut Page) -> NKResult<()> {
        if self.is_leaf {
            p.flags = PageFlag::LeafPageFlag;
        } else {
            p.flags = PageFlag::BranchPageFlag;
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

#[derive(Clone, Debug)]
pub(crate) struct INode {
    flags: u32,
    pub(crate) pgid: Pgid,
    key: Vec<u8>,
    value: Vec<u8>,
}
