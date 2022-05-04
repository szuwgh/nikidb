use crate::bucket::IBucket;
use crate::{error::NKError, error::NKResult};
use crate::{magic, version};
use fnv::FnvHasher;
use memoffset::offset_of;
use std::hash::Hasher;
use std::marker::PhantomData;
//
pub type Pgid = u64;

pub type Txid = u64;

pub struct Node {
    is_leaf: bool,
}

pub struct INode {}

pub enum PageFlag {
    BranchPageFlag,
    LeafPageFlag,
    MetaPageFlag,
    FreeListPageFlag,
}

//页数据
pub struct Page {
    pub id: Pgid,
    pub flags: PageFlag,
    // 个数 2字节，统计叶子节点、非叶子节点、空闲列表页的个数
    pub count: u16,
    // 4字节，数据是否有溢出，主要在空闲列表上有用
    pub overflow: u32,
    ptr: PhantomData<u8>,
}

struct BranchPageElement {
    pos: u32,
    ksize: u32,
    pgid: Pgid,
}

struct LeafPageElement {
    flags: u32,
    pos: u32,
    ksize: u32,
    vsize: u32,
}

pub struct Meta {
    pub magic: u32,
    pub version: u32,
    pub page_size: u32,
    pub flags: u32,
    pub root: IBucket,
    pub freelist: Pgid,
    pub pgid: Pgid,
    pub txid: Txid,
    pub checksum: u64,
}

impl Meta {
    pub fn sum64(&self) -> u64 {
        let mut h = FnvHasher::default();
        let bytes = unsafe {
            std::slice::from_raw_parts(self as *const Self as *const u8, offset_of!(Meta, checksum))
        };
        h.write(bytes);
        h.finish()
    }

    pub fn validate(&self) -> NKResult<()> {
        if self.magic != magic {
            return Err(NKError::ErrInvalid);
        } else if self.version != version {
            return Err(NKError::ErrVersionMismatch);
        } else if self.checksum != self.sum64() {
            return Err(NKError::ErrChecksum);
        }
        Ok(())
    }
}

impl Page {
    pub fn from_buf_mut(buf: &mut [u8]) -> &mut Page {
        crate::u8_to_struct_mut::<Page>(buf)
    }

    pub fn from_buf(buf: &[u8]) -> &Page {
        crate::u8_to_struct::<Page>(buf)
    }

    pub fn meta_mut(&mut self) -> &mut Meta {
        self.element_mut::<Meta>()
    }

    pub fn meta(&self) -> &Meta {
        self.element::<Meta>()
    }

    fn elements_mut<T>(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.data_ptr_mut() as *mut T, 10) }
    }

    fn element<T>(&self) -> &T {
        unsafe { &*(self.data_ptr() as *const T) }
    }

    fn element_mut<T>(&mut self) -> &mut T {
        unsafe { &mut *(self.data_ptr_mut() as *mut T) }
    }

    fn leaf_page_elements_mut(&mut self) -> &mut [LeafPageElement] {
        self.elements_mut::<LeafPageElement>()
    }

    fn branch_page_elements_mut(&mut self) -> &mut [BranchPageElement] {
        self.elements_mut::<BranchPageElement>()
    }

    fn data_ptr_mut(&mut self) -> *mut u8 {
        &mut self.ptr as *mut PhantomData<u8> as *mut u8
    }

    fn data_ptr(&self) -> *const u8 {
        &self.ptr as *const PhantomData<u8> as *const u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_struct_to_slice() {
        let mut b = vec![0u8; 4 * 1024];
        let a = Page::from_buf_mut(&mut b);
        a.id = 100;
        println!("id:{:?}", a.id);
        let mut v = a.leaf_page_elements_mut();
        v[0].pos = 200;
        assert!(v[0].pos == 200);

        let mut b1 = b.clone();
        let a1 = Page::from_buf_mut(&mut b1);
        let v1 = a1.leaf_page_elements_mut();
        println!("v1[0].pos:{:?}", v1[0].pos);
        assert!(v1[0].pos == 200);
    }
}
