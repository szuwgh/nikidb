use crate::bucket::IBucket;
use crate::{error::NKError, error::NKResult};
use crate::{magic, version};
use fnv::FnvHasher;
use memoffset::offset_of;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::mem::size_of;
//
pub(crate) type Pgid = u64;

pub(crate) type Txid = u64;

pub(crate) const LeafPageElementSize: usize = size_of::<LeafPageElement>();

pub(crate) const BranchPageElementSize: usize = size_of::<BranchPageElement>();

#[derive(Clone, Debug)]
pub(crate) struct Node {
    pub(crate) is_leaf: bool,
    pub(crate) inodes: Vec<INode>,
}

impl Node {
    pub(crate) fn new(is_leaf: bool) -> Node {
        Self {
            is_leaf: false,
            inodes: Vec::new(),
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

    fn write(&self, p: &mut Page) -> NKResult<()> {
        if self.is_leaf {
            p.flags = PageFlag::LeafPageFlag;
        } else {
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

#[derive(Copy, Clone)]
pub(crate) enum PageFlag {
    BranchPageFlag = 0b00001,
    LeafPageFlag = 0b00010,
    MetaPageFlag = 0b00100,
    FreeListPageFlag = 0b10000,
}

pub(crate) const BucketLeafFlag: u32 = 0x01;

impl PartialEq for PageFlag {
    fn eq(&self, other: &PageFlag) -> bool {
        *self as u16 == *other as u16
    }
}

//页数据
pub(crate) struct Page {
    pub(crate) id: Pgid,
    pub(crate) flags: PageFlag,
    // 个数 2字节，统计叶子节点、非叶子节点、空闲列表页的个数
    pub(crate) count: u16,
    // 4字节，数据是否有溢出，主要在空闲列表上有用
    pub(crate) overflow: u32,
    ptr: PhantomData<u8>,
}

pub(crate) struct BranchPageElement {
    pos: u32, //存储键相对于当前页面数据部分的偏移量
    ksize: u32,
    pub(crate) pgid: Pgid,
}

impl BranchPageElement {
    pub(crate) fn key(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self as *const u8).add(self.pos as usize),
                self.ksize as usize,
            )
        }
    }
}

pub(crate) struct LeafPageElement {
    pub(crate) flags: u32, //标志位，为0的时候表示就是普通的叶子节点，而为1的时候表示是子bucket，子bucket后面再展开说明。
    pos: u32,              //存储键相对于当前页面数据部分的偏移量
    ksize: u32,
    vsize: u32,
}

impl LeafPageElement {
    pub(crate) fn key(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self as *const u8).add(self.pos as usize),
                self.ksize as usize,
            )
        }
    }

    pub(crate) fn value(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self as *const u8).add((self.pos + self.ksize) as usize),
                self.vsize as usize,
            )
        }
    }
}

pub(crate) struct Meta {
    pub(crate) magic: u32,
    pub(crate) version: u32,
    pub(crate) page_size: u32,
    pub(crate) flags: u32,
    pub(crate) root: IBucket,
    pub(crate) freelist: Pgid,
    pub(crate) pgid: Pgid,
    pub(crate) txid: Txid,
    pub(crate) checksum: u64,
}

impl Meta {
    pub(crate) fn sum64(&self) -> u64 {
        let mut h = FnvHasher::default();
        let bytes = unsafe {
            std::slice::from_raw_parts(self as *const Self as *const u8, offset_of!(Meta, checksum))
        };
        h.write(bytes);
        h.finish()
    }

    pub(crate) fn validate(&self) -> NKResult<()> {
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
    pub(crate) fn header_size() -> usize {
        offset_of!(Page, ptr)
    }

    pub(crate) fn from_buf_mut(buf: &mut [u8]) -> &mut Page {
        crate::u8_to_struct_mut::<Page>(buf)
    }

    pub(crate) fn from_buf(buf: &[u8]) -> &Page {
        crate::u8_to_struct::<Page>(buf)
    }

    pub(crate) fn meta_mut(&mut self) -> &mut Meta {
        self.element_mut::<Meta>()
    }

    pub(crate) fn meta(&self) -> &Meta {
        self.element::<Meta>()
    }

    fn elements<T>(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.data_ptr() as *const T, 10) }
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

    pub(crate) fn branch_page_elements(&self) -> &[BranchPageElement] {
        self.elements::<BranchPageElement>()
    }

    pub(crate) fn leaf_page_elements(&self) -> &[LeafPageElement] {
        self.elements::<LeafPageElement>()
    }

    pub(crate) fn leaf_page_element(&self, index: usize) -> &LeafPageElement {
        &self.elements::<LeafPageElement>()[index]
    }

    pub(crate) fn branch_page_element(&self, index: usize) -> &BranchPageElement {
        &self.elements::<BranchPageElement>()[index]
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
