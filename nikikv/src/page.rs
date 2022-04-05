use crate::bucket::Bucket;
use std::marker::PhantomData;

//
pub type Pgid = u64;

type Txid = u64;

//页数据
pub struct Page {
    id: Pgid,
    flags: u16,
    count: u16,
    overflow: u32,
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

struct Meta {
    magic: u32,
    version: u32,
    page_size: u32,
    flags: u32,
    root: Bucket,
    freelist: Pgid,
    pgid: Pgid,
    txid: Txid,
    checksum: u64,
}

impl Page {
    fn meta_mut(&mut self) -> &Meta {
        self.element_mut::<Meta>()
    }

    fn elements_mut<T>(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.data_ptr_mut() as *mut T, 10) }
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
        let a = u8_to_value::<Page>(&mut b);
        a.a = 100;
        println!("MyStruct:{:?}", a.a);
        let mut v = a.leaf_page_element();
        v[0].pos = 200;
        println!("v[0].b:{:?}", v[0].b);

        let mut b1 = b.clone();
        let a1 = u8_to_value(&mut b1);
        let v1 = a1.leaf_page_element();
        println!("v1[0].b:{:?}", v1[0].pos);
    }
}
