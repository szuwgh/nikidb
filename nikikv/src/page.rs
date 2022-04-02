use std::marker::PhantomData;
type pgid = u64;
//页数据
struct Page {
    id: pgid,
    flags: u16,
    count: u16,
    overflow: u32,
    ptr: PhantomData<u8>,
}

struct LeafPageElement {
    flags: u32,
    pos: u32,
    ksize: u32,
    vsize: u32,
}

#[repr(C)]
pub struct MyStruct {
    a: u32,
    b: u64,
    ptr: PhantomData<u8>,
}

impl MyStruct {
    fn leaf_page_element(&mut self) -> &mut [LeafPageElement] {
        unsafe {
            std::slice::from_raw_parts_mut(
                &mut self.ptr as *mut PhantomData<u8> as *mut u8 as *mut LeafPageElement,
                10,
            )
        }
    }
}

pub fn u8_to_value(buf: &mut [u8]) -> &mut MyStruct {
    let s = unsafe { &mut *(buf.as_mut_ptr() as *mut u8 as *mut MyStruct) };
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_struct_to_slice() {
        // let s: MyStruct = MyStruct { a: 123, b: 456 };
        // let b = any_as_u8_slice(&s);
        // println!("b:{:?}", b);
        let mut b = vec![0u8; 4 * 1024];
        let a = u8_to_value(&mut b);
        a.a = 100;
        println!("MyStruct:{:?}", a.a);
        let mut v = a.leaf_page_element();
        v[0].b = 200;
        println!("v[0].b:{:?}", v[0].b);

        let mut b1 = b.clone();
        let a1 = u8_to_value(&mut b1);
        let v1 = a1.leaf_page_element();
        println!("v1[0].b:{:?}", v1[0].b);
    }
}
