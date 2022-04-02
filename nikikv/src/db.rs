use crate::error::NKError;
use crate::error::NKResult;
use std::fs::OpenOptions;
use std::marker::PhantomData;

fn get_page_size() -> u32 {
    0
}

pub struct DB {
    //  file: File,
}

#[derive(Debug)]
pub struct MyStruct {
    a: u32,
    b: u64,
    ptr: PhantomData<u8>,
}

impl MyStruct {
    fn leaf_page_element(&self) -> Vec<LeafEele> {
        let k: Vec<LeafEele> = unsafe {
            Vec::from_raw_parts(
                &self.ptr as *const PhantomData<u8> as *mut u8 as *mut LeafEele,
                10,
                10,
            )
        };
        k
    }
}

struct LeafEele {
    c: u32,
    b: u32,
}

impl DB {
    pub fn open(db_path: &str) -> NKResult<DB> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(db_path)
            .map_err(|e| NKError::OpenDBFail(e))?;
        Ok(DB {})
    }

    fn init(&mut self) {}
}

pub fn u8_to_value(v: &[u8]) -> MyStruct {
    let s: MyStruct = unsafe { std::ptr::read(v.as_ptr() as *const _) };
    s
}

fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    unsafe { std::slice::from_raw_parts((p as *const T) as *const u8, ::std::mem::size_of::<T>()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_struct_to_slice() {
        // let s: MyStruct = MyStruct { a: 123, b: 456 };
        // let b = any_as_u8_slice(&s);
        // println!("b:{:?}", b);
        let b = vec![0; 4 * 1024];
        let a: MyStruct = u8_to_value(&b);
        let mut v = a.leaf_page_element();
        v[0].b = 100;
        println!("MyStruct:{:?}", v.len());

        // let b1 = b.clone();
        // let a1: MyStruct = u8_to_value(&b1);
        // let v1 = a1.leaf_page_element();
        // println!("v1[0].b:{:?}", v1[0].b);
    }
}
