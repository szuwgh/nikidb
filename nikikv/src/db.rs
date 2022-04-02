use crate::error::NKError;
use crate::error::NKResult;
use std::fs::File;
use std::fs::OpenOptions;
use std::marker::PhantomData;

fn get_page_size() -> u32 {
    0
}

const fn get_page_size() -> u32 {
    return 4 * 1024; //4k
}

static default_page_size: u32 = get_page_size();

pub struct DB {
    file: File,
    page_size: u32,
}

type pgid = u64;

type uintptr = u64;

struct Page {
    id: pgid,
    flags: u16,
    count: u16,
    overflow: u32,
    ptr: uintptr,
}

struct Meta {}

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
            .map_err(|e| NKError::IoFail(e))?;
        let size = f.metadata().map_err(|e| NKError::IoFail(e))?.len();
        let mut db = Self::new(f);
        if size == 0 {
            db.init();
        }
        Ok(db)
    }

    fn new(file: File) -> DB {
        Self {
            file: file,
            page_size: 0,
        }
    }

    fn init(&mut self) -> NKResult<()> {
        self.page_size = get_page_size();
        let buf: Vec<u8> = vec![0; 4 * self.page_size as usize];

        Ok(())
    }
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
