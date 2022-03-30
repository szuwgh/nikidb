use crate::error::NKError;
use crate::error::NKResult;
use std::fs::OpenOptions;

pub struct DB {
    //  file: File,
}

#[derive(Debug)]
pub struct MyStruct {
    a: u32,
    b: u64,
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
        let s: MyStruct = MyStruct { a: 123, b: 456 };
        let b = any_as_u8_slice(&s);
        println!("b:{:?}", b);
        let a: MyStruct = u8_to_value(b);
        println!("MyStruct:{:?}", a);
    }
}
