pub struct DB {
    //  file: File,
}

pub struct MyStruct {}

impl DB {
    pub fn open(path: &str) {
        // let f = OpenOptions::new
    }
}

pub fn u8_to_value(v: &[u8]) -> MyStruct {
    let s: MyStruct = unsafe { std::ptr::read(v.as_ptr() as *const _) };
    s
}

unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    std::slice::from_raw_parts((p as *const T) as *const u8, ::std::mem::size_of::<T>())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_struct_to_slice() {}
}
