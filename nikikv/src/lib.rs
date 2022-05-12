mod bucket;
mod cursor;
mod db;
mod error;
mod freelist;
mod page;
mod tx;

pub const magic: u32 = 0xED0CDAED;
pub const version: u32 = 2;

pub(crate) fn u8_to_struct_mut<T>(buf: &mut [u8]) -> &mut T {
    let s = unsafe { &mut *(buf.as_mut_ptr() as *mut u8 as *mut T) };
    s
}

pub(crate) fn u8_to_struct<T>(buf: &[u8]) -> &T {
    let s = unsafe { &*(buf.as_ptr() as *const u8 as *const T) };
    s
}
