mod bucket;
mod cursor;
pub mod db;
pub mod error;
mod freelist;
mod node;
mod page;
pub mod tx;

pub(crate) const magic: u32 = 0xED0CDAED;
pub(crate) const version: u32 = 2;

pub(crate) fn u8_to_struct_mut<T>(buf: &mut [u8]) -> &mut T {
    let s = unsafe { &mut *(buf.as_mut_ptr() as *mut u8 as *mut T) };
    s
}

pub(crate) fn u8_to_struct<T>(buf: &[u8]) -> &T {
    let s = unsafe { &*(buf.as_ptr() as *const u8 as *const T) };
    s
}
