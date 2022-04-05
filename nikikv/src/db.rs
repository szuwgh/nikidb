use crate::error::NKError;
use crate::error::NKResult;
use crate::page::{Page, Pgid};
use std::fs::File;
use std::fs::OpenOptions;

const magic: u32 = 0xED0CDAED;

fn get_page_size() -> u32 {
    4 * 1024
}

fn u8_to_struct<T>(buf: &mut [u8]) -> &mut T {
    let s = unsafe { &mut *(buf.as_mut_ptr() as *mut u8 as *mut T) };
    s
}

// const fn get_page_size() -> u32 {
//     return 4 * 1024; //4k
// }

// static default_page_size: u32 = get_page_size();

pub struct DB {
    file: File,
    page_size: u32,
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
            page_size: get_page_size(),
        }
    }

    fn init(&mut self) -> NKResult<()> {
        self.page_size = get_page_size();
        let buf: Vec<u8> = vec![0; 4 * self.page_size as usize];
        for i in 0..2 {}
        Ok(())
    }

    fn page_in_buffer<'a>(&mut self, buf: &'a mut [u8], id: u32) -> &'a mut Page {
        let i = (id * self.page_size) as usize;
        u8_to_struct::<Page>(&mut buf[i..])
    }
}
