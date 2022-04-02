use crate::error::NKError;
use crate::error::NKResult;
use std::fs::File;
use std::fs::OpenOptions;

fn get_page_size() -> u32 {
    0
}

// const fn get_page_size() -> u32 {
//     return 4 * 1024; //4k
// }

// static default_page_size: u32 = get_page_size();

pub struct DB {
    file: File,
    page_size: u32,
}

struct Meta {}

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
