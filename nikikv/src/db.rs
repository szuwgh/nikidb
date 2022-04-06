use crate::bucket::Bucket;
use crate::error::{NKError, NKResult};
use crate::page::{Meta, Page, PageFlag, Pgid};
use crate::{magic, version};
use page_size;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::prelude::{FileExt, OpenOptionsExt};

fn get_page_size() -> u32 {
    page_size::get() as u32
}

pub struct DB {
    file: InnerFile,
    mmap: memmap::Mmap,
    // meta0: Meta,
    // meta1: Meta,
}

struct InnerFile {
    file: File,
    page_size: u32,
}

struct InnerMmap<'a> {
    meta0: &'a Meta,
    meta1: &'a Meta,
}

impl InnerFile {
    fn new(file: File) -> InnerFile {
        Self {
            file: file,
            page_size: 0,
        }
    }

    fn init(&mut self) -> NKResult<()> {
        self.page_size = get_page_size();
        let mut buf: Vec<u8> = vec![0; 4 * self.page_size as usize];
        for i in 0..2 {
            let p = self.page_in_buffer_mut(&mut buf, i);
            p.id = i as Pgid;
            p.flags = PageFlag::MetaPageFlag;

            let m = p.meta_mut();
            m.magic = magic;
            m.version = version;
            m.page_size = self.page_size;
            m.freelist = 2;
            m.root = Bucket::new(3);
            m.pgid = 4;
            m.txid = 1;
            m.checksum = m.sum64();
        }

        // write an empty freelist at page 3
        let mut p = self.page_in_buffer_mut(&mut buf, 2);
        p.id = 2;
        p.flags = PageFlag::FreeListPageFlag;
        p.count = 0;

        p = self.page_in_buffer_mut(&mut buf, 3);
        p.id = 3;
        p.flags = PageFlag::LeafPageFlag;
        p.count = 0;

        self.write_at(&mut buf, 0)?;
        self.sync()?;

        Ok(())
    }

    fn write_at(&mut self, buf: &mut [u8], pos: u64) -> NKResult<()> {
        self.file
            .write_at(buf, pos)
            .map_err(|_e| ("can't write to file", _e))?;
        Ok(())
    }

    fn sync(&mut self) -> NKResult<()> {
        self.file.flush().map_err(|_e| ("can't flush file", _e))?;
        Ok(())
    }

    fn page_in_buffer_mut<'a>(&mut self, buf: &'a mut [u8], id: u32) -> &'a mut Page {
        Page::from_buf_mut(&mut buf[(id * self.page_size) as usize..])
    }

    fn page_in_buffer<'a>(&self, buf: &'a [u8], id: u32) -> &'a Page {
        Page::from_buf(&buf[(id * self.page_size) as usize..])
    }
}

impl DB {
    pub fn open(db_path: &str) -> NKResult<DB> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_path)
            .map_err(|e| NKError::DBOpenFail(e))?;
        let size = f.metadata().map_err(|e| NKError::DBOpenFail(e))?.len();
        let mut inner_file = InnerFile::new(f);
        if size == 0 {
            inner_file.init()?;
        } else {
            let mut buf = vec![0; 0x1000];
            inner_file
                .file
                .read_at(&mut buf, 0)
                .map_err(|_e| ("can't read to file", _e))?;
            let m = inner_file.page_in_buffer(&buf, 0).meta();
            m.validate()?;
            inner_file.page_size = m.page_size;
            println!("read:checksum {}", m.checksum);
        }
        Ok(db)
    }

    fn mmap(&mut self, mut min_size: u64) -> NKResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_db_open() {
        DB::open("./test.db").unwrap();
    }
}
