use crate::data_file_format;
use crate::error::IoResult;

use crate::util::binary;
use crc::crc32;
use crc::Hasher32;
use memmap::MmapMut;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

//4 + 4 + 4 + 8
//crc + key_len + value_len + timestamp
pub const ENTRY_HEADER_SIZE: usize = 20;

pub struct DataFile {
    file: File,
    mmap: MmapMut,
    pub offset: usize,
    pub file_id: u32,
    pub path: PathBuf,
    file_size: u64,
}

pub struct DataFileIterator<'a> {
    // type Item = (u64, Entry);
    data_file: &'a mut DataFile,
}

impl<'a> Iterator for DataFileIterator<'a> {
    type Item = (u64, Entry);
    fn next(&mut self) -> Option<Self::Item> {
        self.data_file.next()
    }
}

impl<'a> DataFileIterator<'a> {
    fn new(data_file: &'a mut DataFile) -> Self {
        Self {
            data_file: data_file,
        }
    }
}

#[derive(Debug)]
pub struct Entry {
    pub timestamp: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub crc: u32,
}

impl Entry {
    fn decode(buf: &[u8]) -> Option<Entry> {
        let crc = binary::big_endian::read_u32(&buf[0..4]);
        if crc == 0 {
            return None;
        }
        let ks = binary::big_endian::read_u32(&buf[4..8]);
        let vs = binary::big_endian::read_u32(&buf[8..12]);
        let timestamp = binary::big_endian::read_u64(&buf[12..20]);
        Some(Entry {
            timestamp: timestamp,
            key: vec![0; ks as usize],
            value: vec![0; vs as usize],
            crc: crc,
        })
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = vec![0; self.size()];
        let ks = self.key.len();
        let vs = self.value.len();
        let mut digest = crc32::Digest::new(crc32::CASTAGNOLI);
        digest.write(self.value.as_slice());
        binary::big_endian::put_uint32(&mut buf[0..4], digest.sum32());
        binary::big_endian::put_uint32(&mut buf[4..8], ks as u32);
        binary::big_endian::put_uint32(&mut buf[8..12], vs as u32);
        binary::big_endian::put_uint64(&mut buf[12..20], self.timestamp);
        buf[ENTRY_HEADER_SIZE..ENTRY_HEADER_SIZE + ks].clone_from_slice(self.key.as_slice());
        buf[ENTRY_HEADER_SIZE + ks..ENTRY_HEADER_SIZE + ks + vs]
            .clone_from_slice(self.value.as_slice());
        buf
    }

    pub fn size(&self) -> usize {
        self.key.len() + self.value.len() + ENTRY_HEADER_SIZE
    }
}

impl DataFile {
    pub fn open(dir_path: &str, file_id: u32, data_type: &str) -> IoResult<DataFile> {
        let path = Path::new(dir_path);
        let data_file_name = path.join(data_file_format!(data_type, file_id));
        let f = OpenOptions::new().read(true).open(&data_file_name)?;
        // f.set_len(size)?;
        let file_size = f.metadata()?.len();
        let mmap = unsafe { MmapMut::map_mut(&f).expect("Error creating memory map") };
        Ok(Self {
            file: f,
            mmap: mmap,
            offset: 0,
            file_id: file_id,
            path: data_file_name,
            file_size: file_size,
        })
    }

    pub fn new(dir_path: &str, size: u64, file_id: u32, data_type: &str) -> IoResult<DataFile> {
        let path = Path::new(dir_path);
        let data_file_name = path.join(data_file_format!(data_type, file_id));
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&data_file_name)?;
        f.set_len(size)?;
        // let file_size = f.metadata()?.len();
        let mmap = unsafe { MmapMut::map_mut(&f).expect("Error creating memory map") };
        Ok(Self {
            file: f,
            mmap: mmap,
            offset: 0,
            file_id: file_id,
            path: data_file_name,
            file_size: size,
        })
    }

    pub fn put(&mut self, e: &Entry) -> IoResult<u64> {
        let buf = e.encode();
        //self.file.write_all(buf.as_slice())?;
        (&mut self.mmap[self.offset..]).write_all(buf.as_slice())?;
        self.mmap.flush().expect("Error flushing memory map");
        let s = self.offset;
        self.offset += buf.len();
        Ok(s as u64)
    }

    pub fn sync(&mut self) -> IoResult<()> {
        self.file.sync_all()?;
        Ok(())
    }

    pub fn get(&self, offset: u64) -> IoResult<Entry> {
        let offset = offset as usize;
        let a = &self.mmap[offset..offset + ENTRY_HEADER_SIZE];
        let mut e = Entry::decode(a).ok_or(Error::from(ErrorKind::Interrupted))?;
        let offset = offset + ENTRY_HEADER_SIZE;
        let koff = offset + e.key.len();
        e.key.copy_from_slice(&self.mmap[offset..koff]);
        let voff = koff + e.value.len();
        e.value.copy_from_slice(&self.mmap[koff..voff]);

        let mut digest = crc32::Digest::new(crc32::CASTAGNOLI);
        digest.write(e.value.as_slice());

        if e.crc != digest.sum32() {
            return Err(Error::from(ErrorKind::Interrupted));
        }
        Ok(e)
    }

    pub fn next(&mut self) -> Option<(u64, Entry)> {
        let offset = self.offset;
        let hoff = self.offset + ENTRY_HEADER_SIZE;
        let file_size = self.file_size as usize;
        if hoff > file_size {
            return None;
        }
        let head = &self.mmap[self.offset..hoff];
        let mut e = Entry::decode(head)?;
        if offset + e.size() > file_size {
            return None;
        }

        let koff = hoff + e.key.len();
        e.key.copy_from_slice(&self.mmap[hoff..koff]);
        let voff = koff + e.value.len();
        e.value.copy_from_slice(&self.mmap[koff..voff]);
        let mut digest = crc32::Digest::new(crc32::CASTAGNOLI);
        digest.write(e.value.as_slice());
        if e.crc != digest.sum32() {
            return None;
        }
        self.offset = voff;
        Some((offset as u64, e))
    }

    pub fn iter(&mut self) -> DataFileIterator {
        DataFileIterator::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_put_and_read() {
        let mut _db = DataFile::new("./db", 1024, 0, DataType::String).unwrap();
        let mut e = Entry {
            timestamp: 123456,
            key: "zhangsan".as_bytes().to_vec(),
            value: "kingdee".as_bytes().to_vec(),
            crc: 0,
        };
        let sz = _db.put(&e).unwrap();
        let _e = _db.get(0).unwrap();
        println!("{:?}", String::from_utf8(_e.value).unwrap());
        e.key = "lisi".as_bytes().to_vec();
        let sz2 = _db.put(&e).unwrap();
        let _e = _db.get(sz2).unwrap();
        println!("{:?}", String::from_utf8(_e.value).unwrap());
    }
    #[test]
    fn test_data_file_format() {
        println!("{}", data_file_format!("str", 1));
    }
    #[test]
    fn test_file_mmap() {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("./db/test.txt")
            .unwrap();
        let file_size = f.metadata().unwrap().len();
        f.set_len(20).unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&f).expect("Error creating memory map") };
        println!("size is {}", file_size);
        (&mut mmap[19..]).write_all("a".as_bytes());
        println!("{:?}", &mmap[..]);
    }
}
