use crate::util::binary;
//use crate::util::binary::BigEndian;
use crate::error::IoResult;
use crc::crc32;
use crc::Hasher32;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;

const FILENAME: &str = "my.db";

//4 + 4 + 4 + 8
//crc + key_len + value_len + timestamp
const entryHeaderSize: usize = 20;

pub struct DataFile {
    file: File,
    pub offset: u64,
}

pub struct DataFileIterator<'a> {
    data_file: &'a mut DataFile,
}

impl<'a> DataFileIterator<'a> {
    fn new(data_file: &'a mut DataFile) -> Self {
        Self {
            data_file: data_file,
        }
    }
    pub fn next(&mut self) -> IoResult<Entry> {
        self.data_file.next()
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
    fn decode(buf: &[u8]) -> Self {
        let crc = binary::BigEndian::read_u32(&buf[0..4]);
        let ks = binary::BigEndian::read_u32(&buf[4..8]);
        let vs = binary::BigEndian::read_u32(&buf[8..12]);
        let timestamp = binary::BigEndian::read_u64(&buf[12..20]);
        Self {
            timestamp: timestamp,
            key: vec![0; ks as usize],
            value: vec![0; vs as usize],
            crc: crc,
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = vec![0; self.size()];
        let ks = self.key.len();
        let vs = self.value.len();
        let mut digest = crc32::Digest::new(crc32::CASTAGNOLI);
        digest.write(self.value.as_slice());
        binary::BigEndian::put_uint32(&mut buf[0..4], digest.sum32());
        binary::BigEndian::put_uint32(&mut buf[4..8], ks as u32);
        binary::BigEndian::put_uint32(&mut buf[8..12], vs as u32);
        binary::BigEndian::put_uint64(&mut buf[12..20], self.timestamp);
        buf[entryHeaderSize..entryHeaderSize + ks].clone_from_slice(self.key.as_slice());
        buf[entryHeaderSize + ks..entryHeaderSize + ks + vs]
            .clone_from_slice(self.value.as_slice());
        buf
    }

    pub fn size(&self) -> usize {
        self.key.len() + self.value.len() + entryHeaderSize
    }
}

impl DataFile {
    pub fn new(dir_path: &str) -> IoResult<DataFile> {
        let path = Path::new(dir_path);
        let f = OpenOptions::new()
            .read(true)
            .append(true)
            .write(true)
            .create(true)
            .open(path.join(FILENAME))?;
        let data_file = DataFile { file: f, offset: 0 };
        Ok(data_file)
    }

    pub fn put(&mut self, e: &Entry) -> IoResult<u64> {
        let buf = e.encode();
        self.file.write_all(buf.as_slice())?;
        self.file.sync_all()?;
        let s = self.offset;
        self.offset += buf.len() as u64;
        Ok(s)
    }

    pub fn sync() {}

    pub fn read(&mut self, offset: u64) -> IoResult<Entry> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.next()
    }

    pub fn next(&mut self) -> IoResult<Entry> {
        let mut buf = vec![0; entryHeaderSize];
        let sz = self.file.read(&mut buf)?;
        if sz == 0 {
            return Err(Error::from(ErrorKind::Interrupted));
        }
        let mut e = Entry::decode(&buf);
        self.file.read(&mut e.key)?;
        self.file.read(&mut e.value)?;
        Ok(e)
    }

    pub fn iterator(&mut self) -> DataFileIterator {
        DataFileIterator::new(self)
    }

    pub fn set_offset(&mut self, offset: u64) {
        self.offset = offset;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_put_and_read() {
        let mut _db = DataFile::new("./dbfile").unwrap();
        let mut e = Entry {
            timestamp: 123456,
            key: "zhangsan".as_bytes().to_vec(),
            value: "kingdee".as_bytes().to_vec(),
            crc: 0,
        };
        let sz = _db.put(&e).unwrap();
        let _e = _db.read(0).unwrap();
        println!("{:?}", _e);
        e.key = "lisi".as_bytes().to_vec();
        let sz2 = _db.put(&e).unwrap();
        let _e = _db.read(sz2).unwrap();
        println!("{:?}", _e);
    }
}
