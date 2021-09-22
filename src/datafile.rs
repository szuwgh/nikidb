use crate::util::binary;
use crc::crc32;
use crc::Hasher32;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Result;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;

const FILENAME: &str = "my.db";

pub struct DataFile {
    file: File,
    share: [u8; 10],
    buf1: Vec<u8>,
    buf2: Vec<u8>,
}

#[derive(Debug)]
pub struct Entry {
    pub timestamp: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl DataFile {
    pub fn new(dir_path: &str) -> Result<DataFile> {
        let path = Path::new(dir_path);
        let f = OpenOptions::new()
            .read(true)
            .append(true)
            .write(true)
            .create(true)
            .open(path.join(FILENAME))
            .map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        let data_file = DataFile {
            file: f,
            share: [0; 10],
            buf1: Vec::new(),
            buf2: Vec::new(),
        };

        Ok(data_file)
    }

    pub fn put(&mut self, e: &Entry) -> Result<usize> {
        self.buf1.clear();
        self.buf2.clear();
        let mut m = 0;
        let mut n = binary::put_varuint64(&mut self.share, e.timestamp);
        m += n;
        self.buf1.extend_from_slice(&self.share[0..n]);
        n = binary::put_varuint64(&mut self.share, e.key.len() as u64);
        m += n;
        self.buf1.extend_from_slice(&self.share[0..n]);
        self.buf1.extend_from_slice(e.key.as_slice());
        n = binary::put_varuint64(&mut self.share, e.value.len() as u64);
        m += n;
        self.buf1.extend_from_slice(&self.share[0..n]);
        self.buf1.extend_from_slice(e.value.as_slice());

        let mut digest = crc32::Digest::new(crc32::CASTAGNOLI);
        digest.write(self.buf1.as_slice());
        // let mut n = binary::put_uint32(&mut self.share);
        //self.buf2.w

        self.buf2.append(&mut self.buf1);

        self.file
            .write_all(self.buf2.as_slice())
            .map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        self.file.sync_all()?;
        Ok(m + e.key.len() + e.value.len())
    }

    pub fn sync() {}

    pub fn read(&mut self, offset: u64, size: usize) -> Result<Entry> {
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        let mut buf = vec![0; size];
        let n = self
            .file
            .read(&mut buf[..])
            .map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        let _buf = buf.as_slice();
        let (timestamp, n0) = binary::read_varuint64(&_buf)
            .ok_or(Error::from(ErrorKind::Unsupported))
            .map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        let (nkey, n1) = binary::read_varuint64(&_buf[n0..])
            .ok_or(Error::from(ErrorKind::Unsupported))
            .map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        let nkey = nkey as usize;
        let key = &_buf[n0 + n1..n0 + n1 + nkey];
        let (nvalue, n2) = binary::read_varuint64(&_buf[n0 + n1 + nkey..])
            .ok_or(Error::from(ErrorKind::Unsupported))
            .map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        let nvalue = nvalue as usize;
        let value = &_buf[n0 + n1 + nkey + n2..n0 + n1 + nkey + n2 + nvalue];

        let e = Entry {
            timestamp: timestamp,
            key: key.to_vec(),
            value: value.to_vec(),
        };
        Ok(e)
    }

    pub fn iterator(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_put_and_read() {
        let mut _db = DataFile::new("E:\\rustproject\\photon\\dbfile").unwrap();
        let mut e = Entry {
            timestamp: 123456,
            key: "zhangsan".as_bytes().to_vec(),
            value: "kingdee".as_bytes().to_vec(),
        };
        let sz = _db.put(&e).unwrap();
        let _e = _db.read(0, sz).unwrap();
        println!("{:?}", _e);
        e.key = "lisi".as_bytes().to_vec();
        let sz2 = _db.put(&e).unwrap();
        let _e = _db.read(sz as u64, sz2).unwrap();
        println!("{:?}", _e);
    }
}
