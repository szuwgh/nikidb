use crate::util::binary;
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
    offset: i64,
    share: [u8; 10],
    buf: Vec<u8>,
}

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
            offset: 0,
            buf: Vec::new(),
        };

        Ok(data_file)
    }

    pub fn put(&mut self, e: &Entry) -> Result<usize> {
        self.buf.clear();
        let mut m = 0;
        let mut n = binary::put_varuint64(&mut self.share, e.timestamp);
        m += n;
        self.buf.extend_from_slice(&self.share[0..n]);
        n = binary::put_varuint64(&mut self.share, e.key.len() as u64);
        m += n;
        self.buf.extend_from_slice(&self.share[0..n]);
        self.buf.extend_from_slice(e.key.as_slice());
        n = binary::put_varuint64(&mut self.share, e.value.len() as u64);
        m += n;
        self.buf.extend_from_slice(&self.share[0..n]);
        self.buf.extend_from_slice(e.value.as_slice());
        self.file
            .write_all(self.buf.as_slice())
            .map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        Ok(m + e.key.len() + e.value.len())
    }

    pub fn sync() {}

    pub fn read(&mut self, offset: u64, size: usize) -> Result<()> {
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        let mut buf = vec![0; size];
        let n = self
            .file
            .read(&mut buf[..])
            .map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        let _buf = buf.as_slice();
        let (timestamp, m) = binary::read_varuint64(&_buf)
            .ok_or(Error::from(ErrorKind::Unsupported))
            .map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        let (nkey, n1) = binary::read_varuint64(&_buf[m..])
            .ok_or(Error::from(ErrorKind::Unsupported))
            .map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        let nkey = nkey as usize;
        let key = &_buf[m + n1..m + n1 + nkey];
        let (nvalue, n2) = binary::read_varuint64(&_buf[m + n1 + nkey..])
            .ok_or(Error::from(ErrorKind::Unsupported))
            .map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        let nvalue = nvalue as usize;
        let value = &_buf[m + n1 + nkey + n2..m + n1 + nkey + n2 + nvalue];

        println!(
            "{},{},{}",
            timestamp,
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value),
        );
        Ok(())
    }
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
        _db.read(0, sz).unwrap();
        e.key = "lisi".as_bytes().to_vec();
        let sz2 = _db.put(&e).unwrap();
        _db.read(sz as u64, sz2).unwrap();
    }
}
