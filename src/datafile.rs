use crate::error::Error;
use crate::error::ResultError;
use crate::util::binary;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

const FILENAME: &str = "my.db";

pub struct DataFile {
    file: File,
    offset: i64,
    share: [u8; 10],
    buf: Vec<u8>,
    // buf2: Vec<u8>,
}

pub struct Entry {
    pub timestamp: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl DataFile {
    pub fn new(dir_path: &str) -> ResultError<DataFile> {
        let path = Path::new(dir_path);
        let f = OpenOptions::new()
            .read(true)
            .append(true)
            .write(true)
            .create(true)
            .open(path.join(FILENAME))
            .map_err(|err| Error::IoFs { err: err })?;
        let data_file = DataFile {
            file: f,
            share: [0; 10],
            offset: 0,
            buf: Vec::new(),
        };

        Ok(data_file)
    }

    pub fn put(&mut self, e: &Entry) -> ResultError<usize> {
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
            .map_err(|err| Error::IoFs { err: err })?;
        Ok(m + e.key.len() + e.value.len())
    }

    pub fn sync() {}

    pub fn read(&mut self, offset: u64) {
        self.file
            .seek(offset)
            .map_err(|err| Error::IoFs { err: err })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_put() {}
}
