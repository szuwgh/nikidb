use crate::error::Error;
use crate::error::ResultError;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
pub struct DataFile {
    file: File,
    offset: i64,
    share_buf: Vec<u8>,
}

pub struct entry {
    pub timestamp: u128,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl DataFile {
    pub fn new(dir_path: &str) -> ResultError<DataFile> {
        let path = std::path::Path::new(dir_path);
        let f = OpenOptions::new()
            .read(true)
            .append(true)
            .write(true)
            .create(true)
            .open(path.join("my.db"))
            .map_err(|err| Error::IoFs { err: err })?;
        let data_file = DataFile {
            file: f,
            offset: 0,
            share_buf: Vec::new(),
        };

        Ok(data_file)
    }

    pub fn put(&mut self, e: &entry) -> ResultError<u64> {}
}
