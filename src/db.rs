use crate::config::Config;
use crate::datafile::DataFile;
use crate::datafile::Entry;
use crate::error::IoResult;
use crate::util::time;
use std::collections::HashMap;
use std::fs;
use std::io::Error;
use std::io::ErrorKind;

pub struct DB {
    // active file:
    active_data_file: DataFile,
    //memory index message
    indexes: HashMap<String, u64>,
    //db config
    config: Config,
}

impl DB {
    pub fn open(dir_path: &str, config: Config) -> IoResult<DB> {
        //create database dir
        fs::create_dir_all(dir_path).map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        let mut db = DB {
            active_data_file: DataFile::new(dir_path)?,
            indexes: HashMap::new(),
            config: config,
        };
        db.load();
        Ok(db)
    }

    fn load(&mut self) {
        let mut iter = self.active_data_file.iterator();
        let mut offset: u64 = 0;
        loop {
            let e = iter.next();
            let entry = match e {
                Ok(entry) => entry,
                Err(_) => break,
            };
            self.indexes
                .insert(String::from_utf8(entry.key.clone()).unwrap(), offset);
            offset += entry.size() as u64;
        }
        self.active_data_file.set_offset(offset);
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> IoResult<u64> {
        // if key.len() == 0 || value.len() == 0 {
        //     return Ok(());
        // }
        let e = Entry {
            timestamp: time::get_time_unix_nano() as u64,
            key: key.to_vec(),
            value: value.to_vec(),
            crc: 0,
        };
        // let offset = self.active_data_file.put(&e)?;
        let offset = self.store(&e)?;
        self.indexes
            .insert(String::from_utf8(e.key).unwrap(), offset);
        Ok(offset)
    }

    fn store(&mut self, e: &Entry) -> IoResult<u64> {
        let sz = e.size() as u64;
        if self.active_data_file.offset + sz > self.config.file_size {}
        let offset = self.active_data_file.put(e)?;
        Ok(offset)
    }

    pub fn read(&mut self, key: &[u8]) -> IoResult<Entry> {
        let offset = self
            .indexes
            .get(&String::from_utf8(key.to_vec()).unwrap())
            .ok_or(Error::from(ErrorKind::Interrupted))?;
        self.active_data_file.read(*offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_put() {
        let c = Config::default();
        let mut d = DB::open("./dbfile", c).unwrap();
        d.put("a".as_bytes(), "aaabbbcccccc".as_bytes()).unwrap();
    }
}
