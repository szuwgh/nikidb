use crate::datafile::DataFile;
use crate::datafile::Entry;
use crate::error::IoResult;
use crate::option::DataType;
use crate::option::Options;
use crate::option::{DATA_TYPE_HASH, DATA_TYPE_LIST, DATA_TYPE_SET, DATA_TYPE_STR, DATA_TYPE_ZSET};
use crate::util::time;
use std::collections::HashMap;
use std::fs;
use std::io::Error;
use std::io::ErrorKind;

pub struct DB {
    // active file:
    active_file_map: HashMap<DataType, DataFile>,
    //archived_files
    archived_files: HashMap<DataType, HashMap<u32, DataFile>>,
    //memory index message
    indexes: HashMap<Vec<u8>, u64>,
    //db config
    options: Options,
}

fn build_data_file(
    dir_path: &str,
) -> IoResult<(
    HashMap<DataType, DataFile>,
    HashMap<DataType, HashMap<u32, DataFile>>,
)> {
    let mut files_type_map: HashMap<DataType, HashMap<u32, DataFile>> = HashMap::new();
    let dir = fs::read_dir("./db")?;

    let names = dir
        .filter_map(|entry| {
            entry.ok().and_then(|e| {
                e.path().file_name().and_then(|n| {
                    n.to_str().and_then(|s| {
                        if s.contains(".data") {
                            return Some(String::from(s));
                        }
                        None
                    })
                })
            })
        })
        .collect::<Vec<String>>();
    files_type_map.insert(DataType::String, HashMap::new());
    let active_file: HashMap<DataType, DataFile> = HashMap::new();
    for n in names.iter() {
        let split_name: Vec<&str> = n.split(".").collect();
        let id = split_name[0].parse::<i32>().unwrap();
        let data_type = match split_name[2] {
            DATA_TYPE_ZSET => DataType::ZSet,
            DATA_TYPE_STR => DataType::String,
            DATA_TYPE_HASH => DataType::Hash,
            DATA_TYPE_LIST => DataType::List,
            DATA_TYPE_SET => DataType::Set,
            _ => continue,
        };

        //println!("{}", n);
    }
    // if names.len() > 0 {
    //let len_names =

    // let len_names = names.len() - 1;
    // active_file = DataFile::new(dir_path, names[len_names])?;
    // for i in 0..len_names {
    //     files_map.insert(names[i], DataFile::new(dir_path, names[len_names])?);
    // }
    // } else {
    //     active_file = DataFile::new(dir_path, 0)?;
    // }
    Ok((active_file, files_type_map))
}

impl DB {
    pub fn open(dir_path: &str, options: Options) -> IoResult<DB> {
        //create database dir
        fs::create_dir_all(dir_path).map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        let (active_file, archived_files) = build_data_file(dir_path)?;
        let mut db = DB {
            active_file_map: active_file,
            archived_files: archived_files,
            indexes: HashMap::new(),
            options: options,
        };
        // db.load_index();
        Ok(db)
    }

    // fn get_active_file() -> DataFile {}

    // fn load_index(&mut self) {
    //     let mut iter = self.active_data_file.iterator();
    //     let mut offset: u64 = 0;
    //     loop {
    //         let e = iter.next();
    //         let entry = match e {
    //             Ok(entry) => entry,
    //             Err(_) => break,
    //         };
    //         self.indexes.insert(entry.key.clone(), offset);
    //         offset += entry.size() as u64;
    //     }
    //     self.active_data_file.offset = offset;
    // }

    fn load_file() {}

    fn check_kv(&mut self, key: &[u8], value: &[u8]) {}

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> IoResult<u64> {
        let e = Entry {
            timestamp: time::get_time_unix_nano() as u64,
            key: key.to_vec(),
            value: value.to_vec(),
            crc: 0,
        };
        let offset = self.store(&e)?;
        self.indexes.insert(e.key, offset);
        Ok(offset)
    }

    fn store(&mut self, e: &Entry) -> IoResult<u64> {
        let sz = e.size() as u64;
        let active_data_file = self
            .active_file_map
            .get_mut(&DataType::String)
            .ok_or(Error::from(ErrorKind::Interrupted))?;
        if active_data_file.offset + sz > self.options.file_size {
            active_data_file.sync()?;
            //self.active_data_file.close();
        }
        let offset = active_data_file.put(e)?;
        Ok(offset)
    }

    pub fn read(&mut self, key: &[u8]) -> IoResult<Entry> {
        let offset = self
            .indexes
            .get(&key.to_vec())
            .ok_or(Error::from(ErrorKind::Interrupted))?;
        let active_data_file = self
            .active_file_map
            .get_mut(&DataType::String)
            .ok_or(Error::from(ErrorKind::Interrupted))?;
        active_data_file.read(*offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_put() {
        let c = Options::default();
        let mut d = DB::open("./db", c).unwrap();
        d.put("a".as_bytes(), "aaabbbcccccc".as_bytes()).unwrap();
        let value = d.read("a".as_bytes()).unwrap().value;
        println!("{:?}", value);
    }
    #[test]
    fn test_build_data_file() {
        build_data_file("./db");
    }
}
