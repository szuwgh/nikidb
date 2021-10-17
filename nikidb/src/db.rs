use crate::datafile::DataFile;
use crate::datafile::Entry;
use crate::error::IoResult;
use crate::option::DataType;
use crate::option::Options;
use crate::option::{DATA_TYPE_HASH, DATA_TYPE_LIST, DATA_TYPE_SET, DATA_TYPE_STR, DATA_TYPE_ZSET};
use crate::result_skip_fail;
use crate::util::time;
use std::collections::HashMap;
use std::fs;
use std::io::Error;
use std::io::ErrorKind;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct DBHandler {
    pub db: Arc<Mutex<DB>>,
}

impl DBHandler {
    pub fn put(&self, key: &[u8], value: &[u8]) -> IoResult<u64> {
        let db = self.db.lock().unwrap()
        db.put(key, value)
    }
    pub fn get(&self, key: &[u8]) -> IoResult<Entry> {
        self.db.read(key)
    }
}

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

    let mut files_id_map: HashMap<DataType, Vec<u32>> = HashMap::new();
    let mut active_files: HashMap<DataType, DataFile> = HashMap::new();
    for n in names.iter() {
        let split_name: Vec<&str> = n.split(".").collect();
        let data_type = match split_name[2] {
            DATA_TYPE_ZSET => DataType::ZSet,
            DATA_TYPE_STR => DataType::String,
            DATA_TYPE_HASH => DataType::Hash,
            DATA_TYPE_LIST => DataType::List,
            DATA_TYPE_SET => DataType::Set,
            _ => continue,
        };
        let id = result_skip_fail!(split_name[0].parse::<u32>());
        let id_vec = files_id_map.entry(data_type).or_insert(Vec::new());
        id_vec.push(id);
    }

    for k in DataType::iterate() {
        let files_map = files_type_map.entry(k).or_insert(HashMap::new());
        match files_id_map.get(&k) {
            Some(v) => {
                active_files.insert(k, DataFile::new(dir_path, v[v.len() - 1], k)?);
                for i in 0..v.len() - 1 {
                    files_map.insert(v[i], DataFile::new(dir_path, v[i], k)?);
                }
            }
            None => {
                active_files.insert(k, DataFile::new(dir_path, 0, k)?);
            }
        }
    }
    Ok((active_files, files_type_map))
}

impl DB {
    pub fn open(options: Options) -> IoResult<DB> {
        //create database dir
        let dir_path = options
            .data_dir
            .to_str()
            .ok_or(Error::from(ErrorKind::Interrupted))?;
        fs::create_dir_all(dir_path).map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        let (active_files, archived_files) = build_data_file(dir_path)?;
        let mut db = DB {
            active_file_map: active_files,
            archived_files: archived_files,
            indexes: HashMap::new(),
            options: options,
        };
        db.load_index();
        Ok(db)
    }

    fn load_index(&mut self) {
        for (_, files_map) in self.archived_files.iter_mut() {
            let mut id_vec = files_map
                .iter_mut()
                .map(|(_id, _)| _id.clone())
                .collect::<Vec<u32>>();
            id_vec.sort();
            for id in id_vec.iter() {
                let data_file = files_map.get_mut(id).unwrap();
                let mut iter = data_file.iterator();
                let mut offset: u64 = 0;
                loop {
                    let e = iter.next();
                    let entry = match e {
                        Ok(entry) => entry,
                        Err(_) => break,
                    };
                    self.indexes.insert(entry.key.clone(), offset);
                    offset += entry.size() as u64;
                }
            }
        }
        for (_, file) in self.active_file_map.iter_mut() {
            let mut iter = file.iterator();
            let mut offset: u64 = 0;
            loop {
                let e = iter.next();
                let entry = match e {
                    Ok(entry) => entry,
                    Err(_) => break,
                };
                self.indexes.insert(entry.key.clone(), offset);
                offset += entry.size() as u64;
            }
            file.offset = offset;
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> IoResult<u64> {
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
    //
    fn store(&self, e: &Entry) -> IoResult<u64> {
        let sz = e.size() as u64;
        let active_file_id: u32;
        {
            let active_data_file = self
                .active_file_map
                .get_mut(&DataType::String)
                .ok_or(Error::from(ErrorKind::Interrupted))?;
            if active_data_file.offset + sz < self.options.file_size {
                let offset = active_data_file.put(e)?;
                return Ok(offset);
            }
            active_data_file.sync()?;
            active_file_id = active_data_file.file_id;
        }
        let files_map = self
            .archived_files
            .get_mut(&DataType::String)
            .ok_or(Error::from(ErrorKind::Interrupted))?;
        let data_file = self
            .active_file_map
            .remove(&DataType::String)
            .ok_or(Error::from(ErrorKind::Interrupted))?;
        files_map.insert(active_file_id, data_file);
        let active_file_id = active_file_id + 1;
        let dir_path = self
            .options
            .data_dir
            .to_str()
            .ok_or(Error::from(ErrorKind::Interrupted))?;
        let mut active_data_file = DataFile::new(dir_path, active_file_id, DataType::String)?;
        let offset = active_data_file.put(e)?;
        self.active_file_map
            .insert(DataType::String, active_data_file);
        Ok(offset)
    }

    pub fn read(&self, key: &[u8]) -> IoResult<Entry> {
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
        let mut d = DB::open(c).unwrap();
        d.put("a".as_bytes(), "aaabbbccccccfffffff".as_bytes())
            .unwrap();
        let value = d.read("a".as_bytes()).unwrap().value;
        println!("{:?}", String::from_utf8(value).unwrap());
    }
    #[test]
    fn test_read() {
        let c = Options::default();
        let mut d = DB::open(c).unwrap();
        let value = d.read("a".as_bytes()).unwrap().value;
        println!("{:?}", String::from_utf8(value).unwrap());
    }
    #[test]
    fn test_build_data_file() {
        let (active_file_map, archived_files) = build_data_file("./db").unwrap();
        for (k, v) in active_file_map {
            println!("{:?},{:?}", k, v.file_id);
        }

        for (k, v) in archived_files {
            println!("{:?}", k);
            for (k1, v1) in v {
                println!("{:?},{:?}", k1, v1.file_id);
            }
        }
    }
}
