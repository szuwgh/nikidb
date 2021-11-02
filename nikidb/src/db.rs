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
use std::mem;
use std::sync::{Arc, Mutex, RwLock};

trait LevelGet {}

trait LevelPut {}

#[derive(Clone)]
pub struct DbDropGuard {
    db: DB,
}

impl DbDropGuard {
    pub fn new(options: Options) -> DbDropGuard {
        DbDropGuard {
            db: DB::open(options).unwrap(),
        }
    }
    pub fn db(&self) -> DB {
        self.db.clone()
    }
}

// #[derive(Clone)]
// pub struct DBHandler {
//     pub db: DB,
// }

// impl DBHandler {
//     pub fn new(options: Options) -> DBHandler {
//         let db = DB::open(options).unwrap();
//         DBHandler { db }
//     }
//     pub fn put(&self, key: &[u8], value: &[u8]) {
//         // let mut db = self.db.lock().unwrap();
//         //db.put(key, value)
//         //  drop(db);
//     }
//     pub fn get(&self, key: &[u8]) {
//         // let mut db = self.db.lock().unwrap();
//         // db.read(key)
//         // drop(db);
//     }
// }

#[derive(Clone)]
pub struct DB {
    levels: Arc<RwLock<Levels>>,
    //db config
    options: Options,
}

struct Levels {
    active_level: Arc<RwLock<ActiveUnit>>,

    archived_level: Vec<ArchivedUnit>,

    merged_level: Vec<MergeUnit>,
}

impl Levels {
    fn new(file_size: u64, dir_path: String) -> IoResult<Levels> {
        let levels = Levels {
            active_level: Arc::new(RwLock::new(ActiveUnit::new(file_size, dir_path)?)),
            archived_level: Vec::new(),
            merged_level: Vec::new(),
        };
        Ok(levels)
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> IoResult<u64> {
        let mut active = self.active_level.write().unwrap();
        active.put(key, value)
    }

    fn get(&mut self, key: &[u8]) -> IoResult<Entry> {
        let mut active = self.active_level.write().unwrap();
        active.get(key)
    }
}

struct ActiveUnit {
    //active file
    active_file: DataFile,
    //archived files
    archived_files: HashMap<u32, DataFile>,
    //memory index message
    indexes: HashMap<Vec<u8>, u64>,
    //every one file size,
    file_size: u64,
    //data dir path
    data_dir: String,
}

impl ActiveUnit {
    fn new(file_size: u64, data_dir: String) -> IoResult<ActiveUnit> {
        let (active_file, archived_files) = build_data_file(&data_dir)?;
        let mut active_unit = ActiveUnit {
            active_file: active_file,
            archived_files: archived_files,
            indexes: HashMap::new(),
            file_size: file_size,
            data_dir: data_dir,
        };
        active_unit.load_index();
        Ok(active_unit)
    }

    fn load_index(&mut self) {
        let mut id_vec = self
            .archived_files
            .iter_mut()
            .map(|(_id, _)| _id.clone())
            .collect::<Vec<u32>>();
        id_vec.sort();
        for id in id_vec.iter() {
            let data_file = self.archived_files.get_mut(id).unwrap();
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

        let mut iter = self.active_file.iterator();
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
        self.active_file.offset = offset;
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> IoResult<u64> {
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
        let active_file_id: u32;
        {
            if self.active_file.offset + sz < self.file_size {
                let offset = self.active_file.put(e)?;
                return Ok(offset);
            }
            self.active_file.sync()?;
            active_file_id = self.active_file.file_id;
        }
        // let files_map = self
        //     .archived_files
        //     .get_mut(&DataType::String)
        //     .ok_or(Error::from(ErrorKind::Interrupted))?;
        // let data_file = self
        //     .active_file_map
        //     .remove(&DataType::String)
        //     .ok_or(Error::from(ErrorKind::Interrupted))?;
        // let data_file = self.active_file;
        let active_file_id = active_file_id + 1;
        // let dir_path = self
        //     .data_dir
        //     .to_str()
        //     .ok_or(Error::from(ErrorKind::Interrupted))?;
        let mut new_active_data_file =
            DataFile::new(&self.data_dir, active_file_id, DataType::String)?;
        let offset = new_active_data_file.put(e)?;

        let old_active_data_file = mem::replace(&mut self.active_file, new_active_data_file);

        self.archived_files
            .insert(active_file_id, old_active_data_file);
        // self.active_file = active_data_file;
        // self.active_file_map
        //     .insert(DataType::String, active_data_file);
        Ok(offset)
    }

    fn get(&mut self, key: &[u8]) -> IoResult<Entry> {
        let offset = self
            .indexes
            .get(&key.to_vec())
            .ok_or(Error::from(ErrorKind::Interrupted))?;
        self.active_file.read(*offset)
    }
}

struct ArchivedUnit {
    archived_files: HashMap<u32, DataFile>,
    //memory index message
    indexes: HashMap<Vec<u8>, u64>,
}

impl ArchivedUnit {
    fn get() {}
}

struct MergeUnit {
    archived_files: DataFile,
    indexes: HashMap<Vec<u8>, u64>,
}

impl MergeUnit {
    fn get() {}
}

struct SSTableUnit {}

fn build_data_file(dir_path: &str) -> IoResult<(DataFile, HashMap<u32, DataFile>)> {
    let dir = fs::read_dir(dir_path)?;

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
    let mut files_map: HashMap<u32, DataFile> = HashMap::new();
    if names.len() == 0 {
        let active_file = DataFile::new(dir_path, 0, DataType::String)?;
        return Ok((active_file, files_map));
    }
    let mut files: Vec<u32> = Vec::new();
    // let mut files_id_map: HashMap<DataType, Vec<u32>> = HashMap::new();
    // let mut active_files: HashMap<DataType, DataFile> = HashMap::new();
    for n in names.iter() {
        let split_name: Vec<&str> = n.split(".").collect();
        // let data_type = match split_name[2] {
        //     DATA_TYPE_ZSET => DataType::ZSet,
        //     DATA_TYPE_STR => DataType::String,
        //     DATA_TYPE_HASH => DataType::Hash,
        //     DATA_TYPE_LIST => DataType::List,
        //     DATA_TYPE_SET => DataType::Set,
        //     _ => continue,
        // };
        let id = result_skip_fail!(split_name[0].parse::<u32>());
        // let id_vec = files_id_map.entry(data_type).or_insert(Vec::new());
        files.push(id);
    }

    let active_file = DataFile::new(dir_path, files[files.len() - 1], DataType::String)?;
    for i in 0..files.len() - 1 {
        files_map.insert(
            files[i],
            DataFile::new(dir_path, files[i], DataType::String)?,
        );
    }

    //  active_files.insert(k, DataFile::new(dir_path, 0, k)?);

    Ok((active_file, files_map))
}

impl DB {
    pub fn open(options: Options) -> IoResult<DB> {
        //create database dir
        let dir_path = options
            .data_dir
            .to_str()
            .ok_or(Error::from(ErrorKind::Interrupted))?;
        fs::create_dir_all(dir_path).map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        //let (active_files, archived_files) = build_data_file(dir_path)?;
        let data_dir = options
            .data_dir
            .to_str()
            .ok_or(Error::from(ErrorKind::Interrupted))?;
        let mut db = DB {
            levels: Arc::new(RwLock::new(Levels::new(
                options.file_size,
                data_dir.to_owned(),
            )?)),
            // active_file_map: active_files,
            // archived_files: archived_files,
            // indexes: HashMap::new(),
            options: options,
        };
        //    db.load_index();
        Ok(db)
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> IoResult<u64> {
        let mut levels = self.levels.write().unwrap();
        levels.put(key, value)
    }

    fn get(&mut self, key: &[u8]) -> IoResult<Entry> {
        let mut levels = self.levels.write().unwrap();
        levels.get(key)
    }

    // fn load_index(&mut self) {
    //     for (_, files_map) in self.archived_files.iter_mut() {
    //         let mut id_vec = files_map
    //             .iter_mut()
    //             .map(|(_id, _)| _id.clone())
    //             .collect::<Vec<u32>>();
    //         id_vec.sort();
    //         for id in id_vec.iter() {
    //             let data_file = files_map.get_mut(id).unwrap();
    //             let mut iter = data_file.iterator();
    //             let mut offset: u64 = 0;
    //             loop {
    //                 let e = iter.next();
    //                 let entry = match e {
    //                     Ok(entry) => entry,
    //                     Err(_) => break,
    //                 };
    //                 self.indexes.insert(entry.key.clone(), offset);
    //                 offset += entry.size() as u64;
    //             }
    //         }
    //     }
    //     for (_, file) in self.active_file_map.iter_mut() {
    //         let mut iter = file.iterator();
    //         let mut offset: u64 = 0;
    //         loop {
    //             let e = iter.next();
    //             let entry = match e {
    //                 Ok(entry) => entry,
    //                 Err(_) => break,
    //             };
    //             self.indexes.insert(entry.key.clone(), offset);
    //             offset += entry.size() as u64;
    //         }
    //         file.offset = offset;
    //     }
    // }

    // pub fn put(&mut self, key: &[u8], value: &[u8]) -> IoResult<u64> {
    //     let e = Entry {
    //         timestamp: time::get_time_unix_nano() as u64,
    //         key: key.to_vec(),
    //         value: value.to_vec(),
    //         crc: 0,
    //     };
    //     let offset = self.store(&e)?;
    //     self.indexes.insert(e.key, offset);
    //     Ok(offset)
    // }
    // //

    // pub fn read(&mut self, key: &[u8]) -> IoResult<Entry> {
    //     let offset = self
    //         .indexes
    //         .get(&key.to_vec())
    //         .ok_or(Error::from(ErrorKind::Interrupted))?;
    //     let active_data_file = self
    //         .active_file_map
    //         .get_mut(&DataType::String)
    //         .ok_or(Error::from(ErrorKind::Interrupted))?;
    //     active_data_file.read(*offset)
    // }

    pub fn merge() {}
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_put() {
        let c = Options::default();
        let mut d = DB::open(c).unwrap();
        d.put("a".as_bytes(), "aaabbbccccccfffffffeeee".as_bytes())
            .unwrap();
        let value = d.get("a".as_bytes()).unwrap().value;
        println!("{:?}", String::from_utf8(value).unwrap());
    }
    #[test]
    fn test_read() {
        let c = Options::default();
        let mut d = DB::open(c).unwrap();
        let value = d.get("a".as_bytes()).unwrap().value;
        println!("{:?}", String::from_utf8(value).unwrap());
    }
    // fn test_build_data_file() {
    //     let (active_file_map, archived_files) = build_data_file("./db").unwrap();
    //     for (k, v) in active_file_map {
    //         println!("{:?},{:?}", k, v.file_id);
    //     }

    //     for (k, v) in archived_files {
    //         println!("{:?}", k);
    //         for (k1, v1) in v {
    //             println!("{:?},{:?}", k1, v1.file_id);
    //         }
    //     }
    // }
}
