use crate::datafile::DataFile;
use crate::datafile::Entry;
use crate::error::IoResult;
use crate::option::Options;
use crate::util::time;
use std::collections::HashMap;
use std::fs;
use std::io::Error;
use std::io::ErrorKind;

pub struct DB {
    // active file:
    active_data_file: DataFile,
    // archived_files: HashMap<u32, DataFile>,
    //memory index message
    indexes: HashMap<String, u64>,
    //db config
    options: Options,
}

// let path = Path::new("./");
// for entry in fs::read_dir(path).expect("读取目录失败") {
//     if let Ok(entry) = entry {
//         let file = entry.path();
//         let filename = file.to_str().unwrap();
//         let new_filename = format!("{}.jpg", filename);
//         match fs::rename(filename, &new_filename) {
//             Err(why) => panic!("{} => {}: {}", filename, new_filename, why.description()),
//             Ok(_) => println!("{} => {}", filename, new_filename),
//         }
//     }
// }

// let names =
// paths.filter_map(|entry| {
//   entry.ok().and_then(|e|
//     e.path().file_name()
//     .and_then(|n| n.to_str().map(|s| String::from(s)))
//   )
// }).collect::<Vec<String>>();
// fn build(dir_path: &str) -> IoResult<(DataFile, HashMap<u32, DataFile>)> {
//     //let files_map
//     let dir = fs::read_dir(dir_path)?;
//     for path in dir {
//         let p = path.unwrap().path();
//         let file_name = String::from(p.file_name().unwrap());
//         let splic_name = file_name.split(".").collect();

//         // splict
//     }
// }

impl DB {
    pub fn open(dir_path: &str, options: Options) -> IoResult<DB> {
        //create database dir
        fs::create_dir_all(dir_path).map_err(|err| Error::new(ErrorKind::Interrupted, err))?;

        let mut db = DB {
            active_data_file: DataFile::new(dir_path, 2)?,
            indexes: HashMap::new(),
            options: options,
        };
        db.load_index();
        Ok(db)
    }

    // fn get_active_file() -> DataFile {}

    fn load_index(&mut self) {
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
        self.active_data_file.offset = offset;
    }

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
        self.indexes
            .insert(String::from_utf8(e.key).unwrap(), offset);
        Ok(offset)
    }

    fn store(&mut self, e: &Entry) -> IoResult<u64> {
        let sz = e.size() as u64;
        if self.active_data_file.offset + sz > self.options.file_size {
            self.active_data_file.sync()?;
            //self.active_data_file.close();
        }
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
        let c = Options::default();
        let mut d = DB::open("./db", c).unwrap();
        d.put("a".as_bytes(), "aaabbbcccccc".as_bytes()).unwrap();
    }
    #[test]
    fn test_read_dir() {
        let dir = fs::read_dir("D:\\rsproject\\photon\\nikidb\\db").unwrap();
        for path in dir {
            let p = path.unwrap().path();
            let file_name = String::from(p.file_name().unwrap().to_str().unwrap());
            let split_name: Vec<&str> = file_name.split(".").collect();
            let id = split_name[0] as u32;
            println!("{:?}", id);
        }
    }
}
