use crate::datafile::DataFile;
use std::collections::HashMap;
use std::fs;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
pub struct DB {
    // active file:
    active_data_file: DataFile,
    //memory index message
    indexes: HashMap<Vec<u8>, (u64, usize)>,
}

impl DB {
    pub fn open(dirPath: &str) -> Result<DB> {
        //create database dir
        fs::create_dir_all(dirPath).map_err(|err| Error::new(ErrorKind::Interrupted, err))?;
        let db = DB {
            active_data_file: DataFile::new(dirPath)?,
            indexes: HashMap::new(),
        };
        Ok(db)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.len() == 0 {
            //Ok(())
        }
        //
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
