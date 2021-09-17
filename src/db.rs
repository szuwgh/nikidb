use crate::datafile::DataFile;
use crate::error::Error;
use crate::error::ResultError;
use std::collections::HashMap;
use std::fs;
pub struct DB {
    // active file:
    active_data_file: DataFile,
    //memory index message
    indexes: HashMap<Vec<u8>, i64>,
}

impl DB {
    pub fn open(dirPath: &str) -> ResultError<DB> {
        //create database dir
        fs::create_dir_all(dirPath).map_err(|err| Error::IoFs { err: err })?;
        let db = DB {
            active_data_file: DataFile::new(dirPath)?,
            indexes: HashMap::new(),
        };
        Ok(db)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> ResultError<()> {
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
