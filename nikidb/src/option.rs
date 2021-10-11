use std::path::PathBuf;

const DEFAULT_FILE_SIZE: u64 = 16 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum DataType {
    String,
    List,
    Hash,
    Set,
    ZSet,
}

pub const DATA_TYPE_STR: &str = "str";
pub const DATA_TYPE_LIST: &str = "list";
pub const DATA_TYPE_HASH: &str = "hash";
pub const DATA_TYPE_SET: &str = "set";
pub const DATA_TYPE_ZSET: &str = "zset";

pub struct Options {
    pub file_size: u64,
    pub data_dir: PathBuf,
}

impl Options {
    pub fn default() -> Self {
        Self {
            file_size: DEFAULT_FILE_SIZE,
            data_dir: PathBuf::from("./db"),
        }
    }
}
