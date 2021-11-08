use std::path::PathBuf;

//default a data file size
const DEFAULT_FILE_SIZE: u64 = 60; //16 * 1024 * 1024;
const ARCHIEVD_LIMIT_NUM: u32 = 2;

macro_rules! data_type_enum {
    ($visibility:vis, $name:ident, $($member:tt),*) => {
        #[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
        $visibility enum $name {$($member),*}
        impl $name {
            pub fn iterate() -> Vec<$name> {
                vec![$($name::$member,)*]
            }
        }
    };
    // ($name:ident, $($member:tt),*) => {
    //     data_type_enum!(, $name, $($member),*)
    // };
}

data_type_enum!(pub, DataType, String, List, Hash, Set, ZSet);

pub const DATA_TYPE_ACTIVE: &str = "a";
pub const DATA_TYPE_MEGRE: &str = "m";
pub const DATA_TYPE_SSTABLE: &str = "s";
// pub const DATA_TYPE_HASH: &str = "hash";
// pub const DATA_TYPE_SET: &str = "set";
// pub const DATA_TYPE_ZSET: &str = "zset";

#[derive(Clone)]
pub struct Options {
    pub file_size: u64,
    pub data_dir: PathBuf,
    pub archievd_limit_num: u32,
}

impl Options {
    pub fn default() -> Self {
        Self {
            file_size: DEFAULT_FILE_SIZE,
            data_dir: PathBuf::from("./db"),
            archievd_limit_num: ARCHIEVD_LIMIT_NUM,
        }
    }
}
