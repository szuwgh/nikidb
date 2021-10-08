use std::path::PathBuf;

const DEFAULT_FILE_SIZE: u64 = 16 * 1024 * 1024;

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
