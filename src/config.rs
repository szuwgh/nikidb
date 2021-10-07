const default_file_size: u64 = 16 * 1024 * 1024;

pub struct Config {
    pub file_size: u64,
}

impl Config {
    pub fn default() -> Self {
        Self {
            file_size: default_file_size,
        }
    }
}
