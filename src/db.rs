use crate::datafile::DataFile;
pub struct DB {
    // active file:
    active_data_file: DataFile,
}

impl DB {
    pub fn open() {
        println!("open db");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
