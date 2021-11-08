use crate::option_skip_fail;
use std::fs;
pub fn next_sequence_file(dir_path: &str) -> Option<u32> {
    let dir = fs::read_dir(dir_path).ok()?;
    let mut i: u32 = 0;
    for path in dir {
        i = option_skip_fail!(path.ok().and_then(|e| {
            e.path().file_name().and_then(|n| {
                n.to_str().and_then(|s| {
                    if s.contains(".data") {
                        let split_name: Vec<&str> = s.split(".").collect();
                        return split_name[0].parse::<u32>().ok();
                    }
                    None
                })
            })
        }));
    }
    i = i + 1;
    Some(i)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_next_sequence_file() {
        next_sequence_file("E:\\rustproject\\nikidb\\nikidb\\db");
    }
}
