use std::time::{SystemTime, UNIX_EPOCH};

pub fn get_time_unix_nano() -> i64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let ms = since_the_epoch.as_secs() as i64 * 1000i64
        + (since_the_epoch.subsec_nanos() as f64 / 1_000_000.0) as i64;
    ms
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_get_time_unix_nano() {
        let t = get_time_unix_nano();
        println!("time is {}", t);
    }
}
