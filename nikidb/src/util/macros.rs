#[macro_export]
#[warn(unused_imports)]
macro_rules! result_skip_fail {
    ($res:expr) => {
        match $res {
            Ok(val) => val,
            Err(_) => {
                // warn!("An error: {}; skipped.", e);
                continue;
            }
        }
    };
}

#[macro_export]
#[warn(unused_imports)]
macro_rules! option_skip_fail {
    ($res:expr) => {
        match $res {
            Some(val) => val,
            None => {
                continue;
            }
        }
    };
}

#[macro_export]
#[warn(unused_imports)]
macro_rules! data_file_format {
    ($ext:expr,$file_id:expr) => {
        format!("{:09}.data.{}", $file_id, $ext).to_lowercase()
    };
}
