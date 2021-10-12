#[macro_export]
#[warn(unused_imports)]
macro_rules! result_skip_fail {
    ($res:expr) => {
        match $res {
            Ok(val) => val,
            Err(e) => {
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
