use std::io;
use thiserror::Error;

pub type NKResult<T> = Result<T, NKError>;

#[derive(Error, Debug)]
pub enum NKError {
    #[error("open db fail :{0}")]
    OpenDBFail(io::Error),
}
