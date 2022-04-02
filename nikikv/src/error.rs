use std::io;
use thiserror::Error;

pub type NKResult<T> = Result<T, NKError>;

#[derive(Error, Debug)]
pub enum NKError {
    #[error("io fail :{0}")]
    IoFail(io::Error),
}
