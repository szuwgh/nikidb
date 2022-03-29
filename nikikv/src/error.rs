use std::io;
use thiserror::Error;
pub type NKResult<T> = Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("open db fail :{0}")]
    OpenDBFail(io::Error),
}
