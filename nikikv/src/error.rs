use std::fmt;
use std::io;
use std::io::Error as IOError;
use thiserror::Error;
pub type NKResult<T> = Result<T, NKError>;

#[derive(Error, Debug)]
pub enum NKError {
    #[error("Unexpected: {0}, {1}")]
    UnexpectIO(String, io::Error),
    #[error("Unexpected: {0}")]
    Unexpected(String),
    #[error("db open fail: {0}")]
    DBOpenFail(io::Error),
    #[error("invalid database")]
    ErrInvalid,
    #[error("version mismatch")]
    ErrVersionMismatch,
    #[error("checksum error")]
    ErrChecksum,
}

impl From<&str> for NKError {
    fn from(e: &str) -> Self {
        NKError::Unexpected(e.to_string())
    }
}

impl From<(&str, io::Error)> for NKError {
    fn from(e: (&str, io::Error)) -> Self {
        NKError::UnexpectIO(e.0.to_string(), e.1)
    }
}

impl From<String> for NKError {
    fn from(e: String) -> Self {
        NKError::Unexpected(e)
    }
}

impl From<IOError> for NKError {
    fn from(e: IOError) -> Self {
        NKError::Unexpected(e.to_string())
    }
}

impl From<NKError> for String {
    fn from(e: NKError) -> Self {
        format!("{}", e)
    }
}
