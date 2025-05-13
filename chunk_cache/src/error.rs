use std::array::TryFromSliceError;
use std::str::Utf8Error;

use base64::DecodeError;
use merklehash::DataHashBytesParseError;
use thiserror::Error;
use tokio::task::JoinError;

#[derive(Debug, Error)]
pub enum ChunkCacheError {
    #[error("General: {0}")]
    General(String),
    #[error("IO: {0}")]
    IO(#[from] std::io::Error),
    #[error("ParseError: {0}")]
    Parse(String),
    #[error("bad range")]
    BadRange,
    #[error("cache is empty when it is presumed no empty")]
    CacheEmpty,
    #[error("Infallible")]
    Infallible,
    #[error("LockPoison")]
    LockPoison,
    #[error("invalid arguments")]
    InvalidArguments,

    #[error("RuntimeError")]
    RuntimeError(#[from] JoinError),
}

impl ChunkCacheError {
    pub fn parse<T: ToString>(value: T) -> ChunkCacheError {
        ChunkCacheError::Parse(value.to_string())
    }

    pub fn general<T: ToString>(value: T) -> ChunkCacheError {
        ChunkCacheError::General(value.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for ChunkCacheError {
    fn from(_value: std::sync::PoisonError<T>) -> Self {
        ChunkCacheError::LockPoison
    }
}

macro_rules! impl_parse_error_from_error {
    ($error_type:ty) => {
        impl From<$error_type> for ChunkCacheError {
            fn from(value: $error_type) -> Self {
                ChunkCacheError::parse(value)
            }
        }
    };
}

impl_parse_error_from_error!(TryFromSliceError);
impl_parse_error_from_error!(DecodeError);
impl_parse_error_from_error!(DataHashBytesParseError);
impl_parse_error_from_error!(Utf8Error);
