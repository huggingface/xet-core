use std::{array::TryFromSliceError, str::Utf8Error};

use base64::DecodeError;
use merklehash::DataHashBytesParseError;
use xet_error::Error;

#[derive(Debug, Error)]
pub enum ChunkCacheError {
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
}

impl ChunkCacheError {
    pub fn parse<T: ToString>(value: T) -> ChunkCacheError {
        ChunkCacheError::Parse(value.to_string())
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
