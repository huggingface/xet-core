use std::array::TryFromSliceError;

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
}

impl ChunkCacheError {
    pub fn parse<T: ToString>(value: T) -> ChunkCacheError {
        ChunkCacheError::Parse(value.to_string())
    }
}

impl From<TryFromSliceError> for ChunkCacheError {
    fn from(value: TryFromSliceError) -> Self {
        ChunkCacheError::parse(value)
    }
}
