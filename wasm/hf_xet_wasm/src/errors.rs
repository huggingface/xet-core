use std::fmt::Debug;

use thiserror::Error;
use xet_client::ClientError;
use xet_core_structures::CoreError;
use xet_core_structures::merklehash::DataHashHexParseError;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum DataProcessingError {
    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Bad hash: {0}")]
    BadHash(#[from] DataHashHexParseError),

    #[error("Client error: {0}")]
    ClientError(#[from] ClientError),

    #[error("Core structures error: {0}")]
    CoreError(#[from] CoreError),
}

impl DataProcessingError {
    pub fn internal<T: Debug>(value: T) -> Self {
        DataProcessingError::InternalError(format!("{value:?}"))
    }
}

pub type Result<T> = std::result::Result<T, DataProcessingError>;
