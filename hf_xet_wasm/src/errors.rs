use std::fmt::Debug;

use cas_client::CasClientError;
use cas_object::error::CasObjectError;
use mdb_shard::error::MDBShardError;
use merklehash::DataHashHexParseError;
use thiserror::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum DataProcessingError {
    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Bad hash: {0}")]
    BadHash(#[from] DataHashHexParseError),

    #[error("CAS service error : {0}")]
    CasClientError(#[from] CasClientError),

    #[error("Xorb Serialization error : {0}")]
    XorbSerializationError(#[from] CasObjectError),

    #[error("MerkleDB Shard error: {0}")]
    MDBShardError(#[from] MDBShardError),
}

impl DataProcessingError {
    pub fn internal<T: Debug>(value: T) -> Self {
        DataProcessingError::InternalError(format!("{value:?}"))
    }
}

pub type Result<T> = std::result::Result<T, DataProcessingError>;
