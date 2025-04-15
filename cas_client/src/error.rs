use std::num::TryFromIntError;

use anyhow::anyhow;
use merklehash::MerkleHash;
use thiserror::Error;
use tokio::sync::{mpsc::error::SendError, AcquireError};
use tokio::task::JoinError;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum CasClientError {
    #[error("ChunkCache Error: {0}")]
    ChunkCache(#[from] chunk_cache::error::ChunkCacheError),

    #[error("Cas Object Error: {0}")]
    CasObjectError(#[from] cas_object::error::CasObjectError),

    #[error("Configuration Error: {0} ")]
    ConfigurationError(String),

    #[error("Invalid Range")]
    InvalidRange,

    #[error("Invalid Arguments")]
    InvalidArguments,

    #[error("File not found for hash: {0}")]
    FileNotFound(MerkleHash),

    #[error("IO Error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("Invalid Shard Key: {0}")]
    InvalidShardKey(String),

    #[error("Other Internal Error: {0}")]
    InternalError(#[from] anyhow::Error),

    #[error("MerkleDB Shard Error : {0}")]
    MDBShardError(#[from] mdb_shard::error::MDBShardError),

    #[error("Error : {0}")]
    Other(String),

    #[error("Parse Error: {0}")]
    ParseError(#[from] url::ParseError),

    #[error("ReqwestMiddleware Error: {0}")]
    ReqwestMiddlewareError(#[from] reqwest_middleware::Error),

    #[error("Reqwest Error: {0}")]
    ReqwestError(#[from] reqwest::Error),

    #[error("LMDB Error: {0}")]
    ShardDedupDBError(String),

    #[error("CAS object not found for hash: {0}")]
    XORBNotFound(MerkleHash),

    #[error("Presigned S3 URL Expired on fetching range")]
    PresignedUrlExpirationError,
}

// Define our own result type here (this seems to be the standard).
pub type Result<T> = std::result::Result<T, CasClientError>;

impl PartialEq for CasClientError {
    fn eq(&self, other: &CasClientError) -> bool {
        match (self, other) {
            (CasClientError::XORBNotFound(a), CasClientError::XORBNotFound(b)) => a == b,
            (e1, e2) => std::mem::discriminant(e1) == std::mem::discriminant(e2),
        }
    }
}

impl From<utils::errors::SingleflightError<CasClientError>> for CasClientError {
    fn from(value: utils::singleflight::SingleflightError<CasClientError>) -> Self {
        match value {
            utils::singleflight::SingleflightError::InternalError(e) => e,
            e => CasClientError::Other(format!("single flight error: {e}")),
        }
    }
}

impl<T> From<std::sync::PoisonError<T>> for CasClientError {
    fn from(value: std::sync::PoisonError<T>) -> Self {
        CasClientError::InternalError(anyhow!("{value:?}"))
    }
}

impl From<AcquireError> for CasClientError {
    fn from(value: AcquireError) -> Self {
        CasClientError::InternalError(anyhow!("{value:?}"))
    }
}

impl<T> From<SendError<T>> for CasClientError {
    fn from(value: SendError<T>) -> Self {
        CasClientError::InternalError(anyhow!("{value:?}"))
    }
}

impl From<JoinError> for CasClientError {
    fn from(value: JoinError) -> Self {
        CasClientError::InternalError(anyhow!("{value:?}"))
    }
}

impl From<TryFromIntError> for CasClientError {
    fn from(value: TryFromIntError) -> Self {
        CasClientError::InternalError(anyhow!("{value:?}"))
    }
}
