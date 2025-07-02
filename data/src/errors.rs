use std::string::FromUtf8Error;
use std::sync::mpsc::RecvError;

use cas_client::CasClientError;
use cas_object::error::CasObjectError;
use mdb_shard::error::MDBShardError;
use merkledb::error::MerkleDBError;
use thiserror::Error;
use tokio::sync::AcquireError;
use tracing::error;
use utils::errors::{AuthError, SingleflightError};

#[derive(Error, Debug)]
pub enum DataProcessingError {
    #[error("File query policy configuration error: {0}")]
    FileQueryPolicyError(String),

    #[error("CAS configuration error: {0}")]
    CASConfigError(String),

    #[error("Shard configuration error: {0}")]
    ShardConfigError(String),

    #[error("Cache configuration error: {0}")]
    CacheConfigError(String),

    #[error("Deduplication configuration error: {0}")]
    DedupConfigError(String),

    #[error("Clean task error: {0}")]
    CleanTaskError(String),

    #[error("Upload task error: {0}")]
    UploadTaskError(String),

    #[error("Internal error : {0}")]
    InternalError(String),

    #[error("Synchronization error: {0}")]
    SyncError(String),

    #[error("Channel error: {0}")]
    ChannelRecvError(#[from] RecvError),

    #[error("MerkleDB error: {0}")]
    MerkleDBError(#[from] MerkleDBError),

    #[error("MerkleDB Shard error: {0}")]
    MDBShardError(#[from] MDBShardError),

    #[error("CAS service error : {0}")]
    CasClientError(#[from] CasClientError),

    #[error("Xorb Serialization error : {0}")]
    XorbSerializationError(#[from] CasObjectError),

    #[error("Subtask scheduling error: {0}")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("Non-small file not cleaned: {0}")]
    FileNotCleanedError(#[from] FromUtf8Error),

    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("Hash not found")]
    HashNotFound,

    #[error("Parameter error: {0}")]
    ParameterError(String),

    #[error("Unable to parse string as hex hash value")]
    HashStringParsingFailure(#[from] merklehash::DataHashHexParseError),

    #[error("Deprecated feature: {0}")]
    DeprecatedError(String),

    #[error("AuthError: {0}")]
    AuthError(#[from] AuthError),

    #[error("Permit Acquisition Error: {0}")]
    PermitAcquisitionError(#[from] AcquireError),
}

pub type Result<T> = std::result::Result<T, DataProcessingError>;

// Specific implementation for this one so that we can extract the internal error when appropriate
impl From<SingleflightError<DataProcessingError>> for DataProcessingError {
    fn from(value: SingleflightError<DataProcessingError>) -> Self {
        let msg = format!("{value:?}");
        error!("{msg}");
        match value {
            SingleflightError::InternalError(e) => e,
            _ => DataProcessingError::InternalError(format!("SingleflightError: {msg}")),
        }
    }
}
