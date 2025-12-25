use std::string::FromUtf8Error;
use std::sync::mpsc::RecvError;

use cas_client::CasClientError;
use cas_object::error::CasObjectError;
use file_reconstruction::FileReconstructionError;
use mdb_shard::error::MDBShardError;
use thiserror::Error;
use tokio::sync::AcquireError;
use tracing::error;
use utils::errors::{AuthError, SingleflightError};
use xet_runtime::utils::ParutilsError;

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

    #[error("File Reconstruction Error: {0}")]
    FileReconstructionError(#[from] FileReconstructionError),
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

impl From<ParutilsError<DataProcessingError>> for DataProcessingError {
    fn from(value: ParutilsError<DataProcessingError>) -> Self {
        match value {
            ParutilsError::Join(e) => DataProcessingError::JoinError(e),
            ParutilsError::Acquire(e) => DataProcessingError::PermitAcquisitionError(e),
            ParutilsError::Task(e) => e,
            e => DataProcessingError::InternalError(e.to_string()),
        }
    }
}
