use std::string::FromUtf8Error;
use std::sync::mpsc::RecvError;

use thiserror::Error;
use tokio::sync::AcquireError;
use tracing::error;
use xet_client::ClientError;
use xet_client::cas_client::auth::AuthError;
use xet_core_structures::CoreError;
use xet_core_structures::merklehash::DataHashHexParseError;
use xet_runtime::RuntimeError;
use xet_runtime::core::par_utils::ParutilsError;
use xet_runtime::utils::errors::SingleflightError;

use crate::file_reconstruction::FileReconstructionError;

#[derive(Error, Debug)]
pub enum DataError {
    #[error("File query policy configuration error: {0}")]
    FileQueryPolicyError(String),

    #[error("CAS configuration error: {0}")]
    CASConfigError(String),

    #[error("Shard configuration error: {0}")]
    ShardConfigError(String),

    #[error("Deduplication configuration error: {0}")]
    DedupConfigError(String),

    #[error("Clean task error: {0}")]
    CleanTaskError(String),

    #[error("Upload task error: {0}")]
    UploadTaskError(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Synchronization error: {0}")]
    SyncError(String),

    #[error("Channel error: {0}")]
    ChannelRecvError(#[from] RecvError),

    #[error("Core structures error: {0}")]
    FormatError(#[from] CoreError),

    #[error("Client error: {0}")]
    ClientError(#[from] ClientError),

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
    HashStringParsingFailure(#[from] DataHashHexParseError),

    #[error("Deprecated feature: {0}")]
    DeprecatedError(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("File size mismatch: expected {expected} bytes but downloaded {actual} bytes")]
    SizeMismatch { expected: u64, actual: u64 },
    #[error("Auth error: {0}")]
    AuthError(#[from] AuthError),

    #[error("Permit acquisition error: {0}")]
    PermitAcquisitionError(#[from] AcquireError),

    #[error("File reconstruction error: {0}")]
    FileReconstructionError(#[from] FileReconstructionError),

    #[error("Runtime error: {0}")]
    RuntimeError(#[from] RuntimeError),
}

pub type Result<T> = std::result::Result<T, DataError>;

impl From<SingleflightError<DataError>> for DataError {
    fn from(value: SingleflightError<DataError>) -> Self {
        let msg = format!("{value:?}");
        error!("{msg}");
        match value {
            SingleflightError::InternalError(e) => e,
            _ => DataError::InternalError(format!("SingleflightError: {msg}")),
        }
    }
}

impl From<ParutilsError<DataError>> for DataError {
    fn from(value: ParutilsError<DataError>) -> Self {
        match value {
            ParutilsError::Join(e) => DataError::JoinError(e),
            ParutilsError::Acquire(e) => DataError::PermitAcquisitionError(e),
            ParutilsError::Task(e) => e,
            e => DataError::InternalError(e.to_string()),
        }
    }
}
