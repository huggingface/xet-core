use std::sync::Arc;

use thiserror::Error;

/// Errors that can occur during file reconstruction.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum FileReconstructionError {
    #[error("CAS Client Error: {0}")]
    CasClientError(Arc<cas_client::CasClientError>),

    #[error("IO Error: {0}")]
    IoError(Arc<std::io::Error>),

    #[error("Task Runtime Error: {0}")]
    TaskRuntimeError(Arc<utils::RwTaskLockError>),

    #[error("Corrupted Reconstruction: {0}")]
    CorruptedReconstruction(String),

    #[error("Configuration Error: {0}")]
    ConfigurationError(String),

    #[error("Internal Writer Error: {0}")]
    InternalWriterError(String),

    #[error("Internal Error: {0}")]
    InternalError(String),

    #[error("Task Join Error: {0}")]
    TaskJoinError(Arc<tokio::task::JoinError>),

    #[error("Runtime Error: {0}")]
    RuntimeError(Arc<xet_runtime::errors::MultithreadedRuntimeError>),
}

pub type Result<T> = std::result::Result<T, FileReconstructionError>;

impl From<std::io::Error> for FileReconstructionError {
    fn from(err: std::io::Error) -> Self {
        FileReconstructionError::IoError(Arc::new(err))
    }
}

impl From<cas_client::CasClientError> for FileReconstructionError {
    fn from(err: cas_client::CasClientError) -> Self {
        FileReconstructionError::CasClientError(Arc::new(err))
    }
}

impl From<utils::RwTaskLockError> for FileReconstructionError {
    fn from(err: utils::RwTaskLockError) -> Self {
        FileReconstructionError::TaskRuntimeError(Arc::new(err))
    }
}

impl From<tokio::task::JoinError> for FileReconstructionError {
    fn from(err: tokio::task::JoinError) -> Self {
        FileReconstructionError::TaskJoinError(Arc::new(err))
    }
}

impl From<xet_runtime::errors::MultithreadedRuntimeError> for FileReconstructionError {
    fn from(err: xet_runtime::errors::MultithreadedRuntimeError) -> Self {
        FileReconstructionError::RuntimeError(Arc::new(err))
    }
}
