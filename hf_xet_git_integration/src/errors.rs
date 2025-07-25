use std::fmt::Display;

use data::errors::DataProcessingError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum GitXetError {
    #[error("Git command failed: {0}")]
    GitCommandError(String),

    #[error("Incorrect LFS protocol: {0}")]
    GitLFSProtocolError(String),

    #[error("Operation not supported: {0}")]
    NotSupported(String),

    #[error("Serde to Json failed: {0:?}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Transfer error: {0}")]
    TransferError(#[from] DataProcessingError),
}

pub type Result<T> = std::result::Result<T, GitXetError>;

pub(crate) fn bad_protocol(e: impl Display) -> GitXetError {
    GitXetError::GitLFSProtocolError(e.to_string())
}

pub(crate) fn internal(e: impl Display) -> GitXetError {
    GitXetError::InternalError(e.to_string())
}
