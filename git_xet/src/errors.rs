use std::fmt::Display;

use data::errors::DataProcessingError;
use thiserror::Error;

use crate::lfs_agent_protocol::GitLFSProtocolError;

#[derive(Error, Debug)]
pub enum GitXetError {
    #[error("Git command failed: {0}")]
    GitCommandError(String),

    #[error("Incorrect LFS protocol: {0}")]
    GitLFSProtocolError(#[from] GitLFSProtocolError),

    #[error("Operation not supported: {0}")]
    NotSupported(String),

    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Transfer agent error: {0}")]
    TransferAgentError(#[from] DataProcessingError),
}

pub type Result<T> = std::result::Result<T, GitXetError>;

pub(crate) fn internal(e: impl Display) -> GitXetError {
    GitXetError::InternalError(e.to_string())
}
