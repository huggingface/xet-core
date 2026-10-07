use std::fmt::Display;

use thiserror::Error;
use xet_client::ClientError;
use xet_data::DataError;

use crate::lfs_agent_protocol::GitLFSProtocolError;

#[derive(Error, Debug)]
pub enum GitXetError {
    #[error(transparent)]
    GitExtension(#[from] git_extension_common::GitExtensionError),

    #[error("Invalid Git config: {0}")]
    InvalidGitConfig(String),

    #[error("Invalid LFS protocol: {0}")]
    InvalidGitLFSProtocol(#[from] GitLFSProtocolError),

    #[error("Operation not supported: {0}")]
    NotSupported(String),

    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Transfer agent error: {0}")]
    TransferAgent(#[from] DataError),

    #[error("Client error: {0}")]
    Client(#[from] ClientError),
}

pub type Result<T> = std::result::Result<T, GitXetError>;

impl GitXetError {
    pub(crate) fn not_supported(e: impl Display) -> GitXetError {
        GitXetError::NotSupported(e.to_string())
    }

    pub(crate) fn config_error(e: impl Display) -> GitXetError {
        GitXetError::InvalidGitConfig(e.to_string())
    }

    pub(crate) fn internal(e: impl Display) -> GitXetError {
        GitXetError::Internal(e.to_string())
    }
}
