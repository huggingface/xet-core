use std::fmt::Display;
use std::path::PathBuf;

use cas_client::CasClientError;
use data::errors::DataProcessingError;
use thiserror::Error;

use crate::lfs_agent_protocol::GitLFSProtocolError;

#[derive(Error, Debug)]
pub enum GitXetError {
    #[error("Git command failed: {reason}, {source:?}")]
    GitCommandFailed {
        reason: String,
        source: Option<std::io::Error>,
    },

    #[error("Failed to find Git repo at {path}, internal error {source}")]
    NoGitRepo { path: PathBuf, source: git2::Error },

    #[error("Internal Git error: {0}")]
    GitError(#[from] git2::Error),

    #[error("Invalid Git config: {0}")]
    InvalidGitConfig(String),

    #[error("Invalid Git URL: {0}")]
    InvalidGitUrl(#[from] git_url_parse::GitUrlParseError),

    #[error("Invalid LFS protocol: {0}")]
    InvalidGitLFSProtocol(#[from] GitLFSProtocolError),

    #[error("Operation not supported: {0}")]
    NotSupported(String),

    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Transfer agent error: {0}")]
    TransferAgent(#[from] DataProcessingError),

    #[error("Hub client error: {0}")]
    HubClient(#[from] hub_client::HubClientError),
}

pub type Result<T> = std::result::Result<T, GitXetError>;

impl GitXetError {
    pub(crate) fn git_cmd_failed(e: impl Display, source: Option<std::io::Error>) -> GitXetError {
        GitXetError::GitCommandFailed {
            reason: e.to_string(),
            source,
        }
    }

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

impl From<CasClientError> for GitXetError {
    fn from(value: CasClientError) -> Self {
        Self::from(DataProcessingError::CasClientError(value))
    }
}
