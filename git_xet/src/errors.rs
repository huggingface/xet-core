use std::fmt::Display;
use std::path::PathBuf;

use git_extension_common::GitExtensionError;
use thiserror::Error;
use xet_client::ClientError;
use xet_data::DataError;

use crate::lfs_agent_protocol::GitLFSProtocolError;

#[derive(Error, Debug)]
pub enum GitXetError {
    #[error("Command failed: {reason}, {source:?}")]
    CommandFailed {
        reason: String,
        source: Option<std::io::Error>,
    },

    #[error("Failed to find Git repo at {path}, internal error {source}")]
    NoGitRepo { path: PathBuf, source: git2::Error },

    #[error("Internal Git error: {0}")]
    GitError(git2::Error),

    #[error("Invalid Git config: {0}")]
    InvalidGitConfig(String),

    #[error("Invalid Git URL: {0}")]
    InvalidGitUrl(git_url_parse::GitUrlParseError),

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

// Maps each shared error to the same git-xet variant, so callers and messages see git-xet's error
// type directly. The match is exhaustive: a new shared variant must be mapped here.
impl From<GitExtensionError> for GitXetError {
    fn from(error: GitExtensionError) -> Self {
        match error {
            GitExtensionError::CommandFailed { reason, source } => Self::CommandFailed { reason, source },
            GitExtensionError::NoGitRepo { path, source } => Self::NoGitRepo { path, source },
            GitExtensionError::GitError(error) => Self::GitError(error),
            GitExtensionError::InvalidGitConfig(message) => Self::InvalidGitConfig(message),
            GitExtensionError::InvalidGitUrl(error) => Self::InvalidGitUrl(error),
            GitExtensionError::NotSupported(message) => Self::NotSupported(message),
            GitExtensionError::IO(error) => Self::IO(error),
            GitExtensionError::Internal(message) => Self::Internal(message),
            GitExtensionError::Client(error) => Self::Client(error),
        }
    }
}

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
