use std::path::PathBuf;

use git_extension_common::GitExtensionError;
use thiserror::Error;
use xet_client::ClientError;

use crate::PointerError;

#[derive(Error, Debug)]
pub enum GitHfError {
    #[error("Invalid filter protocol: {0}")]
    FilterProtocol(String),
    #[error("Cleaning failed: {0}")]
    Data(#[from] xet_data::DataError),
    #[error("Runtime error: {0}")]
    Runtime(#[from] xet_runtime::RuntimeError),
    #[error("Pre-push hook error: {0}")]
    Hook(String),

    #[error("Command failed: {reason}, {source:?}")]
    CommandFailed {
        reason: String,
        source: Option<std::io::Error>,
    },

    #[error("Failed to find Git repo at {path}, internal error {source}")]
    NoGitRepo { path: PathBuf, source: git2::Error },

    #[error("Internal Git error: {0}")]
    GitError(#[from] git2::Error),

    #[error("Authentication failed: {0}")]
    Auth(String),

    #[error("Invalid Git config: {0}")]
    InvalidGitConfig(String),

    #[error("Invalid Git URL: {0}")]
    InvalidGitUrl(#[from] git_url_parse::GitUrlParseError),

    #[error("Operation not supported: {0}")]
    NotSupported(String),

    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Client error: {0}")]
    Client(#[from] ClientError),

    #[error(transparent)]
    Pointer(#[from] PointerError),
}

pub type Result<T> = std::result::Result<T, GitHfError>;

impl From<GitExtensionError> for GitHfError {
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
