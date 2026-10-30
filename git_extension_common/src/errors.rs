use std::fmt::Display;
use std::path::PathBuf;

use thiserror::Error;
use xet_client::ClientError;

#[derive(Error, Debug)]
pub enum GitExtensionError {
    #[error("Command failed: {reason}, {source:?}")]
    CommandFailed {
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

    #[error("Operation not supported: {0}")]
    NotSupported(String),

    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Client error: {0}")]
    Client(#[from] ClientError),
}

pub type Result<T> = std::result::Result<T, GitExtensionError>;

impl GitExtensionError {
    pub(crate) fn cmd_failed(e: impl Display, source: Option<std::io::Error>) -> GitExtensionError {
        GitExtensionError::CommandFailed {
            reason: e.to_string(),
            source,
        }
    }

    pub(crate) fn not_supported(e: impl Display) -> GitExtensionError {
        GitExtensionError::NotSupported(e.to_string())
    }

    pub(crate) fn config_error(e: impl Display) -> GitExtensionError {
        GitExtensionError::InvalidGitConfig(e.to_string())
    }

    pub(crate) fn internal(e: impl Display) -> GitExtensionError {
        GitExtensionError::Internal(e.to_string())
    }
}
