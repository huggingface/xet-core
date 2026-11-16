use std::path::PathBuf;

use git_extension_common::GitExtensionError;
use thiserror::Error;
use xet_client::ClientError;

use crate::{MAX_POINTER_SIZE, PointerError};

#[derive(Error, Debug)]
pub enum GitHfError {
    #[error("Invalid filter protocol: {0}")]
    FilterProtocol(String),

    #[error("Cleaning failed: {0}")]
    Data(#[from] xet_data::DataError),

    #[error("Runtime error: {0}")]
    Runtime(#[from] xet_runtime::RuntimeError),

    #[error("Local staging error: {0}")]
    Staging(String),

    #[error("Command failed: {reason}{}", .source.as_ref().map(|e| format!(", {e}")).unwrap_or_default())]
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

    #[error("{path}: {}", invalid_pointer_advice(source))]
    InvalidPointer { path: String, source: PointerError },
}

fn invalid_pointer_advice(error: &PointerError) -> String {
    match error {
        PointerError::UnsupportedLfs => {
            "this is a Git LFS pointer, which is not accepted in a Xet repository; replace it with the file's content"
                .to_owned()
        },
        error => format!(
            "this file looks like a Xet pointer but is not valid ({error}), so it is not accepted. If it isn't \
             meant to be a pointer, make it larger than {MAX_POINTER_SIZE} bytes or change its \
             `version https://hf.co/spec/xet/v1` or `oid xet:` lines"
        ),
    }
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
