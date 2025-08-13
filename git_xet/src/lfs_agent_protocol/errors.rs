use std::fmt::Display;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum GitLFSProtocolError {
    #[error("Bad custom transfer protocol syntax: {0}")]
    SyntaxError(String),

    #[error("Invalid custom transfer protocol argument: {0}")]
    ArgumentError(String),

    #[error("Invalid custom transfer agent state: {0}")]
    StateError(String),

    #[error("Serde to/from Json failed: {0:?}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    InternalError(String),
}

pub(super) type Result<T> = std::result::Result<T, GitLFSProtocolError>;

pub(crate) fn bad_syntax(e: impl Display) -> GitLFSProtocolError {
    GitLFSProtocolError::SyntaxError(e.to_string())
}

pub(crate) fn bad_argument(e: impl Display) -> GitLFSProtocolError {
    GitLFSProtocolError::ArgumentError(e.to_string())
}

pub(crate) fn bad_state(e: impl Display) -> GitLFSProtocolError {
    GitLFSProtocolError::StateError(e.to_string())
}

pub(super) fn internal(e: impl Display) -> GitLFSProtocolError {
    GitLFSProtocolError::InternalError(e.to_string())
}
