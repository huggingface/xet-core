use std::fmt::Display;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum GitLFSProtocolError {
    #[error("Bad custom transfer protocol syntax: {0}")]
    Syntax(String),

    #[error("Invalid custom transfer protocol argument: {0}")]
    Argument(String),

    #[error("Invalid custom transfer agent state: {0}")]
    State(String),

    #[error("Serde to/from Json failed: {0:?}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),
}

pub(super) type Result<T> = std::result::Result<T, GitLFSProtocolError>;

impl GitLFSProtocolError {
    pub(crate) fn bad_syntax(e: impl Display) -> GitLFSProtocolError {
        GitLFSProtocolError::Syntax(e.to_string())
    }

    pub(crate) fn bad_argument(e: impl Display) -> GitLFSProtocolError {
        GitLFSProtocolError::Argument(e.to_string())
    }

    pub(crate) fn bad_state(e: impl Display) -> GitLFSProtocolError {
        GitLFSProtocolError::State(e.to_string())
    }
}
