use std::convert::Infallible;

use thiserror::Error;
use tracing::warn;

use crate::merklehash::MerkleHash;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum FormatError {
    // -- Common ----------------------------------------------------------
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(anyhow::Error),

    #[error("{0}")]
    Other(String),

    // -- Shard-specific --------------------------------------------------
    #[error("Too many collisions for truncated hash: {0}")]
    TruncatedHashCollision(u64),

    #[error("Shard version error: {0}")]
    ShardVersion(String),

    #[error("Bad filename: {0}")]
    BadFilename(String),

    #[error("Shard not found: {0}")]
    ShardNotFound(MerkleHash),

    #[error("File not found: {0}")]
    FileNotFound(MerkleHash),

    #[error("Query failed: {0}")]
    QueryFailed(String),

    #[error("Smudge query policy error: {0}")]
    SmudgeQueryPolicy(String),

    #[error("Invalid shard: {0}")]
    InvalidShard(String),

    // -- XORB-specific ---------------------------------------------------
    #[error("Invalid range")]
    InvalidRange,

    #[error("Invalid arguments")]
    InvalidArguments,

    #[error("Format error: {0}")]
    Format(anyhow::Error),

    #[error("Hash mismatch")]
    HashMismatch,

    #[error("Compression error: {0}")]
    Compression(#[from] lz4_flex::frame::Error),

    #[error("Hash parsing error: {0}")]
    HashParsing(#[from] Infallible),

    #[error("Chunk header parse error")]
    ChunkHeaderParse,

    // -- Runtime ---------------------------------------------------------
    #[error("Runtime error: {0}")]
    Runtime(#[from] xet_runtime::RuntimeError),

    #[error("Task lock error: {0}")]
    TaskRuntime(#[from] xet_runtime::utils::RwTaskLockError),

    #[error("Task join error: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),
}

pub type Result<T> = std::result::Result<T, FormatError>;

impl PartialEq for FormatError {
    fn eq(&self, other: &FormatError) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl FormatError {
    pub fn other(inner: impl ToString) -> Self {
        Self::Other(inner.to_string())
    }

    pub fn invalid_shard(inner: impl ToString) -> Self {
        Self::InvalidShard(inner.to_string())
    }
}

/// Helper trait to swallow XORB object format errors during validation.
pub trait Validate<T> {
    fn ok_for_format_error(self) -> Result<Option<T>>;
}

impl<T> Validate<T> for Result<T> {
    fn ok_for_format_error(self) -> Result<Option<T>> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(FormatError::Format(e)) => {
                warn!("XORB Validation: {e}");
                Ok(None)
            },
            Err(e) => Err(e),
        }
    }
}

impl From<crate::merklehash::DataHashHexParseError> for FormatError {
    fn from(_: crate::merklehash::DataHashHexParseError) -> Self {
        FormatError::Other("Invalid hex input for DataHash".to_string())
    }
}

impl From<crate::merklehash::DataHashBytesParseError> for FormatError {
    fn from(_: crate::merklehash::DataHashBytesParseError) -> Self {
        FormatError::Other("Invalid bytes input for DataHash".to_string())
    }
}
