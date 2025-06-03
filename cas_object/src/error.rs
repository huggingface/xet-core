use std::convert::Infallible;

use thiserror::Error;
use tracing::warn;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum CasObjectError {
    #[error("Invalid Range Read")]
    InvalidRange,

    #[error("Invalid Arguments")]
    InvalidArguments,

    #[error("Format Error: {0}")]
    FormatError(anyhow::Error),

    #[error("Hash Mismatch")]
    HashMismatch,

    #[error("Internal IO Error: {0}")]
    InternalIOError(#[from] std::io::Error),

    #[error("Other Internal Error: {0}")]
    InternalError(anyhow::Error),

    #[error("(De)Compression Error: {0}")]
    CompressionError(#[from] lz4_flex::frame::Error),

    #[error("Internal Hash Parsing Error")]
    HashParsingError(#[from] Infallible),

    #[error("ChunkHeaderParseErrorFooterIdent")]
    ChunkHeaderParseErrorFooterIdent,
}

// Define our own result type here (this seems to be the standard).
pub type Result<T> = std::result::Result<T, CasObjectError>;

impl PartialEq for CasObjectError {
    fn eq(&self, other: &CasObjectError) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

/// Helper trait to swallow CAS object format errors. Used in object
/// validation to reject the object instead of propagating errors.
pub trait Validate<T> {
    fn ok_for_format_error(self) -> Result<Option<T>>;
}

impl<T> Validate<T> for Result<T> {
    fn ok_for_format_error(self) -> Result<Option<T>> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(CasObjectError::FormatError(e)) => {
                warn!("XORB Validation: {e}");
                Ok(None)
            },
            Err(e) => Err(e),
        }
    }
}
