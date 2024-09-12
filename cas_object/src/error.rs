use std::convert::Infallible;

use merklehash::MerkleHash;
use xet_error::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum CasObjectError {
    #[error("Invalid Range Read")]
    InvalidRange,

    #[error("Invalid Arguments")]
    InvalidArguments,

    #[error("Format Error: {0}")]
    FormatError(anyhow::Error),

    #[error("Internal IO Error: {0}")]
    InternalIOError(#[from] std::io::Error),

    #[error("Other Internal Error: {0}")]
    InternalError(anyhow::Error),

    #[error("Internal Hash Parsing Error")]
    HashParsingError(#[from] Infallible),

    #[error("CAS Hash not found")]
    XORBNotFound(MerkleHash),
}

// Define our own result type here (this seems to be the standard).
pub type Result<T> = std::result::Result<T, CasObjectError>;

impl PartialEq for CasObjectError {
    fn eq(&self, other: &CasObjectError) -> bool {
        match (self, other) {
            (CasObjectError::XORBNotFound(a), CasObjectError::XORBNotFound(b)) => a == b,
            (e1, e2) => std::mem::discriminant(e1) == std::mem::discriminant(e2),
        }
    }
}
