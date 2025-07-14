use std::io;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum MerkleDBError {
    #[error("File I/O error")]
    IOError(#[from] io::Error),

    #[error("Graph invariant broken : {0}")]
    GraphInvariantError(String),

    #[error("Serialization/Deserialization Error : {0}")]
    BinCodeError(#[from] bincode::Error),

    #[error("Bad file name format: {0}")]
    BadFilename(String),

    #[error("Error: {0}")]
    Other(String),
}

// Define our own result type here (this seems to be the standard).
pub type Result<T> = std::result::Result<T, MerkleDBError>;

// For error checking
impl PartialEq for MerkleDBError {
    fn eq(&self, other: &MerkleDBError) -> bool {
        match (self, other) {
            (MerkleDBError::IOError(ref e1), MerkleDBError::IOError(ref e2)) => e1.kind() == e2.kind(),
            (MerkleDBError::GraphInvariantError(s1), MerkleDBError::GraphInvariantError(s2)) => s1 == s2,
            (MerkleDBError::BinCodeError(_), MerkleDBError::BinCodeError(_)) => {
                // TODO: expand this.  Currently Encode/decode errors in bincode implement PartialEq,
                // but the general error class has an Other enumeration that does not and thus it doesn't
                // implement it.   For now, just leave this as true since we're implementing this for
                // testing purposes.
                true
            },
            _ => false,
        }
    }
}
