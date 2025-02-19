use std::io;

use merklehash::MerkleHash;
use thiserror::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum MDBShardError {
    #[error("File I/O error")]
    IOError(#[from] io::Error),

    #[error("Too many collisions when searching for truncated hash : {0}")]
    TruncatedHashCollisionError(u64),

    #[error("Shard version error: {0}")]
    ShardVersionError(String),

    #[error("Bad file name format: {0}")]
    BadFilename(String),

    #[error("Other Internal Error: {0}")]
    InternalError(anyhow::Error),

    #[error("Shard not found")]
    ShardNotFound(MerkleHash),

    #[error("File not found")]
    FileNotFound(MerkleHash),

    #[error("Query failed: {0}")]
    QueryFailed(String),

    #[error("Smudge query policy Error: {0}")]
    SmudgeQueryPolicyError(String),

    #[error("Error: {0}")]
    Other(String),
}

// Define our own result type here (this seems to be the standard).
pub type Result<T> = std::result::Result<T, MDBShardError>;

// For error checking
impl PartialEq for MDBShardError {
    fn eq(&self, other: &MDBShardError) -> bool {
        match (self, other) {
            (MDBShardError::IOError(ref e1), MDBShardError::IOError(ref e2)) => e1.kind() == e2.kind(),
            _ => false,
        }
    }
}

// Helper function to swallow io::ErrorKind::NotFound errors. In the case of
// a cached shard was registered but later deleted during the lifetime
// of a shard file manager, queries to this shard should not fail hard.
pub fn not_found_as_none<T>(e: MDBShardError) -> Result<Option<T>> {
    match e {
        MDBShardError::IOError(e) => {
            if e.kind() == io::ErrorKind::NotFound {
                Ok(None)
            } else {
                Err(MDBShardError::IOError(e))
            }
        },
        other_err => Err(other_err),
    }
}
