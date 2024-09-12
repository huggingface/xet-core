use cache::CacheError;
use http::uri::InvalidUri;
use merklehash::MerkleHash;
use tonic::metadata::errors::InvalidMetadataValue;
use xet_error::Error;

use crate::cas_connection_pool::CasConnectionPoolError;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum CasClientError {
    #[error("Tonic RPC error.")]
    TonicError,

    #[error("CAS Cache Error: {0}")]
    CacheError(#[from] CacheError),

    #[error("Configuration Error: {0} ")]
    ConfigurationError(String),

    #[error("URL Parsing Error.")]
    URLError(#[from] InvalidUri),

    #[error("Tonic Trasport Error")]
    TonicTransportError(#[from] tonic::transport::Error),

    #[error("Metadata error: {0}")]
    MetadataParsingError(#[from] InvalidMetadataValue),

    #[error("CAS Connection Pool Error")]
    CasConnectionPoolError(#[from] CasConnectionPoolError),

    #[error("Invalid Range Read")]
    InvalidRange,

    #[error("Invalid Arguments")]
    InvalidArguments,

    #[error("Hash Mismatch")]
    HashMismatch,

    #[error("Internal IO Error: {0}")]
    InternalIOError(#[from] std::io::Error),

    #[error("Other Internal Error: {0}")]
    InternalError(anyhow::Error),

    #[error("CAS Hash not found")]
    XORBNotFound(MerkleHash),

    #[error("Data transfer timeout")]
    DataTransferTimeout,

    #[error("Client connection error {0}")]
    Grpc(#[from] anyhow::Error),

    #[error("Batch Error: {0}")]
    BatchError(String),

    #[error("Serialization Error: {0}")]
    SerializationError(#[from] bincode::Error),

    #[error("Runtime Error (Temp files): {0}")]
    RuntimeErrorTempFileError(#[from] tempfile::PersistError),

    #[error("Cas Object Error: {0}")]
    CasObjectError(#[from] cas_object::error::CasObjectError),
}

// Define our own result type here (this seems to be the standard).
pub type Result<T> = std::result::Result<T, CasClientError>;

impl PartialEq for CasClientError {
    fn eq(&self, other: &CasClientError) -> bool {
        match (self, other) {
            (CasClientError::XORBNotFound(a), CasClientError::XORBNotFound(b)) => a == b,
            (e1, e2) => std::mem::discriminant(e1) == std::mem::discriminant(e2),
        }
    }
}
