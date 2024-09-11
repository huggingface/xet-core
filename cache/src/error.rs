use crate::CacheError::OtherTaskError;
use cas::errors::SingleflightError;
use tracing::error;
use xet_error::Error;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum CacheError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error("Couldn't save all data")]
    SaveError,
    #[error("Invariant violated in the function: {0}")]
    InvariantViolated(String),
    #[error("Couldn't find block")]
    BlockNotFound,
    #[error("disk cache not in a directory")]
    CacheNotWritableDirectory,
    #[error("input file not formatted properly")]
    InvalidFilenameFormat,
    #[error("issue joining result back to main")]
    JoinError,

    #[error("Invalid range supplied ({0}, {1})")]
    InvalidRange(u64, u64),

    #[error(transparent)]
    RemoteError(#[from] anyhow::Error),

    #[error("Error with other task reading block: {0}")]
    OtherTaskError(String),

    #[error("Error serializing header: {0}")]
    HeaderError(String),
}

// Define our own result type here (this seems to be the standard).
pub type Result<T> = std::result::Result<T, CacheError>;

impl From<SingleflightError<CacheError>> for CacheError {
    fn from(e: SingleflightError<CacheError>) -> Self {
        match e {
            SingleflightError::InternalError(err) => err,
            SingleflightError::WaiterInternalError(err) => OtherTaskError(err),
            _ => CacheError::InvariantViolated(format!("BUG: Call to remote: {e:?}")),
        }
    }
}
