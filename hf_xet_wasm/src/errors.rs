use thiserror::Error;

#[non_exhaustive]
#[derive(Error, Debug)]
pub enum DataProcessingError {
    #[error("Internal error: {0}")]
    InternalError(String),
}

pub type Result<T> = std::result::Result<T, DataProcessingError>;
