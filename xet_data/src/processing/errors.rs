pub use crate::error::DataError as DataProcessingError;
pub type Result<T> = std::result::Result<T, DataProcessingError>;
