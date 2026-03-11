pub use crate::error::FormatError as MDBShardError;
pub type Result<T> = std::result::Result<T, MDBShardError>;
