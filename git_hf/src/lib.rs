//! Foundation for the native Xet V2 Git client. Transfer commands are not yet implemented.

mod cli;
mod error;
mod pointer;

pub use cli::GitHfApp;
pub use error::{GitHfError, Result};
pub use pointer::{MAX_POINTER_SIZE, PointerError, XetPointer};
