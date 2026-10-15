//! Foundation for the native Xet V2 Git client. Transfer commands are not yet implemented.

mod cli;
mod error;

pub use cli::GitHfApp;
pub use error::{GitHfError, Result};
pub use xet_core_structures::xet_pointer::{MAX_POINTER_SIZE, PointerError, XetPointer};
