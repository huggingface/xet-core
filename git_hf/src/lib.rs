//! Native Xet V2 Git client.

mod clean_session;
mod cleaner;
mod cli;
mod constants;
mod error;
mod filter_protocol;
mod pointer;
mod selection;
mod staging;
mod utils;

pub use cli::GitHfApp;
pub use error::{GitHfError, Result};
pub use pointer::{MAX_POINTER_SIZE, PointerError, XetPointer};
