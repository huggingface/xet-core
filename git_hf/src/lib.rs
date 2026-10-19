//! Native Xet V2 Git client.

mod auth;
mod clean_session;
mod cleaner;
mod cli;
mod constants;
mod error;
mod filter_protocol;
mod hook;
mod selection;
mod staging;
mod utils;

pub use cli::GitHfApp;
pub use error::{GitHfError, Result};
pub use xet_core_structures::xet_pointer::{MAX_POINTER_SIZE, PointerError, XetPointer};
