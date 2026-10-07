//! Shared Git repository, credential, token-refresh, and process support for Xet Git clients.

mod auth;
mod constants;
mod errors;
mod git_repo;
mod git_url;
mod token_refresher;
mod utils;

pub use auth::{AccessMode, get_credential};
pub use errors::{GitExtensionError, Result};
pub use git_repo::GitRepo;
pub use git_url::{GitUrl, Scheme};
pub use token_refresher::new_git_token_refresher;
pub use utils::process_wrapping;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
