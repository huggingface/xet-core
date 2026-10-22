//! Shared Git repository, URL, credential helper, and process support for Xet Git clients.

mod auth;
mod constants;
mod errors;
mod git_repo;
mod git_url;
mod utils;

pub use auth::{GitCredentialHelper, SSHCredentialHelper};
pub use errors::{GitExtensionError, Result};
pub use git_repo::GitRepo;
pub use git_url::{GitUrl, Scheme};
pub use utils::process_wrapping;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
