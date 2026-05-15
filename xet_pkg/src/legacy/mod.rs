//! Legacy helpers re-exported for backward compatibility.
//!
//! This module exposes lower-level types and functions used by the Python
//! bindings (`hf_xet`) and `git_xet`.  New code should use the
//! [`xet_session`](crate::xet_session) API instead — it provides a safer,
//! higher-level interface with built-in progress tracking, token refresh,
//! and automatic runtime management.

#[cfg(not(target_family = "wasm"))]
pub mod data_client;
#[cfg(not(target_family = "wasm"))]
pub mod progress_tracking;

// Re-exports from xet_data so external consumers (hf_xet, git_xet) don't need
// a direct xet_data dependency.
#[cfg(not(target_family = "wasm"))]
pub use data_client::hash_files_async;
#[cfg(not(target_family = "wasm"))]
pub use xet_data::processing::configurations::{SessionContext, TranslatorConfig};
#[cfg(not(target_family = "wasm"))]
pub use xet_data::processing::data_client::legacy::clean_bytes;
#[cfg(not(target_family = "wasm"))]
pub use xet_data::processing::data_client::{clean_file, default_config};
#[cfg(not(target_family = "wasm"))]
pub use xet_data::processing::{FileDownloadSession, FileUploadSession, Sha256Policy, XetFileInfo};
