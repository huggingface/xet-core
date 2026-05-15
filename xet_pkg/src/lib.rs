//! Rust client library for the Hugging Face Xet storage system.
//!
//! Xet is the storage backend used by the [Hugging Face Hub](https://huggingface.co) for
//! large files.  Files are split into variable-size chunks, deduplicated, and stored in CAS
//! (Content-Addressed Storage) server.  This crate provides a high-level
//! API for uploading and downloading those files.
//!
//! # Getting started
//!
//! All operations go through [`xet_session::XetSession`], which manages a
//! tokio runtime and shared HTTP settings.  Create one with
//! [`XetSessionBuilder`](xet_session::XetSessionBuilder), then use it to
//! build upload commits or download groups:
//!
//! ```rust,no_run
//! use xet::xet_session::{Sha256Policy, XetFileInfo, XetSessionBuilder};
//!
//! # fn example() -> Result<(), xet::xet_session::SessionError> {
//! let session = XetSessionBuilder::new().build()?;
//!
//! // Upload a file
//! let commit = session
//!     .new_upload_commit()?
//!     .with_endpoint("https://cas.example.com")
//!     .with_token_info("write-token", 1_700_000_000)
//!     .build_blocking()?;
//! let handle = commit.upload_from_path_blocking("file.bin".into(), Sha256Policy::Compute)?;
//! let report = commit.commit_blocking()?;
//!
//! // Download a file using the metadata from the upload
//! let meta = report.uploads.values().next().unwrap();
//! let group = session
//!     .new_file_download_group()?
//!     .with_token_info("read-token", 1_700_000_000)
//!     .build_blocking()?;
//! group.download_file_to_path_blocking(meta.xet_info.clone(), "out/file.bin".into())?;
//! group.finish_blocking()?;
//! # Ok(())
//! # }
//! ```
//!
//! See the [`xet_session`] module for the full API, including async
//! variants, streaming uploads and downloads, and progress tracking.
//!
//! # Modules
//!
//! - [`xet_session`] — the primary API: [`XetSession`](xet_session::XetSession), upload commits, file download groups,
//!   and streaming downloads.
//! - [`error`] — [`XetError`], the unified error type for the public API.

pub mod error;
pub use error::XetError;
#[cfg(feature = "python")]
pub use error::{XetAuthenticationError, XetObjectNotFoundError, register_exceptions};

// Legacy helpers re-exported for backward compatibility with `hf_xet` (Python bindings)
// and `git_xet`.  New code should use the [`xet_session`] API instead.
#[cfg(not(target_family = "wasm"))]
pub mod legacy;
pub mod xet_session;

/// Initialize the global tracing subscriber using xet_runtime defaults.
///
/// Reads `HF_XET_LOG_FILE` / `RUST_LOG` environment variables.  Repeated calls
/// are no-ops — the global subscriber is installed only once.
#[cfg(not(target_family = "wasm"))]
pub fn init_logging(version_info: String) {
    let log_dir = xet_runtime::core::xet_cache_root().join("logs");

    // Called before any XetContext is created, so we use a standalone default config for
    // early-init logging setup.
    let cfg = xet_runtime::logging::LoggingConfig::from_directory(
        &xet_runtime::config::XetConfig::new(),
        version_info,
        log_dir,
    );

    xet_runtime::logging::init(cfg);
}
