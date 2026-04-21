//! Async runtime, configuration, logging, and utility infrastructure for
//! the Hugging Face Xet storage tools.
//!
//! This crate provides the shared foundation used by all crates in the
//! xet-core ecosystem: a Tokio-based async runtime, hierarchical
//! configuration, structured tracing-based logging, and common error types.

#![cfg_attr(feature = "strict", deny(warnings))]

pub mod error;
pub use error::RuntimeError;

pub mod error_printer;
pub mod file_utils;
pub mod utils;
pub use utils::configuration_utils;
pub mod config;
pub mod core;
pub mod fd_diagnostics;
pub mod logging;
