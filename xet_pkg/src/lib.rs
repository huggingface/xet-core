//! Client library for the Hugging Face Xet data storage system.
//!
//! Provides the high-level [`xet_session::XetSession`] API for uploading
//! and downloading files with chunk-based deduplication, tying together
//! the lower-level [`xet_runtime`], [`xet_core_structures`],
//! [`xet_client`], and [`xet_data`] crates.

pub mod error;
pub use error::XetError;
#[cfg(feature = "python")]
pub use error::{XetAuthenticationError, XetObjectNotFoundError, register_exceptions};

pub mod legacy;
pub mod xet_session;
