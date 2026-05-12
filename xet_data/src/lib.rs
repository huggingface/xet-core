//! Data processing pipeline for chunking, deduplication, and file
//! reconstruction, used in the Hugging Face Xet storage tools.
//!
//! Provides content-defined chunking via gear hashing, deduplication
//! against metadata shards, and file reconstruction from deduplicated
//! chunk references.

#![cfg_attr(feature = "strict", deny(warnings))]

pub mod error;
pub use error::{DataError, Result};

pub mod deduplication;
pub mod file_reconstruction;
pub mod processing;
pub mod progress_tracking;
