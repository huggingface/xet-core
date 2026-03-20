#![cfg_attr(feature = "strict", deny(warnings))]

pub mod error;
pub use error::{DataError, Result};

pub mod deduplication;
#[cfg(not(target_family = "wasm"))]
pub mod file_reconstruction;
#[cfg(not(target_family = "wasm"))]
pub mod processing;
pub mod progress_tracking;
