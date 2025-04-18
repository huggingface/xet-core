#![cfg_attr(feature = "strict", deny(warnings))]

pub mod auth;
pub mod errors;
pub mod serialization_utils;
#[cfg(not(target_family = "wasm"))]
pub mod singleflight;

#[cfg(not(target_family = "wasm"))]
mod async_read;
#[cfg(not(target_family = "wasm"))]
pub mod limited_joinset;
mod output_bytes;
pub mod progress;

#[cfg(not(target_family = "wasm"))]
pub use async_read::CopyReader;
pub use output_bytes::output_bytes;

pub mod constant_declarations;
