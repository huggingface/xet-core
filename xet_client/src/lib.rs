//! HTTPS client for communicating with Hugging Face Xet storage servers.
//!
//! Includes the [`cas_client`] for uploading/downloading Xorb objects and
//! metadata shards, the [`hub_client`] for Hugging Face Hub API
//! interactions, and a local [`chunk_cache`] with LRU eviction.

#![cfg_attr(feature = "strict", deny(warnings))]

pub mod error;
pub use error::ClientError;

pub mod cas_client;
pub mod cas_types;
pub mod chunk_cache;
pub mod common;
pub mod hub_client;
