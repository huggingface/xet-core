//! Core data structures for the Hugging Face Xet storage system.
//!
//! Provides [`merklehash::MerkleHash`] (256-bit content-addressed hashes),
//! [`metadata_shard`] (compact shard format mapping file ranges to Xorb
//! chunks), and [`xorb_object`] (content-addressed storage objects with
//! byte-grouping compression).

#![cfg_attr(feature = "strict", deny(warnings))]

pub mod error;
pub use error::CoreError;

pub mod data_structures;
pub mod merklehash;
pub mod metadata_shard;
pub mod utils;
pub mod xorb_object;

// Re-export commonly used items at the crate root for convenience
pub use data_structures::{MerkleHashMap, PassThroughHashMap, TruncatedMerkleHashMap, U64HashExtractable};
pub use utils::{ExpWeightedMovingAvg, serialization_utils};
