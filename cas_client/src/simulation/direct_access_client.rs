//! Direct Access Client Trait
//!
//! This module defines the `DirectAccessClient` trait, which extends the standard
//! `Client` interface with direct XORB and file access methods. This is used by
//! the local server and testing utilities to access stored data directly.

use std::ops::Range;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use cas_object::CasObject;
use cas_types::{CASReconstructionFetchInfo, FileRange, QueryReconstructionResponse, QueryReconstructionResponseV2};
use merklehash::MerkleHash;

use crate::error::Result;
use crate::interface::Client;

/// A Client with direct access to XORB and file storage.
///
/// This trait extends the standard Client interface with methods for:
/// - Direct XORB access (read, list, delete)
/// - File data retrieval
/// - URL expiration control
/// - API delay simulation
///
/// Both `LocalClient` and `MemoryClient` implement this trait, allowing the
/// local server to work with either backend.
#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
pub trait DirectAccessClient: Client + Send + Sync {
    /// Sets the expiration duration for fetch term URLs.
    fn set_fetch_term_url_expiration(&self, expiration: Duration);

    /// Sets a random delay range for all Client API calls.
    ///
    /// When set, each Client trait method will sleep for a random duration
    /// within the specified range before returning. This simulates network latency.
    ///
    /// Pass `None` to disable the delay.
    fn set_api_delay_range(&self, delay_range: Option<Range<Duration>>);

    /// Sets the maximum number of byte ranges per `XorbMultiRangeFetch` entry
    /// in V2 reconstruction responses.
    ///
    /// Default is `usize::MAX` (all ranges in one fetch). When set to N,
    /// ranges for each xorb are grouped into entries of at most N ranges.
    /// This simulates the CloudFront URL length limit that forces splitting.
    fn set_max_ranges_per_fetch(&self, max_ranges: usize);

    /// Disables V2 reconstruction responses. When disabled, the V2 endpoint
    /// returns 404, forcing clients to fall back to V1.
    fn disable_v2_reconstruction(&self);

    /// Returns whether V2 reconstruction is disabled.
    fn is_v2_reconstruction_disabled(&self) -> bool {
        false
    }

    /// V1 reconstruction: returns per-range presigned URLs.
    async fn get_reconstruction_v1(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>>;

    /// V2 reconstruction: returns per-xorb multi-range fetch descriptors.
    async fn get_reconstruction_v2(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponseV2>>;

    /// Returns all XORB hashes stored in this client.
    async fn list_xorbs(&self) -> Result<Vec<MerkleHash>>;

    /// Deletes a XORB by hash.
    async fn delete_xorb(&self, hash: &MerkleHash);

    /// Get all uncompressed bytes from a XORB.
    async fn get_full_xorb(&self, hash: &MerkleHash) -> Result<Bytes>;

    /// Get uncompressed bytes from a XORB within chunk ranges.
    /// Each tuple represents a chunk index range [start, end).
    async fn get_xorb_ranges(&self, hash: &MerkleHash, chunk_ranges: Vec<(u32, u32)>) -> Result<Vec<Bytes>>;

    /// Get the length of the uncompressed XORB data.
    async fn xorb_length(&self, hash: &MerkleHash) -> Result<u32>;

    /// Check if a XORB exists.
    async fn xorb_exists(&self, hash: &MerkleHash) -> Result<bool>;

    /// Get the CasObject footer/metadata for a XORB.
    async fn xorb_footer(&self, hash: &MerkleHash) -> Result<CasObject>;

    /// Get the file size for a given file hash.
    async fn get_file_size(&self, hash: &MerkleHash) -> Result<u64>;

    /// Get file data, optionally within a byte range.
    async fn get_file_data(&self, hash: &MerkleHash, byte_range: Option<FileRange>) -> Result<Bytes>;

    /// Get raw (serialized) bytes from a XORB, optionally within a byte range.
    ///
    /// Unlike `get_xorb_ranges` which returns decompressed chunk data, this returns
    /// the raw bytes as stored (including compression headers). This is used by the
    /// server's fetch_term endpoint to serve data that clients can then decompress.
    async fn get_xorb_raw_bytes(&self, hash: &MerkleHash, byte_range: Option<FileRange>) -> Result<Bytes>;

    /// Get the total length of the raw (serialized) XORB data.
    async fn xorb_raw_length(&self, hash: &MerkleHash) -> Result<u64>;

    /// Fetches term data for a given hash and fetch term.
    /// Returns (data bytes, chunk byte indices) matching `Client::get_file_term_data`.
    async fn fetch_term_data(
        &self,
        hash: MerkleHash,
        fetch_term: CASReconstructionFetchInfo,
    ) -> Result<(Bytes, Vec<u32>)>;
}
