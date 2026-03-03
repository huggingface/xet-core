use bytes::Bytes;
use cas_object::SerializedCasObject;
use cas_types::{BatchQueryReconstructionResponse, FileRange, HttpRange, QueryReconstructionResponseV2};
use mdb_shard::file_structs::MDBFileInfo;
use merklehash::MerkleHash;

use crate::adaptive_concurrency::ConnectionPermit;
use crate::error::Result;
use crate::progress_tracked_streams::ProgressCallback;

#[async_trait::async_trait]
pub trait URLProvider: Send + Sync {
    /// Retrieves the URL and the byte ranges to fetch.
    /// For single-range (V1) blocks, the Vec has one entry.
    /// For multi-range (V2) blocks, all ranges are included.
    async fn retrieve_url(&self) -> Result<(String, Vec<HttpRange>)>;

    /// Asks for a refresh of the URL; triggered on 403 errors.
    async fn refresh_url(&self) -> Result<()>;
}

/// A Client to the Shard service. The shard service
/// provides for
/// 1. upload shard to the shard service
/// 2. querying of file->reconstruction information
/// 3. querying of chunk->shard information
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
pub trait Client: Send + Sync {
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>>;

    /// Returns reconstruction info always in V2 format.
    /// Implementations may try V2 first and fall back to V1 + convert.
    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponseV2>>;

    async fn batch_get_reconstruction(&self, file_ids: &[MerkleHash]) -> Result<BatchQueryReconstructionResponse>;

    async fn acquire_download_permit(&self) -> Result<ConnectionPermit>;

    /// Optional progress callback receives (delta, completed, total) in transfer bytes.
    /// When [uncompressed_size_if_known] is [Some], the returned Bytes must have len() equal to that value.
    async fn get_file_term_data(
        &self,
        url_info: Box<dyn URLProvider>,
        download_permit: ConnectionPermit,
        progress_callback: Option<ProgressCallback>,
        uncompressed_size_if_known: Option<usize>,
    ) -> Result<(Bytes, Vec<u32>)>;

    async fn query_for_global_dedup_shard(&self, prefix: &str, chunk_hash: &MerkleHash) -> Result<Option<Bytes>>;

    /// Acquire an upload permit.
    async fn acquire_upload_permit(&self) -> Result<ConnectionPermit>;

    /// Upload a new shard.
    async fn upload_shard(&self, shard_data: bytes::Bytes, upload_permit: ConnectionPermit) -> Result<bool>;

    /// Upload a new xorb. Optional progress callback receives (delta, completed, total) in transfer bytes.
    async fn upload_xorb(
        &self,
        prefix: &str,
        serialized_cas_object: SerializedCasObject,
        progress_callback: Option<ProgressCallback>,
        upload_permit: ConnectionPermit,
    ) -> Result<u64>;
}
