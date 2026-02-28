use bytes::Bytes;
use cas_object::SerializedCasObject;
use cas_types::{
    BatchQueryReconstructionResponse, FileRange, HttpRange, QueryReconstructionResponse, ReconstructionResponse,
};
use mdb_shard::file_structs::MDBFileInfo;
use merklehash::MerkleHash;

use crate::adaptive_concurrency::ConnectionPermit;
use crate::error::{CasClientError, Result};
use crate::progress_tracked_streams::ProgressCallback;

#[async_trait::async_trait]
pub trait URLProvider: Send + Sync {
    // Retrieves the URL.
    async fn retrieve_url(&self) -> Result<(String, HttpRange)>;

    // Asks for a refresh of the URL; triggered on 403 errors.
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

    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>>;

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

    /// Request reconstruction with V2 support. Returns V2 if the server supports it, V1 otherwise.
    /// Default implementation delegates to get_reconstruction and wraps in V1.
    async fn get_reconstruction_v2(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<ReconstructionResponse>> {
        Ok(self
            .get_reconstruction(file_id, bytes_range)
            .await?
            .map(ReconstructionResponse::V1))
    }

    /// Download multiple byte ranges from a single URL via multi-range HTTP request,
    /// deserializing each part as a CAS object into (data, chunk_offsets).
    /// Default implementation downloads each range individually via get_file_term_data.
    async fn get_multi_range_term_data(
        &self,
        _url: &str,
        _range_header: &str,
        _expected_parts: usize,
        _download_permit: ConnectionPermit,
        _progress_callback: Option<ProgressCallback>,
    ) -> Result<Vec<(Bytes, Vec<u32>)>> {
        Err(CasClientError::Other("get_multi_range_term_data not supported by this client".to_string()))
    }

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
