use std::sync::Arc;

use bytes::Bytes;
use xet_core_structures::merklehash::MerkleHash;
use xet_core_structures::metadata_shard::file_structs::MDBFileInfo;
use xet_core_structures::xorb_object::SerializedXorbObject;

use super::adaptive_concurrency::ConnectionPermit;
use super::progress_tracked_streams::ProgressCallback;
use crate::cas_types::{BatchQueryReconstructionResponse, FileRange, HttpRange, QueryReconstructionResponseV2};
use crate::error::Result;

#[async_trait::async_trait]
pub trait URLProvider: Send + Sync {
    /// Retrieves the URL and the byte ranges to fetch.
    /// For single-range (V1) blocks, the Vec has one entry.
    /// For multi-range (V2) blocks, all ranges are included.
    async fn retrieve_url(&self) -> Result<(String, Vec<HttpRange>)>;

    /// Asks for a refresh of the URL; triggered on 403 errors.
    async fn refresh_url(&self) -> Result<()>;
}

/// A `URLProvider` wrapper that exposes exactly one of the byte ranges returned by an inner
/// provider as if it were a single-range fetch.
///
/// This is used by `RemoteClient::get_file_term_data` as a fallback when a multi-range signed
/// URL is rejected by the storage backend (some CDN edges reject multi-range Range headers
/// with 403 before the request reaches the origin). Splitting the failed multi-range fetch
/// into N independent single-range GETs against the same URL travels the same single-range
/// code path that V1 reconstruction already uses, and avoids the broken multi-range path.
///
/// `refresh_url` is delegated to the inner provider so that signature refresh semantics are
/// preserved.
pub struct SingleRangeURLProvider {
    inner: Arc<Box<dyn URLProvider>>,
    range_index: usize,
}

impl SingleRangeURLProvider {
    pub fn new(inner: Arc<Box<dyn URLProvider>>, range_index: usize) -> Self {
        Self { inner, range_index }
    }
}

#[async_trait::async_trait]
impl URLProvider for SingleRangeURLProvider {
    async fn retrieve_url(&self) -> Result<(String, Vec<HttpRange>)> {
        let (url, ranges) = self.inner.retrieve_url().await?;
        // The inner provider's range count can change across a refresh; clamp defensively
        // rather than panic. If the index is out of bounds we fall back to a single-range
        // request using the last available range, which keeps the fetch a valid GET — the
        // caller will detect a content-length mismatch if anything is off.
        let range = ranges
            .get(self.range_index)
            .copied()
            .or_else(|| ranges.last().copied())
            .ok_or_else(|| crate::error::ClientError::Other("URLProvider returned no ranges".to_string()))?;
        Ok((url, vec![range]))
    }

    async fn refresh_url(&self) -> Result<()> {
        self.inner.refresh_url().await
    }
}

/// Build a fan-out of single-range URL providers from a multi-range provider, one per range
/// reported by the inner provider's most recent `retrieve_url()` call.
///
/// Returns an empty vec if the inner provider has no ranges (caller should treat this as an
/// error condition).
pub async fn split_into_single_range_providers(inner: Arc<Box<dyn URLProvider>>) -> Result<Vec<Box<dyn URLProvider>>> {
    let (_, ranges) = inner.retrieve_url().await?;
    Ok((0..ranges.len())
        .map(|i| Box::new(SingleRangeURLProvider::new(inner.clone(), i)) as Box<dyn URLProvider>)
        .collect())
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
        serialized_xorb_object: SerializedXorbObject,
        progress_callback: Option<ProgressCallback>,
        upload_permit: ConnectionPermit,
    ) -> Result<u64>;
}
