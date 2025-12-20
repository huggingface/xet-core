use std::sync::Arc;

use bytes::Bytes;
use cas_object::SerializedCasObject;
use cas_types::QueryReconstructionResponse;
#[cfg(not(target_family = "wasm"))]
use cas_types::{BatchQueryReconstructionResponse, CASReconstructionFetchInfo, FileRange};
use mdb_shard::file_structs::MDBFileInfo;
use merklehash::MerkleHash;
#[cfg(not(target_family = "wasm"))]
use progress_tracking::item_tracking::SingleItemProgressUpdater;
use progress_tracking::upload_tracking::CompletionTracker;

use crate::adaptive_concurrency::ConnectionPermit;
#[cfg(not(target_family = "wasm"))]
use crate::download_utils::TermDownloadOutput;
use crate::error::Result;
#[cfg(not(target_family = "wasm"))]
use crate::output_provider::{SeekingOutputProvider, SequentialOutput};

#[async_trait::async_trait]
pub trait URLRetriever: Send + Sync {
    async fn retrieve_url(&mut self, force_refresh: bool) -> Result<(String, HttpRange)>;
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

    #[cfg(not(target_family = "wasm"))]
    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>>;

    #[cfg(not(target_family = "wasm"))]
    async fn batch_get_reconstruction(&self, file_ids: &[MerkleHash]) -> Result<BatchQueryReconstructionResponse>;

    #[cfg(not(target_family = "wasm"))]
    async fn get_file_term_data_v1(
        &self,
        hash: MerkleHash,
        fetch_term: CASReconstructionFetchInfo,
    ) -> Result<TermDownloadOutput>;

    async fn acquire_download_permit(&self) -> Result<ConnectionPermit>;

    #[cfg(not(target_family = "wasm"))]
    async fn get_file_term_data(
        &self,
        url_info: Box<dyn URLRetriever>,
        download_permit: ConnectionPermit,
    ) -> Result<TermDownloadOutput>;

    #[cfg(not(target_family = "wasm"))]
    async fn get_file_with_sequential_writer(
        self: Arc<Self>,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        output_provider: SequentialOutput,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64>;

    #[cfg(not(target_family = "wasm"))]
    async fn get_file_with_parallel_writer(
        self: Arc<Self>,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        output_provider: SeekingOutputProvider,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64>;

    async fn query_for_global_dedup_shard(&self, prefix: &str, chunk_hash: &MerkleHash) -> Result<Option<Bytes>>;

    /// Acquire an upload permit.
    async fn acquire_upload_permit(&self) -> Result<ConnectionPermit>;

    /// Upload a new shard.
    async fn upload_shard(&self, shard_data: bytes::Bytes, upload_permit: ConnectionPermit) -> Result<bool>;

    /// Upload a new xorb.
    async fn upload_xorb(
        &self,
        prefix: &str,
        serialized_cas_object: SerializedCasObject,
        upload_tracker: Option<Arc<CompletionTracker>>,
        upload_permit: ConnectionPermit,
    ) -> Result<u64>;

    /// Indicates if the serialized cas object should have a written footer.
    /// This should only be true for testing with LocalClient.
    fn use_xorb_footer(&self) -> bool;

    /// Indicates if the serialized cas object should have a written footer.
    /// This should only be true for testing with LocalClient.
    fn use_shard_footer(&self) -> bool;
}
