use std::sync::Arc;

use bytes::Bytes;
use cas_object::SerializedCasObject;
use cas_types::FileRange;
use mdb_shard::file_structs::MDBFileInfo;
use merklehash::MerkleHash;
use progress_tracking::item_tracking::SingleItemProgressUpdater;
use progress_tracking::upload_tracking::CompletionTracker;

use crate::adaptive_concurrency::ConnectionPermit;
use crate::error::Result;
#[cfg(not(target_family = "wasm"))]
use crate::{SeekingOutputProvider, SequentialOutput};

/// A Client to the Shard service. The shard service
/// provides for
/// 1. upload shard to the shard service
/// 2. querying of file->reconstruction information
/// 3. querying of chunk->shard information
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
pub trait Client {
    /// Get an entire file by file hash with an optional bytes range.
    ///
    /// The http_client passed in is a non-authenticated client. This is used to directly communicate
    /// with the backing store (S3) to retrieve xorbs.
    ///
    /// Content is written in-order to the provided SequentialOutput
    #[cfg(not(target_family = "wasm"))]
    async fn get_file_with_sequential_writer(
        &self,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        output_provider: SequentialOutput,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64>;

    #[cfg(not(target_family = "wasm"))]
    async fn get_file_with_parallel_writer(
        &self,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        output_provider: SeekingOutputProvider,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64>;

    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>>;

    async fn query_for_global_dedup_shard(&self, prefix: &str, chunk_hash: &MerkleHash) -> Result<Option<Bytes>>;

    /// Acquire an upload permit.
    async fn acquire_upload_permit(&self) -> Result<ConnectionPermit>;

    /// Upload a new shard.
    async fn upload_shard_with_permit(&self, shard_data: bytes::Bytes, upload_permit: ConnectionPermit)
    -> Result<bool>;

    /// Upload a new xorb.
    async fn upload_xorb_with_permit(
        &self,
        prefix: &str,
        serialized_cas_object: SerializedCasObject,
        upload_tracker: Option<Arc<CompletionTracker>>,
        upload_permit: ConnectionPermit,
    ) -> Result<u64>;

    /// Upload a new shard, acquiring the permit.
    async fn upload_shard(&self, shard_data: bytes::Bytes) -> Result<bool> {
        let permit = self.acquire_upload_permit().await?;
        self.upload_shard_with_permit(shard_data, permit).await
    }

    /// Upload a new xorb, acquiring the permit.
    async fn upload_xorb(
        &self,
        prefix: &str,
        serialized_cas_object: SerializedCasObject,
        upload_tracker: Option<Arc<CompletionTracker>>,
    ) -> Result<u64> {
        let permit = self.acquire_upload_permit().await?;
        self.upload_xorb_with_permit(prefix, serialized_cas_object, upload_tracker, permit)
            .await
    }

    /// Indicates if the serialized cas object should have a written footer.
    /// This should only be true for testing with LocalClient.
    fn use_xorb_footer(&self) -> bool;

    /// Indicates if the serialized cas object should have a written footer.
    /// This should only be true for testing with LocalClient.
    fn use_shard_footer(&self) -> bool;
}
