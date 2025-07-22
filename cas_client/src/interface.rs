use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use cas_object::SerializedCasObject;
use cas_types::FileRange;
use mdb_shard::file_structs::MDBFileInfo;
use merklehash::MerkleHash;
use progress_tracking::item_tracking::SingleItemProgressUpdater;
use progress_tracking::upload_tracking::CompletionTracker;

use crate::adaptive_concurrency::{AdaptiveConcurrencyController, ConnectionPermit};
use crate::constants::{MAX_CONCURRENT_UPLOADS, MIN_CONCURRENT_UPLOADS, NUM_INITIAL_CONCURRENT_UPLOADS};
use crate::error::Result;
#[cfg(not(target_family = "wasm"))]
use crate::OutputProvider;

// The upload concurrency controller
lazy_static::lazy_static! {
    static ref UPLOAD_CONCURRENCY_CONTROLLER : Arc<AdaptiveConcurrencyController>
        = AdaptiveConcurrencyController::new("uploads", *NUM_INITIAL_CONCURRENT_UPLOADS, (*MIN_CONCURRENT_UPLOADS, *MAX_CONCURRENT_UPLOADS));
}

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
    #[cfg(not(target_family = "wasm"))]
    async fn get_file(
        &self,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        output_provider: &OutputProvider,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64>;

    #[cfg(not(target_family = "wasm"))]
    async fn batch_get_file(&self, files: HashMap<MerkleHash, &OutputProvider>) -> Result<u64> {
        let mut n_bytes = 0;
        // Provide the basic naive implementation as a default.
        for (h, w) in files {
            n_bytes += self.get_file(&h, None, w, None).await?;
        }
        Ok(n_bytes)
    }

    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>>;

    async fn query_for_global_dedup_shard(
        &self,
        prefix: &str,
        chunk_hash: &MerkleHash,
        salt: &[u8; 32],
    ) -> Result<Option<Bytes>>;

    /// Upload a new shard.
    async fn upload_shard_with_permit(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        force_sync: bool,
        shard_data: bytes::Bytes,
        salt: &[u8; 32],
        upload_permit: ConnectionPermit,
    ) -> Result<bool>;

    /// Upload a new xorb.
    async fn upload_xorb_with_permit(
        &self,
        prefix: &str,
        serialized_cas_object: SerializedCasObject,
        upload_tracker: Option<Arc<CompletionTracker>>,
        upload_permit: ConnectionPermit,
    ) -> Result<u64>;

    /// Acquire an upload permit.
    async fn acquire_upload_permit(&self) -> Result<ConnectionPermit> {
        UPLOAD_CONCURRENCY_CONTROLLER.acquire_connection_permit().await
    }

    /// Upload a new shard, acquiring the permit.
    async fn upload_shard(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        force_sync: bool,
        shard_data: bytes::Bytes,
        salt: &[u8; 32],
    ) -> Result<bool> {
        let permit = self.acquire_upload_permit().await?;
        self.upload_shard_with_permit(prefix, hash, force_sync, shard_data, salt, permit)
            .await
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

    /// Check if a XORB already exists.
    async fn exists(&self, prefix: &str, hash: &MerkleHash) -> Result<bool>;

    /// Indicates if the serialized cas object should have a written footer.
    /// This should only be true for testing with LocalClient.
    fn use_xorb_footer(&self) -> bool;
}
