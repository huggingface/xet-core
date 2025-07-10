use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use cas_object::SerializedCasObject;
use cas_types::FileRange;
use mdb_shard::file_structs::MDBFileInfo;
use merklehash::MerkleHash;
use progress_tracking::item_tracking::SingleItemProgressUpdater;
use progress_tracking::upload_tracking::CompletionTracker;

use crate::error::Result;
#[cfg(not(target_family = "wasm"))]
use crate::OutputProvider;

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
    async fn upload_shard(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        force_sync: bool,
        shard_data: bytes::Bytes,
        salt: &[u8; 32],
    ) -> Result<bool>;

    /// Upload a new xorb.
    async fn upload_xorb(
        &self,
        prefix: &str,
        serialized_cas_object: SerializedCasObject,
        upload_tracker: Option<Arc<CompletionTracker>>,
    ) -> Result<u64>;

    /// Check if a XORB already exists.
    async fn exists(&self, prefix: &str, hash: &MerkleHash) -> Result<bool>;

    /// Indicates if the serialized cas object should have a written footer.
    /// This should only be true for testing with LocalClient.
    fn use_xorb_footer(&self) -> bool;
}
