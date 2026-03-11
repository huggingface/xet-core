use async_trait::async_trait;
use bytes::Bytes;
use xet_core_structures::merklehash::MerkleHash;

use super::super::error::Result;

/// Trait for clients that support deletion and integrity operations on shards and file entries.
///
/// This is implemented by `LocalClient` which has disk-backed storage. Operations that go
/// through the local server will return 501 Not Implemented if the underlying client does
/// not support these operations.
#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
pub trait DeletionControlableClient: Send + Sync {
    /// Returns all shard hashes from shard files on disk.
    async fn list_shard_entries(&self) -> Result<Vec<MerkleHash>>;

    /// Returns a shard's raw bytes by its hash.
    async fn get_shard_bytes(&self, hash: &MerkleHash) -> Result<Bytes>;

    /// Deletes a shard file by its hash.
    async fn delete_shard_entry(&self, hash: &MerkleHash) -> Result<()>;

    /// Returns (file_hash, shard_hash) tuples for all files across all shards.
    async fn list_file_shard_entries(&self) -> Result<Vec<(MerkleHash, MerkleHash)>>;

    /// Deletes a file entry from all shards that contain it.
    /// Shards that become empty are removed entirely.
    async fn delete_file_entry(&self, file_hash: &MerkleHash) -> Result<()>;

    /// Verifies referential integrity of all shards on disk.
    async fn verify_integrity(&self) -> Result<()>;
}
