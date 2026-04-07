use async_trait::async_trait;
use bytes::Bytes;
use xet_core_structures::merklehash::MerkleHash;

use crate::error::Result;

/// An opaque 32-byte tag used for conditional deletion (compare-and-delete).
///
/// Implementations should derive this from object metadata/content with enough entropy
/// to reduce false matches when objects are rapidly rewritten.
pub type ObjectTag = [u8; 32];

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

    /// Soft-deletes a file entry by hash.
    /// The file is hidden from reconstruction and listing paths without rewriting shard files.
    async fn delete_file_entry(&self, file_hash: &MerkleHash) -> Result<()>;

    /// Removes all global-dedup table entries contributed by the given shard.
    /// Called by GC Stage 4 before replacing or discarding a shard.
    async fn remove_shard_dedup_entries(&self, shard_hash: &MerkleHash) -> Result<()>;

    /// Deletes a XORB by hash.
    async fn delete_xorb(&self, hash: &MerkleHash);

    /// Returns all XORB hashes with their associated object tags.
    async fn list_xorbs_and_tags(&self) -> Result<Vec<(MerkleHash, ObjectTag)>>;

    /// Deletes a XORB only if its current tag matches the provided tag.
    /// Returns `Ok(true)` if deleted, `Ok(false)` if the tag did not match.
    async fn delete_xorb_if_tag_matches(&self, hash: &MerkleHash, tag: &ObjectTag) -> Result<bool>;

    /// Returns all shard hashes with their associated object tags.
    async fn list_shards_with_tags(&self) -> Result<Vec<(MerkleHash, ObjectTag)>>;

    /// Deletes a shard only if its current tag matches the provided tag.
    /// Returns `Ok(true)` if deleted, `Ok(false)` if the tag did not match.
    async fn delete_shard_if_tag_matches(&self, hash: &MerkleHash, tag: &ObjectTag) -> Result<bool>;

    /// Verifies referential integrity of all shards on disk.
    async fn verify_integrity(&self) -> Result<()>;

    /// Verifies completeness: after GC convergence, all on-disk data must be reachable.
    async fn verify_all_reachable(&self) -> Result<()>;
}
