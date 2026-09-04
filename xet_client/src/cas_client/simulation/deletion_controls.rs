use async_trait::async_trait;
use bytes::Bytes;
use xet_core_structures::merklehash::MerkleHash;

use crate::error::Result;

/// An opaque 32-byte etag used for conditional deletion (compare-and-delete), standing in
/// for S3's ETag.
///
/// Implementations should derive this from object metadata/content with enough entropy
/// to reduce false matches when objects are rapidly rewritten.
pub type ObjectETag = [u8; 32];

/// S3-style `(key, value)` tag set attached to an object.
///
/// Distinct from [`ObjectETag`]: writing this leaves the object's bytes, and so its
/// [`ObjectETag`], untouched. That is what lets a caller record something about an object
/// without invalidating anything keyed on its content.
pub type ObjectTagSet = Vec<(String, String)>;

/// Tag key CAS stamps with the unix seconds of a xorb's most recent write.
///
/// Named here so the simulation can model that write (see
/// `LocalTestServerBuilder::with_upload_tagging`). What the value *means* is
/// the reader's business — xet-core only reproduces the stamp.
pub const LAST_UPLOAD_TAG_KEY: &str = "last-upload";

/// The tag set CAS puts on a xorb it has just written.
pub fn last_upload_tag_set_now() -> ObjectTagSet {
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    vec![(LAST_UPLOAD_TAG_KEY.to_string(), unix.to_string())]
}

/// Trait for clients that support deletion and integrity operations on shards and file entries.
///
/// Implemented by `LocalClient` (disk-backed) and `MemoryClient` (in-memory).
/// Operations routed through the local server return 501 if the underlying
/// client does not implement this trait.
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

    /// Returns all XORB hashes with their associated object etags.
    async fn list_xorbs_and_etags(&self) -> Result<Vec<(MerkleHash, ObjectETag)>>;

    /// Deletes a XORB only if its current etag matches the provided etag.
    /// Returns `Ok(true)` if deleted, `Ok(false)` if the etag did not match.
    async fn delete_xorb_if_etag_matches(&self, hash: &MerkleHash, etag: &ObjectETag) -> Result<bool>;

    /// Returns a XORB's tag set, empty if it has none.
    async fn get_xorb_tag_set(&self, hash: &MerkleHash) -> Result<ObjectTagSet>;

    /// Replaces a XORB's tag set wholesale, as S3 `PutObjectTagging` does.
    async fn set_xorb_tag_set(&self, hash: &MerkleHash, tags: ObjectTagSet) -> Result<()>;

    /// Returns all shard hashes with their associated object etags.
    async fn list_shards_with_etags(&self) -> Result<Vec<(MerkleHash, ObjectETag)>>;

    /// Deletes a shard only if its current etag matches the provided etag.
    /// Returns `Ok(true)` if deleted, `Ok(false)` if the etag did not match.
    async fn delete_shard_if_etag_matches(&self, hash: &MerkleHash, etag: &ObjectETag) -> Result<bool>;

    /// Verifies referential integrity of all shards on disk.
    async fn verify_integrity(&self) -> Result<()>;

    /// Verifies completeness: after GC convergence, all on-disk data must be reachable.
    async fn verify_all_reachable(&self) -> Result<()>;
}
