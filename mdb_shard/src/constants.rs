pub const MDB_SHARD_TARGET_SIZE: u64 = 64 * 1024 * 1024;
pub const MDB_SHARD_MIN_TARGET_SIZE: u64 = 48 * 1024 * 1024;

pub const MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS: u64 = 1024;

/// The amount of time a shard should be expired by before it's deleted, in seconds.  
/// By default set to 7 days.
pub const MDB_SHARD_EXPIRATION_BUFFER_SECS: u64 = 7 * 24 * 3600;

// How the MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS is used.
pub fn hash_is_global_dedup_eligible(h: &merklehash::MerkleHash) -> bool {
    (*h) % MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS == 0
}
