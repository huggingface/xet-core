use std::time::Duration;

use merklehash::MerkleHash;

utils::test_configurable_constants! {
    /// The global dedup chunk modulus; a chunk is considered global dedup
    /// eligible if the hash modulus this value is zero.
    ref MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS: u64 = 1024;

    /// The amount of time a shard should be expired by before it's deleted, in seconds.
    /// By default set to 7 days.
    ref MDB_SHARD_EXPIRATION_BUFFER: Duration = Duration::from_secs(7 * 24 * 3600);

    /// The expiration time of a local shard when first placed in the local shard cache.  Currently
    /// set to 3 weeks.
    ref MDB_SHARD_LOCAL_CACHE_EXPIRATION: Duration = Duration::from_secs(3 * 7 * 24 * 3600);
}

// How the MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS is used.
pub fn hash_is_global_dedup_eligible(h: &MerkleHash) -> bool {
    (*h) % *MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS == 0
}
