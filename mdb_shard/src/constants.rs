use merklehash::MerkleHash;

utils::test_configurable_constants! {
    /// The global dedup chunk modulus; a chunk is considered global dedup
    /// eligible if the hash modulus this value is zero.
    ref MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS: u64 = 1024;
}

// How the MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS is used.
pub fn hash_is_global_dedup_eligible(h: &MerkleHash) -> bool {
    (*h) % *MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS == 0
}
