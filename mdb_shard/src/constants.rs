pub const MDB_SHARD_TARGET_SIZE: u64 = 64 * 1024 * 1024;
pub const MDB_SHARD_MIN_TARGET_SIZE: u64 = 48 * 1024 * 1024;

pub const MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS: u64 = 1024;

// How the MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS is used.
pub fn hash_is_global_dedup_eligible(h: &merklehash::MerkleHash) -> bool {
    (*h) % MDB_SHARD_GLOBAL_DEDUP_CHUNK_MODULUS == 0
}
