#[cfg(not(target_family = "wasm"))]
mod parallel_hash_map;
#[cfg(not(target_family = "wasm"))]
mod parallel_hash_map_impl;
#[cfg(not(target_family = "wasm"))]
mod parallel_hash_set;
mod passthrough_hasher;
mod passthrough_hashmap;

use merklehash::MerkleHash;
#[cfg(not(target_family = "wasm"))]
pub use parallel_hash_map::ParallelHashMap;
#[cfg(not(target_family = "wasm"))]
pub use parallel_hash_map_impl::{DEFAULT_READ_CONCURRENCY, DEFAULT_WRITE_CONCURRENCY};
#[cfg(not(target_family = "wasm"))]
pub use parallel_hash_set::ParallelHashSet;
#[cfg(not(target_family = "wasm"))]
use passthrough_hasher::U64DirectHasher;
pub use passthrough_hasher::U64HashExtractable;
pub use passthrough_hashmap::PassThroughHashMap;

/// A HashMap specialized for `MerkleHash` keys using passthrough hashing.
///
/// This is a type alias for `PassThroughHashMap<MerkleHash, Value>`.
pub type MerkleHashMap<Value> = PassThroughHashMap<MerkleHash, Value>;

/// A HashMap specialized for `u64` keys using passthrough hashing.
///
/// This is useful when the key is already a truncated hash value (e.g., the first 8 bytes
/// of a larger hash), and we want to avoid re-hashing.
pub type TruncatedMerkleHashMap<Value> = PassThroughHashMap<u64, Value>;

/// A thread-safe hash map specialized for `MerkleHash` keys using passthrough hashing.
///
/// This uses submap-based partitioning with `U64DirectHasher` for efficient hashing.
#[cfg(not(target_family = "wasm"))]
pub type ParallelMerkleHashMap<Value, const FIRST_LEVEL_SIZE: usize = 64> =
    ParallelHashMap<MerkleHash, Value, U64DirectHasher<MerkleHash>, FIRST_LEVEL_SIZE>;

/// A thread-safe hash set specialized for `MerkleHash` keys using passthrough hashing.
///
/// This uses submap-based partitioning with `U64DirectHasher` for efficient hashing.
#[cfg(not(target_family = "wasm"))]
pub type ParallelMerkleHashSet<const FIRST_LEVEL_SIZE: usize = 64> =
    ParallelHashSet<MerkleHash, U64DirectHasher<MerkleHash>, FIRST_LEVEL_SIZE>;

/// A thread-safe hash map specialized for `u64` keys using passthrough hashing.
///
/// This is useful when the key is already a truncated hash value (e.g., the first 8 bytes
/// of a larger hash), and we want to avoid re-hashing.
#[cfg(not(target_family = "wasm"))]
pub type ParallelTruncatedMerkleHashMap<Value, const FIRST_LEVEL_SIZE: usize = 64> =
    ParallelHashMap<u64, Value, U64DirectHasher<u64>, FIRST_LEVEL_SIZE>;

/// A thread-safe hash set specialized for `u64` keys using passthrough hashing.
///
/// This is useful when the key is already a truncated hash value (e.g., the first 8 bytes
/// of a larger hash), and we want to avoid re-hashing.
#[cfg(not(target_family = "wasm"))]
pub type ParallelTruncatedMerkleHashSet<const FIRST_LEVEL_SIZE: usize = 64> =
    ParallelHashSet<u64, U64DirectHasher<u64>, FIRST_LEVEL_SIZE>;
