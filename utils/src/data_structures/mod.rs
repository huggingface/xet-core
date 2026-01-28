mod passthrough_hasher;
mod passthrough_hashmap;

use merklehash::MerkleHash;
pub use passthrough_hasher::{U64DirectHasher, U64HashExtractable};
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
