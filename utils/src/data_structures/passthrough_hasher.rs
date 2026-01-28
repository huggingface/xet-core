use std::hash::{BuildHasher, Hasher};
use std::marker::PhantomData;

use merklehash::DataHash;
use serde::{Deserialize, Serialize};

/// Trait for types that can efficiently provide a u64 hash value.
/// Types implementing this trait are optimized for use with `U64DirectHasher`,
/// which avoids extra computation by directly using the u64 value as the hash.
pub trait U64HashExtractable {
    fn u64_hash_value(&self) -> u64;
}

impl U64HashExtractable for u64 {
    fn u64_hash_value(&self) -> u64 {
        *self
    }
}

impl U64HashExtractable for DataHash {
    fn u64_hash_value(&self) -> u64 {
        self[0]
    }
}

/// A hasher that maps directly to a u64 hash value for types implementing U64HashExtractable.
///
/// This hasher is designed to work with types that already contain high-quality hash values
/// (like cryptographic hashes), avoiding extra computation and supporting direct bucket selection.
///
/// Note: This is not resistant to attacks where all keys are designed to hash to the same bucket
/// and thus make the lookup time linear, but in our use case, where it's already a cryptographic
/// hash, this gives us two advantages:
/// - Speedup for lookup.
/// - Consistent serialization order, so MUCH faster deserialization.
#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct U64DirectHasher<T: U64HashExtractable> {
    state: u64,
    #[serde(skip)]
    _phantom: PhantomData<T>,
}

impl<T: U64HashExtractable> Default for U64DirectHasher<T> {
    fn default() -> Self {
        Self {
            state: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T: U64HashExtractable> Hasher for U64DirectHasher<T> {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        debug_assert!(bytes.len() >= 8);

        unsafe {
            let dest = &mut self.state as *mut u64 as *mut u8;
            bytes.as_ptr().copy_to_nonoverlapping(dest, 8);
        }
    }

    fn write_u64(&mut self, i: u64) {
        self.state = i;
    }
}

impl<T: U64HashExtractable> BuildHasher for U64DirectHasher<T> {
    type Hasher = U64DirectHasher<T>;

    fn build_hasher(&self) -> Self::Hasher {
        U64DirectHasher::default()
    }
}

#[cfg(test)]
mod tests {
    use std::hash::Hash;

    use merklehash::MerkleHash;

    use super::*;

    #[test]
    fn test_hash_uses_first_u64() {
        let mut hasher1 = U64DirectHasher::<MerkleHash>::default();
        let mut hasher2 = U64DirectHasher::<MerkleHash>::default();

        let hash1 = DataHash::from([0x1234567890ABCDEF, 0, 0, 0]);
        let hash2 = DataHash::from([
            0x1234567890ABCDEF,
            0xFFFFFFFFFFFFFFFF,
            0xFFFFFFFFFFFFFFFF,
            0xFFFFFFFFFFFFFFFF,
        ]);

        hash1.hash(&mut hasher1);
        hash2.hash(&mut hasher2);

        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_hash_different_first_u64() {
        let mut hasher1 = U64DirectHasher::<MerkleHash>::default();
        let mut hasher2 = U64DirectHasher::<MerkleHash>::default();

        let hash1 = DataHash::from([0x1234567890ABCDEF, 0, 0, 0]);
        let hash2 = DataHash::from([0xFEDCBA0987654321, 0, 0, 0]);

        hash1.hash(&mut hasher1);
        hash2.hash(&mut hasher2);

        assert_ne!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_u64_hash_extractable() {
        let value: u64 = 0x1234567890ABCDEF;
        assert_eq!(value.u64_hash_value(), 0x1234567890ABCDEF);

        let hash = MerkleHash::from([0xFEDCBA0987654321, 0x1111111111111111, 0, 0]);
        assert_eq!(hash.u64_hash_value(), 0xFEDCBA0987654321);
    }

    #[test]
    fn test_u64_passthrough_hasher() {
        let mut hasher1 = U64DirectHasher::<u64>::default();
        let mut hasher2 = U64DirectHasher::<u64>::default();

        let value1: u64 = 0x1234567890ABCDEF;
        let value2: u64 = 0x1234567890ABCDEF;

        value1.hash(&mut hasher1);
        value2.hash(&mut hasher2);

        assert_eq!(hasher1.finish(), hasher2.finish());
        assert_eq!(hasher1.finish(), value1);
    }
}
