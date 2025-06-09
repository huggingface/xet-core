use bytes::Bytes;
use merklehash::{compute_data_hash, MerkleHash};

#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    pub hash: MerkleHash,
    pub data: Bytes,
}

impl Chunk {
    pub fn new(data: Bytes) -> Self {
        Chunk {
            hash: compute_data_hash(&data),
            data,
        }
    }
}

// Implement &[u8] dereferencing for the Chunk
impl AsRef<[u8]> for Chunk {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}
