use std::sync::Arc;

use deduplication::Chunk;
use merklehash::MerkleHash;
use sha2::Sha256;

use super::errors::*;

pub struct ShaGenerator {
    hasher: Sha256,
}

impl ShaGenerator {
    pub fn new() -> Self {
        Self {
            hasher: Sha256::default(),
        }
    }

    pub async fn update(&mut self, new_chunks: Arc<[Chunk]>) -> Result<()> {
        todo!()
    }

    pub async fn finalize(mut self) -> Result<MerkleHash> {
        todo!()
    }
}
