use std::sync::Arc;

use deduplication::Chunk;
use merklehash::MerkleHash;
use sha2::{Digest, Sha256};

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

    pub async fn update(&mut self, new_chunks: Arc<[Chunk]>) {
        for chunk in new_chunks.iter() {
            self.hasher.update(&chunk.data);
        }
    }

    pub async fn finalize(self) -> Result<MerkleHash> {
        let sha256 = self.hasher.finalize();
        let hex_str = format!("{sha256:x}");
        Ok(MerkleHash::from_hex(&hex_str)?)
    }
}
