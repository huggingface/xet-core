use deduplication::Chunk;
use merklehash::MerkleHash;
use sha2::{Digest, Sha256};

use super::errors::*;

// utilities to generate sha256 in webassembly in rust.
// The `Value` variant hides an already provided result hash as if it were an operation to maintain
// the same interface but do no work.
pub enum ShaGeneration {
    Value(MerkleHash),
    Action(ShaGenerator),
}

impl ShaGeneration {
    pub fn new(hash: Option<MerkleHash>) -> Self {
        match hash {
            Some(h) => Self::Value(h),
            None => Self::Action(ShaGenerator::new()),
        }
    }

    pub fn update(&mut self, new_chunks: &[Chunk]) {
        match self {
            ShaGeneration::Value(_) => {},
            ShaGeneration::Action(sha_generator) => sha_generator.update(new_chunks),
        }
    }

    pub fn update_with_bytes(&mut self, new_bytes: &[u8]) {
        match self {
            ShaGeneration::Value(_) => {},
            ShaGeneration::Action(sha_generator) => sha_generator.update_with_bytes(new_bytes),
        }
    }

    pub fn finalize(self) -> Result<MerkleHash> {
        match self {
            ShaGeneration::Value(hash) => Ok(hash),
            ShaGeneration::Action(sha_generator) => sha_generator.finalize(),
        }
    }
}

// struct to generate a sha256 progressively by calling `update` or the `with_bytes` variation
// yielding the final hash when calling `finalize()`
pub struct ShaGenerator {
    hasher: Sha256,
}

impl ShaGenerator {
    pub fn new() -> Self {
        Self {
            hasher: Sha256::default(),
        }
    }

    pub fn update(&mut self, new_chunks: &[Chunk]) {
        for chunk in new_chunks.iter() {
            self.hasher.update(&chunk.data);
        }
    }

    pub fn update_with_bytes(&mut self, new_bytes: &[u8]) {
        self.hasher.update(new_bytes);
    }

    pub fn finalize(self) -> Result<MerkleHash> {
        let sha256 = self.hasher.finalize();
        let hex_str = format!("{sha256:x}");
        Ok(MerkleHash::from_hex(&hex_str)?)
    }
}
