use merklehash::MerkleHash;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use crate::error::HFXetJSError;

#[wasm_bindgen]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PointerFile {
    #[serde(with = "merklehash::data_hash::hex::serde")]
    pub(crate) hash: MerkleHash,
    pub(crate) size: u64,
    pub(crate) sha256: String,
}

impl PointerFile {
    pub fn new(hash: MerkleHash, size: u64, sha256: String) -> Self {
        Self {
            hash,
            size,
            sha256,
        }
    }
}
