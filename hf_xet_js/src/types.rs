use merklehash::MerkleHash;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use crate::error::HFXetJSError;

#[wasm_bindgen]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PointerFile {
    // #[wasm_bindgen(getter, readonly)]
    #[serde(with = "merklehash::data_hash::hex::serde")]
    pub(crate) hash: MerkleHash,
    // #[wasm_bindgen(getter, readonly)]
    pub(crate) size: u64,
    // #[wasm_bindgen(getter, readonly)]
    pub(crate) path: String,
    // #[wasm_bindgen(getter, readonly)]
    pub(crate) sha256: String,
}

impl PointerFile {
    pub fn new(hash: MerkleHash, size: u64, path: String, sha256: String) -> Self {
        Self {
            hash,
            size,
            path,
            sha256,
        }
    }
}

// #[wasm_bindgen]
// impl PointerFile {
//
//
//     #[wasm_bindgen(constructor)]
//     pub fn js_new(hash: String, size: u64, path: String, sha256: String) -> Result<Self, HFXetJSError> {
//         let hash = MerkleHash::from_hex(&hash)?;
//         Ok(Self::new(hash, size, path, sha256))
//     }
// }
