use std::convert::Into;
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

#[wasm_bindgen]
impl PointerFile {
    #[wasm_bindgen(getter)]
    pub fn hash(&self) -> JsValue {
        JsValue::from(self.hash.to_string())
    }

    #[wasm_bindgen(getter)]
    pub fn size(&self) -> JsValue {
        JsValue::from(self.size)
    }

    #[wasm_bindgen(getter)]
    pub fn sha256(&self) -> JsValue {
        JsValue::from(self.sha256.to_string())
    }
}
