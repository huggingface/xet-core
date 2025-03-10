use merklehash::MerkleHash;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

/// Describes a chunk within the provided data buffer
/// hash being the hash of the chunk and the boundaries of the chunk within the buffer referred
/// to by the offset i.e. the index of the first byte of the chunk and the length of the chunk
#[derive(Debug, Serialize, Deserialize)]
pub struct Chunk {
    #[serde(with = "merklehash::data_hash::hex::serde")]
    hash: MerkleHash,
    length: u32,
    offset: u64,
}

#[wasm_bindgen]
pub async fn chunk(data: Vec<u8>) -> Result<JsValue, JsValue> {
    let chunks = chunking::wasm::chunk(data)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    serde_wasm_bindgen::to_value(&chunks).map_err(JsValue::from)
}

#[wasm_bindgen(getter_with_clone)]
#[derive(Debug, Serialize, Deserialize)]
pub struct PointerFile {
    pub hash: String,
    pub path: String,
    pub filesize: u64,
}

impl PointerFile {
    fn new(hash: MerkleHash, path: String, filesize: u64) -> Self {
        Self {
            hash: hash.hex(),
            path,
            filesize,
        }
    }
}

#[wasm_bindgen(getter_with_clone)]
#[derive(Debug, Serialize, Deserialize)]
pub struct FileInput {
    pub path: String,
    pub data: Vec<u8>,
}

#[wasm_bindgen]
pub async fn upload_files(files: Vec<FileInput>) -> Result<Vec<PointerFile>, JsValue> {
    Err(JsValue::from_str("unimplemented"))
}
