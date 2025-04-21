use cas_types::HexMerkleHash;
use chunking::{Chunker, TARGET_CHUNK_SIZE};
use merklehash::MerkleHash;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use web_sys::js_sys::{ArrayBuffer, Uint8Array};

const INGESTION_BLOCK_SIZE: usize = 8 * 1024 * 1024;

#[wasm_bindgen]
#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkInfo {
    len: u32,
    hash: HexMerkleHash,
}

#[wasm_bindgen]
impl ChunkInfo {
    #[wasm_bindgen(constructor)]
    pub fn js_new(len: u32, hash: String) -> Self {
        let hash = MerkleHash::from_hex(&hash).expect("failed to parse hex hash").into();
        Self { len, hash }
    }

    #[wasm_bindgen(getter)]
    pub fn len(&self) -> u32 {
        self.len
    }

    #[wasm_bindgen(getter, js_name = "hash")]
    pub fn _hash(&self) -> String {
        self.hash.to_string()
    }
}

/// takes a Uint8Array of bytes representing data
#[wasm_bindgen]
pub fn chunk_vec(data: Vec<u8>) -> JsValue {
    let mut chunker = Chunker::new(*TARGET_CHUNK_SIZE);

    let mut result = Vec::new();
    for vec_chunk in data.chunks(INGESTION_BLOCK_SIZE) {
        let chunks = chunker.next_block(vec_chunk, false);
        for chunk in chunks {
            result.push(ChunkInfo {
                len: chunk.data.len() as u32,
                hash: chunk.hash.into(),
            });
        }
    }

    if let Some(chunk) = chunker.finish() {
        result.push(ChunkInfo {
            len: chunk.data.len() as u32,
            hash: chunk.hash.into(),
        });
    }

    serde_wasm_bindgen::to_value(&result).expect("failed to serialize result")
}

#[wasm_bindgen]
pub fn chunk_array_buffer(data: ArrayBuffer) -> JsValue {
    let mut chunker = Chunker::new(*TARGET_CHUNK_SIZE);

    let mut result = Vec::new();

    let len = data.byte_length();
    for i in (0..len).step_by(INGESTION_BLOCK_SIZE) {
        let data_slice = Uint8Array::new(&data.slice_with_end(i, (i + INGESTION_BLOCK_SIZE as u32).min(len)));
        let chunks = chunker.next_block(&data_slice.to_vec(), false);
        for chunk in chunks {
            result.push(ChunkInfo {
                len: chunk.data.len() as u32,
                hash: chunk.hash.into(),
            });
        }
    }

    if let Some(chunk) = chunker.finish() {
        result.push(ChunkInfo {
            len: chunk.data.len() as u32,
            hash: chunk.hash.into(),
        });
    }

    serde_wasm_bindgen::to_value(&result).expect("failed to serialize result")
}
