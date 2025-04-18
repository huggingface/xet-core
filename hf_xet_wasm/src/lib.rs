use cas_types::HexMerkleHash;
use chunking::{Chunker, TARGET_CHUNK_SIZE};
use serde::{Deserialize, Serialize};
use wasm_bindgen::__rt::VectorIntoJsValue;
use wasm_bindgen::prelude::*;
use web_sys::js_sys::Array;
// extern "C" {}

const INGESTION_BLOCK_SIZE: usize = 8 * 1024 * 1024;

// #[wasm_bindgen(js_name = Chunker)]
// pub struct WasmChunker {
//     inner: Chunker,
// }

#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkInfo {
    len: u32,
    hash: HexMerkleHash,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkInfos {
    chunks: Vec<ChunkInfo>,
}

// #[wasm_bindgen(js_class = Foo)]
// impl WasmChunker {
//     #[wasm_bindgen(constructor)]
//     pub fn new() -> Self {
//         WasmChunker {
//             inner: Chunker::default(),
//         }
//     }
//
//     /// chunk returns the chunk lengths given the passed in data
//     #[wasm_bindgen(method)]
//     pub fn chunk(mut self, data: Vec<u8>) -> Vec<ChunkInfo> {
//         let mut result = Vec::with_capacity(2 * data.len() / *TARGET_CHUNK_SIZE);
//         for vec_chunk in data.chunks(INGESTION_BLOCK_SIZE) {
//             let chunks = self.inner.next_block(vec_chunk, false);
//             for chunk in chunks {
//                 result.push(ChunkInfo {
//                     len: chunk.data.len() as u32,
//                     hash: chunk.hash.into(),
//                 });
//             }
//         }
//         if let Some(chunk) = self.inner.finish() {
//             result.push(ChunkInfo {
//                 len: chunk.data.len() as u32,
//                 hash: chunk.hash.into(),
//             });
//         }
//
//         result
//     }
// }

#[wasm_bindgen]
pub fn chunk(data: Vec<u8>) -> Array {
    let mut chunker = Chunker::new(INGESTION_BLOCK_SIZE);

    let mut result = Array::new(); // Vec::with_capacity(2 * data.len() / *TARGET_CHUNK_SIZE);
    for vec_chunk in data.chunks(INGESTION_BLOCK_SIZE) {
        let chunks = chunker.next_block(vec_chunk, false);
        for chunk in chunks {
            result.push(&serde_wasm_bindgen::to_value(&ChunkInfo {
                len: chunk.data.len() as u32,
                hash: chunk.hash.into(),
            }).expect("failed to serialize chunk"));
        }
    }
    if let Some(chunk) = chunker.finish() {
        result.push(&serde_wasm_bindgen::to_value(&ChunkInfo {
            len: chunk.data.len() as u32,
            hash: chunk.hash.into(),
        }).expect("failed to serialize chunk"));
    }


    result
}
