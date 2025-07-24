use merklehash::{DataHashHexParseError, MerkleHash, xorb_hash};
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

macro_rules! console_log {
    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsChunk {
    pub hash: String,
    pub length: u32,
    pub dedup: bool,
}



impl JsChunk {
    fn new_with_dedup(chunk: deduplication::Chunk, is_first_chunk: bool) -> Self {
        let hash_eligible = mdb_shard::constants::hash_is_global_dedup_eligible(&chunk.hash);
        JsChunk {
            hash: chunk.hash.hex(),
            length: chunk.data.len() as u32,
            dedup: is_first_chunk || hash_eligible,
        }
    }
}

#[wasm_bindgen(js_name = "Chunker")]
pub struct JsChunker {
    inner: deduplication::Chunker,
    first_chunk_outputted: bool,
}

// Default target chunk size is 64 * 1024

#[wasm_bindgen(js_class = "Chunker")]
impl JsChunker {
    #[wasm_bindgen(constructor)]
    pub fn new(target_chunk_size: usize) -> JsChunker {
        JsChunker {
            inner: deduplication::Chunker::new(target_chunk_size),
            first_chunk_outputted: false,
        }
    }

    pub fn add_data(&mut self, data: Vec<u8>) -> Result<JsValue, JsValue> {
        let result = self.inner.next_block(&data, false);
        let mut serializable_result: Vec<JsChunk> = Vec::with_capacity(result.len());
        
        for chunk in result {
            let is_first = !self.first_chunk_outputted;
            serializable_result.push(JsChunk::new_with_dedup(chunk, is_first));
            self.first_chunk_outputted = true;
        }
        
        serde_wasm_bindgen::to_value(&serializable_result).map_err(|e| e.into())
    }

    pub fn finish(&mut self) -> Result<JsValue, JsValue> {
        let mut result: Vec<JsChunk> = vec![];
        if let Some(final_chunk) = self.inner.finish() {
            let is_first = !self.first_chunk_outputted;
            result.push(JsChunk::new_with_dedup(final_chunk, is_first));
            self.first_chunk_outputted = true;
        };
        serialize_result(&result)
    }
}

#[inline]
fn serialize_result<T: Serialize>(result: &T) -> Result<JsValue, JsValue> {
    let res = serde_wasm_bindgen::to_value(result).map_err(|e| e.into());
    console_log!("{res:?}");
    res
}

/// takes an Array of Objects of the form { "hash": string, "length": number }
/// and returns a string of a hash
#[wasm_bindgen]
pub fn compute_xorb_hash(chunks_array: JsValue) -> Result<String, JsValue> {
    let js_chunks: Vec<JsChunk> =
        serde_wasm_bindgen::from_value::<Vec<JsChunk>>(chunks_array).map_err(|e| JsValue::from(e.to_string()))?;

    let chunks: Vec<(MerkleHash, usize)> = js_chunks
        .into_iter()
        .map(|jsc| Ok((MerkleHash::from_hex(&jsc.hash)?, jsc.length as usize)))
        .collect::<Result<_, DataHashHexParseError>>()
        .map_err(|e| JsValue::from(e.to_string()))?;

    Ok(xorb_hash(&chunks).hex())
}

/// takes an Array of Objects of the form { "hash": string, "length": number }
/// and returns a string of a hash
#[wasm_bindgen]
pub fn compute_file_hash(chunks_array: JsValue) -> Result<String, JsValue> {
    let js_chunks =
        serde_wasm_bindgen::from_value::<Vec<JsChunk>>(chunks_array).map_err(|e| JsValue::from(e.to_string()))?;

    let chunk_list: Vec<(MerkleHash, usize)> = js_chunks
        .into_iter()
        .map(|jsc| Ok((MerkleHash::from_hex(&jsc.hash)?, jsc.length as usize)))
        .collect::<Result<_, DataHashHexParseError>>()
        .map_err(|e| JsValue::from(e.to_string()))?;

    Ok(merklehash::file_hash(&chunk_list).hex())
}

/// takes an Array of hashes as strings and returns the verification hash for that range of chunk hashes
#[wasm_bindgen]
pub fn compute_verification_hash(chunk_hashes: Vec<String>) -> Result<String, JsValue> {
    let chunk_hashes: Vec<MerkleHash> = chunk_hashes
        .into_iter()
        .map(|hash| MerkleHash::from_hex(&hash))
        .collect::<Result<_, DataHashHexParseError>>()
        .map_err(|e| JsValue::from(e.to_string()))?;
    Ok(mdb_shard::chunk_verification::range_hash_from_chunks(&chunk_hashes).hex())
}
