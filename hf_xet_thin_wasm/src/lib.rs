use merklehash::{MerkleHash, xorb_hash};
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
}

impl From<deduplication::Chunk> for JsChunk {
    fn from(value: deduplication::Chunk) -> Self {
        JsChunk {
            hash: value.hash.hex(),
            length: value.data.len() as u32,
        }
    }
}

#[wasm_bindgen(js_name = "Chunker")]
pub struct JsChunker {
    inner: deduplication::Chunker,
}

// Default target chunk size is 64 * 1024

#[wasm_bindgen(js_class = "Chunker")]
impl JsChunker {
    #[wasm_bindgen(constructor)]
    pub fn new(target_chunk_size: usize) -> JsChunker {
        JsChunker {
            inner: deduplication::Chunker::new(target_chunk_size),
        }
    }

    pub fn add_data(&mut self, data: Vec<u8>) -> Result<JsValue, JsValue> {
        let result = self.inner.next_block(&data, false);
        let serializable_result: Vec<JsChunk> = result.into_iter().map(JsChunk::from).collect();
        serde_wasm_bindgen::to_value(&serializable_result).map_err(|e| e.into())
    }

    pub fn finish(&mut self) -> Result<JsValue, JsValue> {
        let mut result: Vec<JsChunk> = vec![];
        if let Some(final_chunk) = self.inner.finish() {
            result.push(JsChunk::from(final_chunk));
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

#[wasm_bindgen]
pub fn compute_xorb_hash(chunks_array: JsValue) -> Result<String, JsValue> {
    let js_chunks: Vec<JsChunk> =
        serde_wasm_bindgen::from_value::<Vec<JsChunk>>(chunks_array).map_err(|e| JsValue::from(e.to_string()))?;

    let chunks: Vec<(MerkleHash, usize)> = js_chunks
        .into_iter()
        .map(|jsc| {
            Ok::<_, JsValue>((
                MerkleHash::from_hex(&jsc.hash).map_err(|e| JsValue::from(e.to_string()))?,
                jsc.length as usize,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(xorb_hash(&chunks).hex())
}
