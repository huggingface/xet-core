use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use xet_core_structures::merklehash::{DataHashHexParseError, MerkleHash, MerkleHashSubtree};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct JsChunkIn {
    hash: String,
    length: u32,
}

fn parse_chunks_in(chunks_array: JsValue) -> Result<Vec<(MerkleHash, u64)>, JsValue> {
    let js_chunks: Vec<JsChunkIn> =
        serde_wasm_bindgen::from_value::<Vec<JsChunkIn>>(chunks_array).map_err(|e| JsValue::from(e.to_string()))?;

    js_chunks
        .into_iter()
        .map(|jsc| Ok((MerkleHash::from_hex(&jsc.hash)?, jsc.length as u64)))
        .collect::<Result<_, DataHashHexParseError>>()
        .map_err(|e| JsValue::from(e.to_string()))
}

/// Wasm wrapper around [`MerkleHashSubtree`] for O(log n) composable hash aggregation.
///
/// Chunks are passed as `Array<{hash: string, length: number}>`.
/// Hashes are returned as hex strings.
#[wasm_bindgen(js_name = "MerkleHashSubtree")]
pub struct JsMerkleHashSubtree {
    inner: MerkleHashSubtree,
}

#[wasm_bindgen(js_class = "MerkleHashSubtree")]
impl JsMerkleHashSubtree {
    /// Create from an array of chunks `[{hash, length}, ...]`.
    ///
    /// - `at_start`: true if these are the first chunks of the file.
    /// - `at_end`: true if these are the last chunks of the file.
    #[wasm_bindgen(constructor)]
    pub fn new(at_start: bool, chunks_array: JsValue, at_end: bool) -> Result<JsMerkleHashSubtree, JsValue> {
        let chunks = parse_chunks_in(chunks_array)?;
        Ok(JsMerkleHashSubtree {
            inner: MerkleHashSubtree::from_chunks(at_start, &chunks, at_end),
        })
    }

    /// Merge another subtree (the right neighbor) into this one.
    pub fn merge_into(&mut self, other: &JsMerkleHashSubtree) -> Result<(), JsValue> {
        self.inner.merge_into(&other.inner).map_err(|e| JsValue::from(e.to_string()))
    }

    /// Returns the final aggregated hash as a hex string, or `undefined`
    /// if both boundaries (`at_start` and `at_end`) are not yet known.
    pub fn final_hash(&self) -> Option<String> {
        self.inner.final_hash().map(|h| h.hex())
    }

    pub fn num_nodes(&self) -> usize {
        self.inner.num_nodes()
    }

    pub fn num_levels(&self) -> usize {
        self.inner.num_levels()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Serialize the subtree to a JS object for storage or transfer.
    pub fn serialize(&self) -> Result<JsValue, JsValue> {
        serde_wasm_bindgen::to_value(&self.inner).map_err(|e| e.into())
    }

    /// Reconstruct a subtree from a previously serialized JS object.
    #[wasm_bindgen(js_name = "deserialize")]
    pub fn from_serialized(data: JsValue) -> Result<JsMerkleHashSubtree, JsValue> {
        let inner: MerkleHashSubtree =
            serde_wasm_bindgen::from_value(data).map_err(|e| JsValue::from(e.to_string()))?;
        Ok(JsMerkleHashSubtree { inner })
    }
}
