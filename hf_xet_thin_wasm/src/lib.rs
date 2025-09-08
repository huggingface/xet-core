use merklehash::{DataHashHexParseError, MerkleHash};
use serde::{Deserialize, Serialize};
// use std::cell::RefCell;
// use std::fmt::Write;
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
pub struct JsChunkIn {
    pub hash: String,
    pub length: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsChunkOut {
    pub hash: String,
    pub length: u32,
    pub dedup: bool,
}

impl JsChunkOut {
    fn new_with_dedup(chunk: deduplication::Chunk) -> Self {
        let hash_eligible = mdb_shard::constants::hash_is_global_dedup_eligible(&chunk.hash);
        JsChunkOut {
            hash: chunk.hash.hex(),
            length: chunk.data.len() as u32,
            dedup:  hash_eligible,
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
        let mut serializable_result: Vec<JsChunkOut> = Vec::with_capacity(result.len());

        for chunk in result {
            serializable_result.push(JsChunkOut::new_with_dedup(chunk));
            self.first_chunk_outputted = true;
        }

        serde_wasm_bindgen::to_value(&serializable_result).map_err(|e| e.into())
    }

    pub fn finish(&mut self) -> Result<JsValue, JsValue> {
        let mut result: Vec<JsChunkOut> = vec![];
        if let Some(final_chunk) = self.inner.finish() {
            result.push(JsChunkOut::new_with_dedup(final_chunk));
            self.first_chunk_outputted = true;
        };

        serde_wasm_bindgen::to_value(&result).map_err(|e| e.into())
    }
}

#[inline]
fn parse_chunks(chunks_in: Vec<JsChunkIn>) -> Result<Vec<(MerkleHash, u64)>, DataHashHexParseError> {
    chunks_in
        .into_iter()
        .map(|jsc| Ok((MerkleHash::from_hex(&jsc.hash)?, jsc.length as u64)))
        .collect::<Result<_, DataHashHexParseError>>()
}

/// takes an Array of Objects of the form { "hash": string, "length": number }
/// and returns a string of a hash
#[wasm_bindgen]
pub fn compute_xorb_hash(chunks_array: JsValue) -> Result<String, JsValue> {
    let js_chunks: Vec<JsChunkIn> =
        serde_wasm_bindgen::from_value::<Vec<JsChunkIn>>(chunks_array).map_err(|e| JsValue::from(e.to_string()))?;
    let chunks = parse_chunks(js_chunks).map_err(|e| JsValue::from(e.to_string()))?;
    let xorb_hash = merklehash::xorb_hash(&chunks).hex();

    // console_log!("computed xorb hash with {} chunks, file_len: {} {}", num_chunks, total_len, xorb_hash);

    Ok(xorb_hash)
}

/// takes an Array of Objects of the form { "hash": string, "length": number }
/// and returns a string of a hash
#[wasm_bindgen]
pub fn compute_file_hash(chunks_array: JsValue) -> Result<String, JsValue> {
    let js_chunks =
        serde_wasm_bindgen::from_value::<Vec<JsChunkIn>>(chunks_array).map_err(|e| JsValue::from(e.to_string()))?;

    let chunk_list = parse_chunks(js_chunks).map_err(|e| JsValue::from(e.to_string()))?;

    let file_hash = merklehash::file_hash(&chunk_list).hex();
    // let file_hash = file_hash_with_salt(&chunk_list).hex();

    console_log!("computed file hash: {}", file_hash);

    Ok(file_hash)
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

/// takes a hash and HMAC key (both as hex strings) and returns the HMAC result as a hex string
#[wasm_bindgen]
pub fn compute_hmac(hash_hex: &str, hmac_key_hex: &str) -> Result<String, JsValue> {
    let hash = MerkleHash::from_hex(hash_hex).map_err(|e| JsValue::from(format!("Invalid hash hex: {}", e)))?;

    let hmac_key =
        MerkleHash::from_hex(hmac_key_hex).map_err(|e| JsValue::from(format!("Invalid HMAC key hex: {}", e)))?;

    let hmac_result = hash.hmac(hmac_key.into());
    Ok(hmac_result.hex())
}

// pub const AGGREGATED_HASHES_MEAN_TREE_BRANCHING_FACTOR: u64 = 4;
//
// /// Find the next cut point in a sequence of hashes at which to break.
// ///
// ///
// /// We basically loop through the set of nodes tracking a window between
// /// cur_children_start_idx and idx (current index).
// /// [. . . . . . . . . . . ]
// ///          ^   ^
// ///          |   |
// ///  start_idx   |
// ///              |
// ///             idx
// ///
// /// When the current node at idx satisfies the cut condition:
// ///  - the hash % MEAN_TREE_BRANCHING_FACTOR == 0: assuming a random hash distribution, this implies on average, the
// ///    number of children is AGGREGATED_HASHES_MEAN_TREE_BRANCHING_FACTOR,
// ///  - OR this is the last node in the list.
// ///  - subject to each parent must have at least 2 children, and at most AGGREGATED_MEAN_TREE_BRANCHING_FACTOR * 2
// ///    children: This ensures that the graph always has at most 1/2 the number of parents as children. and we don't have
// ///    too wide branches.
// #[inline]
// fn next_merge_cut(hashes: &[(MerkleHash, usize)]) -> usize {
//     if hashes.len() <= 2 {
//         return hashes.len();
//     }
//
//     let end = (2 * AGGREGATED_HASHES_MEAN_TREE_BRANCHING_FACTOR as usize + 1).min(hashes.len());
//
//     for i in 2..end {
//         let h = unsafe { hashes.get_unchecked(i).0 };
//
//         if h % AGGREGATED_HASHES_MEAN_TREE_BRANCHING_FACTOR == 0 {
//             return i + 1;
//         }
//     }
//
//     end
// }
//
// /// Merge the hashes together, including the size information and returning the new (hash, size) pair.
// #[inline]
// fn merged_hash_of_sequence(hash: &[(MerkleHash, usize)]) -> (MerkleHash, usize) {
//     // Use a threadlocal buffer to avoid the overhead of reallocations.
//     thread_local! {
//         static BUFFER: RefCell<String> =
//         RefCell::new(String::with_capacity(1024));
//     }
//
//     BUFFER.with(|buffer| {
//         let mut buf = buffer.borrow_mut();
//         buf.clear();
//         let mut total_len = 0;
//
//         for (h, s) in hash.iter() {
//             writeln!(buf, "{h:x} : {s}").unwrap();
//             total_len += *s;
//         }
//         (compute_internal_node_hash(buf.as_bytes()), total_len)
//     })
// }
//
// /// The base calculation for the aggregated node hash.
// ///
// /// Iteratively collapse the list of hashes using the criteria in next_merge_cut
// /// until only one hash remains; this is the aggregated hash.
// #[inline]
// fn aggregated_node_hash(chunks: &[(MerkleHash, usize)]) -> MerkleHash {
//     if chunks.is_empty() {
//         return MerkleHash::default();
//     }
//
//     let mut hv = chunks.to_vec();
//
//     let mut round = 0;
//     while hv.len() > 1 {
//         round += 1;
//         let mut write_idx = 0;
//         let mut read_idx = 0;
//
//         while read_idx != hv.len() {
//             // Find the next cut point of hashes at which to merge.
//             let next_cut = read_idx + next_merge_cut(&hv[read_idx..]);
//
//             // Get the merged hash of this block.
//             hv[write_idx] = merged_hash_of_sequence(&hv[read_idx..next_cut]);
//             console_log!("round: {round} cut: {next_cut} merged hash {} {}", hv[write_idx].0, hv[write_idx].1);
//             write_idx += 1;
//
//             read_idx = next_cut;
//         }
//
//         hv.resize(write_idx, Default::default());
//     }
//
//     hv[0].0
// }
//
// /// The file hash when a salt is needed.
// #[inline]
// pub fn file_hash_with_salt(chunks: &[(MerkleHash, usize)]) -> MerkleHash {
//     let salt = &[0; 32];
//     if chunks.is_empty() {
//         return MerkleHash::default();
//     }
//
//
//     let agg = aggregated_node_hash(chunks);
//     console_log!("pre salt: {agg}");
//     agg.hmac(salt.into())
// }
