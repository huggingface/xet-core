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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsXorb {
    pub hash: String,
    pub chunk_hashes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsShardData {
    pub hmac_key: String,
    pub xorbs: Vec<JsXorb>,
    pub all_chunk_hashes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsChunkSearchResult {
    pub found: bool,
    pub xorb_index: Option<usize>,
    pub chunk_index: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsXorbInfo {
    pub stored_hash: String,
    pub computed_hash: String,
    pub chunk_hashes: Vec<String>,
    pub chunk_lengths: Vec<u32>,
}

impl JsChunkOut {
    fn new_with_dedup(chunk: deduplication::Chunk, is_first_chunk: bool) -> Self {
        let hash_eligible = mdb_shard::constants::hash_is_global_dedup_eligible(&chunk.hash);
        JsChunkOut {
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
        let mut serializable_result: Vec<JsChunkOut> = Vec::with_capacity(result.len());
        
        for chunk in result {
            let is_first = !self.first_chunk_outputted;
            serializable_result.push(JsChunkOut::new_with_dedup(chunk, is_first));
            self.first_chunk_outputted = true;
        }
        
        serde_wasm_bindgen::to_value(&serializable_result).map_err(|e| e.into())
    }

    pub fn finish(&mut self) -> Result<JsValue, JsValue> {
        let mut result: Vec<JsChunkOut> = vec![];
        if let Some(final_chunk) = self.inner.finish() {
            let is_first = !self.first_chunk_outputted;
            result.push(JsChunkOut::new_with_dedup(final_chunk, is_first));
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
    let js_chunks: Vec<JsChunkIn> =
        serde_wasm_bindgen::from_value::<Vec<JsChunkIn>>(chunks_array).map_err(|e| JsValue::from(e.to_string()))?;

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
        serde_wasm_bindgen::from_value::<Vec<JsChunkIn>>(chunks_array).map_err(|e| JsValue::from(e.to_string()))?;

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

/// takes a hash and HMAC key (both as hex strings) and returns the HMAC result as a hex string
#[wasm_bindgen]
pub fn compute_hmac(hash_hex: &str, hmac_key_hex: &str) -> Result<String, JsValue> {
    let hash = MerkleHash::from_hex(hash_hex)
        .map_err(|e| JsValue::from(format!("Invalid hash hex: {}", e)))?;
    
    let hmac_key = MerkleHash::from_hex(hmac_key_hex)
        .map_err(|e| JsValue::from(format!("Invalid HMAC key hex: {}", e)))?;
    
    let hmac_result = hash.hmac(hmac_key.into());
    Ok(hmac_result.hex())
}


/// takes a shard as Vec<u8> and returns the HMAC key, xorbs with their hashes, and all chunk hashes
#[wasm_bindgen]
pub fn deserialize_shard(shard_data: Vec<u8>) -> Result<JsValue, JsValue> {
    use std::io::Cursor;
    use mdb_shard::shard_format::MDBShardInfo;
    
    let mut cursor = Cursor::new(shard_data);
    
    // Load the shard info (header and footer)
    let shard_info = MDBShardInfo::load_from_reader(&mut cursor)
        .map_err(|e| JsValue::from(format!("Failed to load shard info: {}", e)))?;
    
    // Extract HMAC key
    let hmac_key = shard_info.metadata.chunk_hash_hmac_key.hex();
    
    // Read all CAS blocks (xorbs and their chunks)
    let cas_blocks = shard_info.read_all_cas_blocks_full(&mut cursor)
        .map_err(|e| JsValue::from(format!("Failed to read CAS blocks: {}", e)))?;
    
    let mut xorbs = Vec::new();
    let mut all_chunk_hashes = Vec::new();
    
    for cas_block in cas_blocks {
        // Extract xorb hash from the metadata
        let xorb_hash = cas_block.metadata.cas_hash.hex();
        
        // Extract chunk hashes from the chunks
        let chunk_hashes: Vec<String> = cas_block.chunks
            .iter()
            .map(|chunk| chunk.chunk_hash.hex())
            .collect();
        
        // Add chunk hashes to the flat list
        all_chunk_hashes.extend(chunk_hashes.clone());
        
        xorbs.push(JsXorb {
            hash: xorb_hash,
            chunk_hashes,
        });
    }
    
    let result = JsShardData {
        hmac_key,
        xorbs,
        all_chunk_hashes,
    };
    
    serde_wasm_bindgen::to_value(&result).map_err(|e| e.into())
}

/// takes a shard as Vec<u8> and a chunk hash as hex string, and checks if the shard contains 
/// the HMAC'd version of that chunk hash (using the shard's HMAC key)
/// returns an object with found (bool), xorb_index (number or null), and chunk_index (number or null)
#[wasm_bindgen]
pub fn shard_contains_chunk(shard_data: Vec<u8>, chunk_hash_hex: &str) -> Result<JsValue, JsValue> {
    use std::io::Cursor;
    use mdb_shard::shard_format::MDBShardInfo;
    
    let mut cursor = Cursor::new(shard_data);
    
    // Load the shard info (header and footer)
    let shard_info = MDBShardInfo::load_from_reader(&mut cursor)
        .map_err(|e| JsValue::from(format!("Failed to load shard info: {}", e)))?;
    
    // Extract HMAC key
    let hmac_key = shard_info.metadata.chunk_hash_hmac_key;
    
    // Parse the input chunk hash
    let chunk_hash = MerkleHash::from_hex(chunk_hash_hex)
        .map_err(|e| JsValue::from(format!("Invalid chunk hash hex: {}", e)))?;
    
    // Compute HMAC of the chunk hash with the shard's HMAC key
    let hmac_result = chunk_hash.hmac(hmac_key.into());
    
    // Read all CAS blocks and check if any chunk matches the HMAC'd hash
    let cas_blocks = shard_info.read_all_cas_blocks_full(&mut cursor)
        .map_err(|e| JsValue::from(format!("Failed to read CAS blocks: {}", e)))?;
    
    for (i, cas_block) in cas_blocks.iter().enumerate() {
        for (j, chunk) in cas_block.chunks.iter().enumerate() {
            if chunk.chunk_hash == hmac_result {
                                 let result = JsChunkSearchResult {
                     found: true,
                     xorb_index: Some(i),
                     chunk_index: Some(j),
                 };
                 return serde_wasm_bindgen::to_value(&result).map_err(|e| e.into());
            }
        }
    }
    
         let result = JsChunkSearchResult {
         found: false,
         xorb_index: None,
         chunk_index: None,
     };
     serde_wasm_bindgen::to_value(&result).map_err(|e| e.into())
}

/// takes a xorb as Vec<u8> and deserializes it to extract stored hash, chunks, and compute the xorb hash
#[wasm_bindgen]
pub fn deserialize_xorb(xorb_data: Vec<u8>) -> Result<JsValue, JsValue> {
    use std::io::Cursor;
    use mdb_shard::cas_structs::MDBCASInfo;
    
    let mut cursor = Cursor::new(xorb_data);
    
    // Deserialize the CAS (xorb) info
    let cas_info = MDBCASInfo::deserialize(&mut cursor)
        .map_err(|e| JsValue::from(format!("Failed to deserialize xorb: {}", e)))?;
    
    let cas_info = cas_info.ok_or_else(|| JsValue::from("Invalid xorb data: bookend entry found"))?;
    
    // Extract stored hash from metadata
    let stored_hash = cas_info.metadata.cas_hash.hex();
    
    // Extract chunks and their lengths
    let chunk_hashes: Vec<String> = cas_info.chunks
        .iter()
        .map(|chunk| chunk.chunk_hash.hex())
        .collect();
    
    let chunk_lengths: Vec<u32> = cas_info.chunks
        .iter()
        .map(|chunk| chunk.unpacked_segment_bytes)
        .collect();
    
    // Compute xorb hash from chunks
    let chunks_for_hash: Vec<(MerkleHash, usize)> = cas_info.chunks
        .iter()
        .map(|chunk| (chunk.chunk_hash, chunk.unpacked_segment_bytes as usize))
        .collect();
    
    let computed_hash = merklehash::xorb_hash(&chunks_for_hash).hex();
    
    let result = JsXorbInfo {
        stored_hash,
        computed_hash,
        chunk_hashes,
        chunk_lengths,
    };
    
    serde_wasm_bindgen::to_value(&result).map_err(|e| e.into())
}