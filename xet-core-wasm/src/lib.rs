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
    let chunks = _chunk(data, TARGET_CDC_CHUNK_SIZE)
        .await
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    serde_wasm_bindgen::to_value(&chunks).map_err(JsValue::from)
}

use merklehash::compute_data_hash;
use std::cmp::min;

struct ChunkerState {
    // configs
    hash: gearhash::Hasher<'static>,
    minimum_chunk: usize,
    maximum_chunk: usize,
    // mask: u64,
    // generator state
    chunkbuf: Vec<u8>,
    cur_chunk_len: usize,
}

const MAX_WINDOW_SIZE: usize = 64;
/// TARGET_CDC_CHUNK_SIZE / MINIMUM_CHUNK_DIVISOR is the smallest chunk size
pub const MINIMUM_CHUNK_DIVISOR: usize = 8;
/// TARGET_CDC_CHUNK_SIZE * MAXIMUM_CHUNK_MULTIPLIER is the largest chunk size
pub const MAXIMUM_CHUNK_MULTIPLIER: usize = 2;
/// Target 1024 chunks per CAS block
pub const TARGET_CDC_CHUNK_SIZE: usize = 64 * 1024;

async fn _chunk(data: Vec<u8>, target_chunk_size: usize) -> anyhow::Result<Vec<Chunk>> {
    let mut res: Vec<Chunk> = Vec::new();

    assert_eq!(target_chunk_size.count_ones(), 1);
    assert!(target_chunk_size > 1);
    // note the strict lesser than. Combined with count_ones() == 1,
    // this limits to 2^31
    assert!(target_chunk_size < u32::MAX as usize);

    let mask = (target_chunk_size - 1) as u64;
    // we will like to shift the mask left by a bunch since the right
    // bits of the gear hash are affected by only a small number of bytes
    // really. we just shift it all the way left.
    let mask = mask << mask.leading_zeros();
    let minimum_chunk = target_chunk_size / MINIMUM_CHUNK_DIVISOR;
    let maximum_chunk = target_chunk_size * MAXIMUM_CHUNK_MULTIPLIER;

    assert!(maximum_chunk > minimum_chunk);
    let hash = gearhash::Hasher::default();

    let mut chunker = ChunkerState {
        hash,
        minimum_chunk,
        maximum_chunk,
        // mask,
        // generator state init
        chunkbuf: Vec::with_capacity(maximum_chunk),
        cur_chunk_len: 0,
    };

    let mut offset = 0;

    for readbuf in data.chunks(64 << 10) {
        let read_bytes = readbuf.len();
        // 0 byte read is assumed EOF
        let mut cur_pos = 0;
        while cur_pos < read_bytes {
            // every pass through this loop we either
            // 1: create a chunk
            // OR
            // 2: consume the entire buffer
            let chunk_buf_copy_start = cur_pos;
            // skip the minimum chunk size
            // and noting that the hash has a window size of 64
            // so we should be careful to skip only minimum_chunk - 64 - 1
            if chunker.cur_chunk_len < chunker.minimum_chunk - MAX_WINDOW_SIZE {
                let max_advance =
                    min(chunker.minimum_chunk - chunker.cur_chunk_len - MAX_WINDOW_SIZE - 1, read_bytes - cur_pos);
                cur_pos += max_advance;
                chunker.cur_chunk_len += max_advance;
            }
            let mut consume_len;
            let mut create_chunk = false;
            // find a chunk boundary after minimum chunk

            // If we have a lot of data, don't read all the way to the end when we'll stop reading
            // at the maximum chunk boundary.
            let read_end = read_bytes.min(cur_pos + chunker.maximum_chunk - chunker.cur_chunk_len);

            if let Some(boundary) = chunker.hash.next_match(&readbuf[cur_pos..read_end], mask) {
                consume_len = boundary;
                create_chunk = true;
            } else {
                consume_len = read_end - cur_pos;
            }

            // if we hit maximum chunk we must create a chunk
            if consume_len + chunker.cur_chunk_len >= chunker.maximum_chunk {
                consume_len = chunker.maximum_chunk - chunker.cur_chunk_len;
                create_chunk = true;
            }
            chunker.cur_chunk_len += consume_len;
            cur_pos += consume_len;
            chunker.chunkbuf.extend_from_slice(&readbuf[chunk_buf_copy_start..cur_pos]);
            if create_chunk {
                // cut a chunk, add to the results
                let chunk = Chunk {
                    hash: compute_data_hash(&chunker.chunkbuf),
                    length: chunker.chunkbuf.len() as u32,
                    offset,
                };
                res.push(chunk);
                offset += chunker.chunkbuf.len() as u64;

                // reset chunk buffer state and continue to find the next chunk
                chunker.chunkbuf.clear();
                chunker.cur_chunk_len = 0;

                chunker.hash.set_hash(0);
            }
        }
    }

    // main loop complete
    if !chunker.chunkbuf.is_empty() {
        let chunk = Chunk {
            hash: compute_data_hash(&chunker.chunkbuf),
            length: chunker.chunkbuf.len() as u32,
            offset,
        };
        res.push(chunk);
    }

    Ok(res)
}
