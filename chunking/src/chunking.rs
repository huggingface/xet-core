use std::cmp::min;
use std::sync::Arc;

use merklehash::{MerkleHash, compute_data_hash};

use crate::constants::{MAXIMUM_CHUNK_MULTIPLIER, MINIMUM_CHUNK_DIVISOR, TARGET_CDC_CHUNK_SIZE};

#[derive(Debug, Clone)]
pub struct Chunk {
    pub hash: MerkleHash,
    pub length: usize,
    pub data: Option<Arc<[u8]>>,
}

/// Chunk Generator given an input stream. Do not use directly.
/// Use `chunk_target_default`.
pub struct Chunker {
    // configs
    hash: gearhash::Hasher<'static>,
    minimum_chunk: usize,
    maximum_chunk: usize,
    mask: u64,

    // generator state
    chunkbuf: Vec<u8>,
    cur_chunk_len: usize,

    // input / output channels
    chunk_callback: Box<dyn FnMut(Chunk) + Sync + Send>,
}

impl Chunker {
    pub fn new_default(chunk_callback: Box<dyn FnMut(Chunk) + Sync + Send>) -> Self {
        Self::new(TARGET_CDC_CHUNK_SIZE, chunk_callback)
    }

    pub fn new(target_chunk_size: usize, chunk_callback: Box<dyn FnMut(Chunk) + Sync + Send>) -> Self {
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

        Chunker {
            hash,
            minimum_chunk,
            maximum_chunk,
            mask,
            // generator state init
            chunkbuf: Vec::with_capacity(maximum_chunk),
            cur_chunk_len: 0,
            chunk_callback,
        }
    }

    pub fn add_data(&mut self, data: &[u8]) {
        const MAX_WINDOW_SIZE: usize = 64;

        let read_bytes = data.len();

        let mut data_pos = 0;
        while data_pos < read_bytes {
            // every pass through this loop we either
            // 1: create a chunk
            // OR
            // 2: consume the entire buffer
            let chunk_buf_copy_start = data_pos;

            // skip the minimum chunk size
            // and noting that the hash has a window size of 64
            // so we should be careful to skip only minimum_chunk - 64 - 1
            if self.cur_chunk_len < self.minimum_chunk - MAX_WINDOW_SIZE {
                let max_advance =
                    min(self.minimum_chunk - self.cur_chunk_len - MAX_WINDOW_SIZE - 1, read_bytes - data_pos);
                data_pos += max_advance;
                self.cur_chunk_len += max_advance;
            }
            let mut consume_len;
            let mut create_chunk = false;
            // find a chunk boundary after minimum chunk

            // If we have a lot of data, don't read all the way to the end when we'll stop reading
            // at the maximum chunk boundary.
            let read_end = read_bytes.min(data_pos + self.maximum_chunk - self.cur_chunk_len);

            if let Some(boundary) = self.hash.next_match(&data[data_pos..read_end], self.mask) {
                consume_len = boundary;
                create_chunk = true;
            } else {
                consume_len = read_end - data_pos;
            }

            // if we hit maximum chunk we must create a chunk
            if consume_len + self.cur_chunk_len >= self.maximum_chunk {
                consume_len = self.maximum_chunk - self.cur_chunk_len;
                create_chunk = true;
            }
            self.cur_chunk_len += consume_len;
            data_pos += consume_len;
            self.chunkbuf.extend_from_slice(&data[chunk_buf_copy_start..data_pos]);

            if create_chunk {
                let chunk = Chunk {
                    length: self.chunkbuf.len(),
                    hash: compute_data_hash(&self.chunkbuf[..]),
                    data: Some(std::mem::take(&mut self.chunkbuf).into()),
                };
                (self.chunk_callback)(chunk);

                self.cur_chunk_len = 0;

                self.hash.set_hash(0);
            }
        }
    }

    pub fn finish(&mut self) {
        // main loop complete
        if !self.chunkbuf.is_empty() {
            let chunk = Chunk {
                length: self.chunkbuf.len(),
                hash: compute_data_hash(&self.chunkbuf[..]),
                data: Some(std::mem::take(&mut self.chunkbuf).into()),
            };
            (self.chunk_callback)(chunk);
        }
    }
}

// Add in tests
