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
}

impl Default for Chunker {
    fn default() -> Self {
        Self::new(TARGET_CDC_CHUNK_SIZE)
    }
}

impl Chunker {
    pub fn new(target_chunk_size: usize) -> Self {
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
        }
    }

    /// Process more data; this is a continuation of any data from before when calls were
    ///
    /// Returns the next chunk, if available, and the amount of data that was digested.
    ///
    /// If is_final is true, then it is assumed that no more data after this block will come,
    /// and any data currently present and at the end will be put into a final chunk.
    pub fn next(&mut self, data: &[u8], is_final: bool) -> (Option<Chunk>, usize) {
        const MAX_WINDOW_SIZE: usize = 64;
        let n_bytes = data.len();

        let mut create_chunk = false;
        let mut consume_len = 0;

        // find a chunk boundary after minimum chunk
        if n_bytes != 0 {
            // skip the minimum chunk size
            // and noting that the hash has a window size of 64
            // so we should be careful to skip only minimum_chunk - 64 - 1
            if self.cur_chunk_len < self.minimum_chunk - MAX_WINDOW_SIZE {
                let max_advance =
                    min(self.minimum_chunk - self.cur_chunk_len - MAX_WINDOW_SIZE - 1, n_bytes - consume_len);
                consume_len += max_advance;
                self.cur_chunk_len += max_advance;
            }

            // If we have a lot of data, don't read all the way to the end when we'll stop reading
            // at the maximum chunk boundary.
            let read_end = n_bytes.min(consume_len + self.maximum_chunk - self.cur_chunk_len);

            let mut bytes_to_next_boundary;
            if let Some(boundary) = self.hash.next_match(&data[consume_len..read_end], self.mask) {
                bytes_to_next_boundary = boundary;
                create_chunk = true;
            } else {
                bytes_to_next_boundary = read_end - consume_len;
            }

            // if we hit maximum chunk we must create a chunk
            if bytes_to_next_boundary + self.cur_chunk_len >= self.maximum_chunk {
                bytes_to_next_boundary = self.maximum_chunk - self.cur_chunk_len;
                create_chunk = true;
            }
            self.cur_chunk_len += bytes_to_next_boundary;
            consume_len += bytes_to_next_boundary;
            self.chunkbuf.extend_from_slice(&data[0..consume_len]);
        }

        let ret;
        if create_chunk || (is_final && !self.chunkbuf.is_empty()) {
            let chunk = Chunk {
                length: self.chunkbuf.len(),
                hash: compute_data_hash(&self.chunkbuf[..]),
                data: Some(std::mem::take(&mut self.chunkbuf).into()),
            };

            self.cur_chunk_len = 0;

            self.hash.set_hash(0);

            ret = (Some(chunk), consume_len)
        } else {
            ret = (None, consume_len)
        }

        // The amount of data consumed should never be more than the amount of data given.
        #[cfg(debug_assertions)]
        {
            debug_assert!(ret.1 <= data.len());

            // If no chunk is returned, then make sure all the data is consumed.
            if ret.0.is_none() {
                debug_assert_eq!(ret.1, data.len());
            }
        }

        ret
    }

    /// Processes several blocks at once, returning
    pub fn next_block(&mut self, data: &[u8], is_final: bool) -> Vec<Chunk> {
        let mut ret = Vec::new();

        let mut pos = 0;
        loop {
            debug_assert!(pos <= data.len());
            if pos == data.len() {
                return ret;
            }

            let (maybe_chunk, bytes_consumed) = self.next(&data[pos..], is_final);

            if let Some(chunk) = maybe_chunk {
                ret.push(chunk);
            }

            pos += bytes_consumed;
        }
    }

    // Simply returns the
    pub fn finish(&mut self) -> Option<Chunk> {
        self.next(&[], true).0
    }
}
