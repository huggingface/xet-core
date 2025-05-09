use std::cmp::min;
use std::sync::Arc;

use merklehash::{compute_data_hash, MerkleHash};

use crate::constants::{MAXIMUM_CHUNK_MULTIPLIER, MINIMUM_CHUNK_DIVISOR, TARGET_CHUNK_SIZE};

#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    pub hash: MerkleHash,
    pub data: Arc<[u8]>,
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
        Self::new(*TARGET_CHUNK_SIZE)
    }
}

impl Chunker {
    pub fn new(target_chunk_size: usize) -> Self {
        assert_eq!(target_chunk_size.count_ones(), 1);

        // Some of the logic only works if the target_chunk_size is greater than the
        // window size of the hash.
        assert!(target_chunk_size > 64);

        // note the strict lesser than. Combined with count_ones() == 1,
        // this limits to 2^31
        assert!(target_chunk_size < u32::MAX as usize);

        let mask = (target_chunk_size - 1) as u64;

        // we will like to shift the mask left by a bunch since the right
        // bits of the gear hash are affected by only a small number of bytes
        // really. we just shift it all the way left.
        let mask = mask << mask.leading_zeros();
        let minimum_chunk = target_chunk_size / *MINIMUM_CHUNK_DIVISOR;
        let maximum_chunk = target_chunk_size * *MAXIMUM_CHUNK_MULTIPLIER;

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
        const HASH_WINDOW_SIZE: usize = 64;
        let n_bytes = data.len();

        let mut create_chunk = false;
        let mut consume_len = 0;

        // find a chunk boundary after minimum chunk
        if n_bytes != 0 {
            // skip the minimum chunk size
            // and noting that the hash has a window size of 64
            // so we should be careful to skip only minimum_chunk - 64 - 1
            if self.cur_chunk_len + HASH_WINDOW_SIZE < self.minimum_chunk {
                let max_advance =
                    min(self.minimum_chunk - self.cur_chunk_len - HASH_WINDOW_SIZE - 1, n_bytes - consume_len);
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

        let ret = {
            if create_chunk || (is_final && !self.chunkbuf.is_empty()) {
                let chunk = Chunk {
                    hash: compute_data_hash(&self.chunkbuf[..]),
                    data: std::mem::take(&mut self.chunkbuf).into(),
                };

                self.cur_chunk_len = 0;

                self.hash.set_hash(0);

                (Some(chunk), consume_len)
            } else {
                (None, consume_len)
            }
        };

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

    // Finishes, returning the final chunk if it exists
    pub fn finish(&mut self) -> Option<Chunk> {
        self.next(&[], true).0
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    /// A helper to create random test data using a specified `seed` and `len`.
    /// Using a fixed seed ensures tests are reproducible.
    fn make_test_data(seed: u64, len: usize) -> Vec<u8> {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut data = vec![0; len];
        rng.fill(&mut data[..]);
        data
    }

    fn check_chunks_equal(chunks: &[Chunk], data: &[u8]) {
        // Validate all the chunks are exact.
        let mut new_vec = Vec::with_capacity(10000);
        for c in chunks.iter() {
            new_vec.extend_from_slice(&c.data[..]);
        }

        assert!(new_vec == data);
    }

    #[test]
    fn test_empty_data_no_chunk_until_final() {
        let mut chunker = Chunker::new(128);

        // Passing empty slice without final => no chunk
        let (chunk, consumed) = chunker.next(&[], false);
        assert!(chunk.is_none());
        assert_eq!(consumed, 0);

        // Passing empty slice again with is_final = true => no leftover data, so no chunk
        let (chunk, consumed) = chunker.next(&[], true);
        assert!(chunk.is_none());
        assert_eq!(consumed, 0);
    }

    #[test]
    fn test_data_smaller_than_minimum_no_boundary() {
        let mut chunker = Chunker::new(128);

        // Create a small random data buffer. For example, length=3.
        let data = make_test_data(0, 63);

        // We expect no chunk until we finalize, because there's not enough data
        // to trigger a boundary, nor to reach the maximum chunk size.
        let (chunk, consumed) = chunker.next(&data, false);
        assert!(chunk.is_none());
        assert_eq!(consumed, data.len());

        // Now finalize: we expect a chunk with the leftover data
        let (chunk, consumed) = chunker.next(&[], true);
        assert!(chunk.is_some());
        assert_eq!(consumed, 0);

        let chunk = chunk.unwrap();
        assert_eq!(chunk.data.len(), 63);
        assert_eq!(&chunk.data[..], &data[..], "Chunk should contain exactly what was passed in");
    }

    #[test]
    fn test_multiple_chunks_produced() {
        let mut chunker = Chunker::new(128);

        // Produce 100 bytes of random data
        let data = make_test_data(42, 10000);

        // Pass everything at once, final = true
        let chunks = chunker.next_block(&data, true);
        assert!(!chunks.is_empty());

        check_chunks_equal(&chunks, &data);
    }

    #[test]
    fn test_repeated_calls_partial_consumption() {
        // We'll feed in two pieces of data to ensure partial consumption

        let data = make_test_data(42, 10000);

        let mut chunks_1 = Vec::new();

        let mut pos = 0;
        let mut chunker = Chunker::new(128);

        while pos < data.len() {
            for i in 0..16 {
                let next_pos = (pos + i).min(data.len());
                chunks_1.append(&mut chunker.next_block(&data[pos..next_pos], next_pos == data.len()));
                pos = next_pos;
            }
        }

        check_chunks_equal(&chunks_1, &data);

        // Now, rechunk with all at once and make sure it's equal.
        let chunks_2 = Chunker::new(128).next_block(&data, true);

        assert_eq!(chunks_1, chunks_2);
    }

    #[test]
    fn test_exact_maximum_chunk() {
        // If the data hits the maximum chunk size exactly, we should force a boundary.
        // For target_chunk_size = 128, if MAXIMUM_CHUNK_MULTIPLIER = 2, then max = 256.
        // Adjust if your constants differ.
        let mut chunker = Chunker::new(512);

        // Use constant data
        let data = vec![0; 8 * *MAXIMUM_CHUNK_MULTIPLIER * 512];

        let chunks = chunker.next_block(&data, true);

        assert_eq!(chunks.len(), 8);

        for c in chunks.iter() {
            assert_eq!(c.data.len(), *MAXIMUM_CHUNK_MULTIPLIER * 512);
        }
    }
}
