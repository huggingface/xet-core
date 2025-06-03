use std::cmp::min;
use std::io;
use std::io::Read;

use merklehash::*;
use rand_chacha::rand_core::{RngCore, SeedableRng};
use rand_chacha::ChaChaRng;

use super::constants::*;

#[derive(Debug, Clone)]
pub struct ChunkInfo {
    pub hash: DataHash,
    pub length: usize,
}

pub const READ_BUF_SIZE: usize = 65536;
pub const HASH_SEED: u64 = 123456;

pub struct Chunker<'a, T: Read> {
    iter: &'a mut T,
    hash: gearhash::Hasher<'a>,
    minimum_chunk: usize,
    maximum_chunk: usize,
    mask: u64,
}

fn fill_buf(reader: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    reader.read(buf)
}

impl<T: Read> Chunker<'_, T> {
    fn gen(&mut self) -> Vec<ChunkInfo> {
        let mut ret: Vec<ChunkInfo> = Vec::with_capacity(1024);
        let mut chunkbuf: Vec<u8> = Vec::with_capacity(self.maximum_chunk);
        let mut cur_chunk_len: usize = 0;
        let mut readbuf: [u8; READ_BUF_SIZE] = [0; READ_BUF_SIZE];
        const MAX_WINDOW_SIZE: usize = 64;
        while let Ok(read_bytes) = fill_buf(&mut self.iter, &mut readbuf) {
            if read_bytes == 0 {
                break;
            }
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
                if cur_chunk_len < self.minimum_chunk - MAX_WINDOW_SIZE {
                    let max_advance =
                        min(self.minimum_chunk - cur_chunk_len - MAX_WINDOW_SIZE - 1, read_bytes - cur_pos);
                    cur_pos += max_advance;
                    cur_chunk_len += max_advance;
                }
                let mut consume_len;
                let mut create_chunk = false;
                // find a chunk boundary after minimum chunk
                if let Some(boundary) = self.hash.next_match(&readbuf[cur_pos..read_bytes], self.mask) {
                    consume_len = boundary;
                    create_chunk = true;
                } else {
                    consume_len = read_bytes - cur_pos;
                }

                // if we hit maximum chunk we must create a chunk
                if consume_len + cur_chunk_len >= self.maximum_chunk {
                    consume_len = self.maximum_chunk - cur_chunk_len;
                    create_chunk = true;
                }
                cur_chunk_len += consume_len;
                cur_pos += consume_len;
                chunkbuf.extend_from_slice(&readbuf[chunk_buf_copy_start..cur_pos]);
                if create_chunk {
                    ret.push(ChunkInfo {
                        length: chunkbuf.len(),
                        hash: compute_data_hash(&chunkbuf[..]),
                    });

                    // reset chunk buffer state and continue to find the next chunk
                    chunkbuf.clear();
                    self.hash.set_hash(0);
                    cur_chunk_len = 0;
                }
            }
        }
        if !chunkbuf.is_empty() {
            ret.push(ChunkInfo {
                length: chunkbuf.len(),
                hash: compute_data_hash(&chunkbuf[..]),
            });
        }
        ret
    }
}

// A version of chunk iter where a default hasher is used and parameters
// automatically determined given a target chunk size in bytes.
// target_chunk_size should be a power of 2, and no larger than 2^31
// Gearhash is the default since it has good perf tradeoffs
pub fn chunk_target<T: Read>(iter: &mut T, target_chunk_size: usize) -> Vec<ChunkInfo> {
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
        iter,
        hash,
        minimum_chunk,
        maximum_chunk,
        mask,
    }
    .gen()
}

pub struct LowVarianceChunker<'a, T: Read> {
    iter: &'a mut T,
    hash: Vec<gearhash::Hasher<'a>>,
    minimum_chunk: usize,
    maximum_chunk: usize,
    mask: u64,
}

impl<T: Read> LowVarianceChunker<'_, T> {
    fn gen(&mut self) -> Vec<ChunkInfo> {
        let mut ret: Vec<ChunkInfo> = Vec::with_capacity(1024);
        let mut chunkbuf: Vec<u8> = Vec::with_capacity(self.maximum_chunk);
        let mut cur_chunk_len: usize = 0;
        let mut readbuf: [u8; READ_BUF_SIZE] = [0; READ_BUF_SIZE];
        const MAX_WINDOW_SIZE: usize = 64;
        let mut cur_hasher = self.hash.as_mut_ptr();
        let mut cur_hash_index: usize = 0;
        while let Ok(read_bytes) = fill_buf(&mut self.iter, &mut readbuf) {
            if read_bytes == 0 {
                break;
            }
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
                if cur_chunk_len < self.minimum_chunk - MAX_WINDOW_SIZE {
                    let max_advance =
                        min(self.minimum_chunk - cur_chunk_len - MAX_WINDOW_SIZE - 1, read_bytes - cur_pos);
                    cur_pos += max_advance;
                    cur_chunk_len += max_advance;
                }
                let mut consume_len;
                let mut create_chunk = false;
                // find a chunk boundary after minimum chunk
                if let Some(boundary) = unsafe { (*cur_hasher).next_match(&readbuf[cur_pos..read_bytes], self.mask) } {
                    consume_len = boundary;
                    create_chunk = true;
                } else {
                    consume_len = read_bytes - cur_pos;
                }

                // if we hit maximum chunk we must create a chunk
                if consume_len + cur_chunk_len >= self.maximum_chunk {
                    consume_len = self.maximum_chunk - cur_chunk_len;
                    create_chunk = true;
                }
                cur_chunk_len += consume_len;
                cur_pos += consume_len;
                chunkbuf.extend_from_slice(&readbuf[chunk_buf_copy_start..cur_pos]);
                if create_chunk {
                    // advance the current hash index.
                    // we actually create a chunk when we run out of hashers
                    unsafe { (*cur_hasher).set_hash(0) };
                    cur_hash_index += 1;
                    unsafe {
                        cur_hasher = self.hash.as_mut_ptr().add(cur_hash_index);
                    }
                    if cur_hash_index >= self.hash.len() {
                        ret.push(ChunkInfo {
                            length: chunkbuf.len(),
                            hash: compute_data_hash(&chunkbuf[..]),
                        });

                        // reset chunk buffer state and continue to find the next chunk
                        chunkbuf.clear();
                        cur_hash_index = 0;
                        cur_hasher = self.hash.as_mut_ptr();
                    }
                    cur_chunk_len = 0;
                }
            }
        }
        if !chunkbuf.is_empty() {
            ret.push(ChunkInfo {
                hash: compute_data_hash(&chunkbuf[..]),
                length: chunkbuf.len(),
            });
        }
        ret
    }
}

// A version of low_variance_chunk_iter where a default hasher is used and parameters
// automatically determined given a target chunk size in bytes.
// target_chunk_size should be a power of 2, and no larger than 2^31
// num_hashers must be a power of 2 and smaller than target_chunk_size.
// Gearhash is the default since it has good perf tradeoffs
#[allow(clippy::needless_lifetimes)]
pub fn low_variance_chunk_target<'a, T: Read>(
    iter: &'a mut T,
    target_chunk_size: usize,
    num_hashers: usize,
) -> Vec<ChunkInfo> {
    assert_eq!(target_chunk_size.count_ones(), 1);
    assert_eq!(num_hashers.count_ones(), 1);
    assert!(target_chunk_size > 1);
    assert!(num_hashers < target_chunk_size);
    // note the strict lesser than. Combined with count_ones() == 1,
    // this limits to 2^31
    assert!(target_chunk_size < u32::MAX as usize);

    let target_per_hash_chunk_size = target_chunk_size / num_hashers;

    let mask = (target_per_hash_chunk_size - 1) as u64;
    // we will like to shift the mask left by a bunch since the right
    // bits of the gear hash are affected by only a small number of bytes
    // really. we just shift it all the way left.
    let mask = mask << mask.leading_zeros();
    let minimum_chunk = target_chunk_size / MINIMUM_CHUNK_DIVISOR;
    let maximum_chunk = target_chunk_size * MAXIMUM_CHUNK_MULTIPLIER;

    let mut hashers: Vec<gearhash::Hasher> = Vec::new();
    let mut tables: Vec<[u64; 256]> = Vec::new();

    for i in 0..num_hashers {
        let mut rng = ChaChaRng::seed_from_u64(HASH_SEED + i as u64);
        let mut bytehash: [u64; 256] = [0; 256];
        #[allow(clippy::needless_range_loop)]
        for i in 0..256 {
            bytehash[i] = rng.next_u64();
        }
        tables.push(bytehash);
    }
    for t in tables.chunks(1) {
        hashers.push(gearhash::Hasher::new(&t[0]));
    }

    assert!(maximum_chunk > minimum_chunk);
    assert!(!hashers.is_empty());
    let num_hashes = hashers.len();
    let mut chunker = LowVarianceChunker {
        iter,
        hash: hashers,
        minimum_chunk: minimum_chunk / num_hashes,
        maximum_chunk: maximum_chunk / num_hashes,
        mask,
    };

    chunker.gen()
}

pub fn chunk_target_default<T: Read>(iter: &mut T) -> Vec<ChunkInfo> {
    low_variance_chunk_target(iter, TARGET_CDC_CHUNK_SIZE, N_LOW_VARIANCE_CDC_CHUNKERS)
}
