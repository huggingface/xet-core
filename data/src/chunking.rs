use std::cmp::min;
use std::pin::Pin;

use lazy_static::lazy_static;
use merkledb::constants::{
    MAXIMUM_CHUNK_MULTIPLIER, MINIMUM_CHUNK_DIVISOR, N_LOW_VARIANCE_CDC_CHUNKERS, TARGET_CDC_CHUNK_SIZE,
};
use merkledb::Chunk;
use merklehash::compute_data_hash;
use rand_chacha::rand_core::{RngCore, SeedableRng};
use rand_chacha::ChaChaRng;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use super::clean::BufferItem;

pub const HASH_SEED: u64 = 123456;

struct HasherPointerBox<'a>(*mut gearhash::Hasher<'a>);

unsafe impl<'a> Send for HasherPointerBox<'a> {}
unsafe impl<'a> Sync for HasherPointerBox<'a> {}

pub type ChunkYieldType = (Chunk, Vec<u8>);

pub struct LowVarianceChunker {
    hash: Vec<gearhash::Hasher<'static>>,
    minimum_chunk: usize,
    maximum_chunk: usize,
    mask: u64,
    // generator state
    chunkbuf: Vec<u8>,
    cur_chunk_len: usize,
    // This hasher is referenced *a lot* and there was quite a
    // measurable performance gain by making this a raw pointer.
    //
    // The key problem is that I need a mutable mutable reference to the
    // current hasher which is basically an index into hash.
    // (Basically cur_hasher = &mut hash[cur_hash_index])
    //
    // But because of rust borrow checker rules, this cannot be done
    // easily. We can of course just use hash[cur_hash_index] all the time
    // but this is in fact a core inner loop and ends up as a perf bottleneck.
    cur_hasher: HasherPointerBox<'static>,
    cur_hash_index: usize,
    data_queue: Receiver<BufferItem<Vec<u8>>>,
    yield_queue: Sender<Option<ChunkYieldType>>,
}

impl LowVarianceChunker {
    pub fn run(chunker: Mutex<Pin<Box<Self>>>) -> JoinHandle<()> {
        const MAX_WINDOW_SIZE: usize = 64;

        tokio::spawn(async move {
            let mut chunker = chunker.lock().await;
            let mut complete = false;
            while !complete {
                match chunker.data_queue.recv().await {
                    Some(BufferItem::Value(readbuf)) => {
                        let read_bytes = readbuf.len();
                        if read_bytes > 0 {
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
                                    let max_advance = min(
                                        chunker.minimum_chunk - chunker.cur_chunk_len - MAX_WINDOW_SIZE - 1,
                                        read_bytes - cur_pos,
                                    );
                                    cur_pos += max_advance;
                                    chunker.cur_chunk_len += max_advance;
                                }
                                let mut consume_len;
                                let mut create_chunk = false;
                                // find a chunk boundary after minimum chunk

                                // If we have a lot of data, don't read all the way to the end when we'll stop reading
                                // at the maximum chunk boundary.
                                let read_end = read_bytes.min(cur_pos + chunker.maximum_chunk - chunker.cur_chunk_len);

                                if let Some(boundary) = unsafe {
                                    (*chunker.cur_hasher.0).next_match(&readbuf[cur_pos..read_end], chunker.mask)
                                } {
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
                                    // advance the current hash index.
                                    // we actually create a chunk when we run out of hashers
                                    unsafe { (*chunker.cur_hasher.0).set_hash(0) };
                                    chunker.cur_hash_index += 1;
                                    unsafe {
                                        chunker.cur_hasher =
                                            HasherPointerBox(chunker.hash.as_mut_ptr().add(chunker.cur_hash_index));
                                    }
                                    if chunker.cur_hash_index >= chunker.hash.len() {
                                        let res = (
                                            Chunk {
                                                length: chunker.chunkbuf.len(),
                                                hash: compute_data_hash(&chunker.chunkbuf[..]),
                                            },
                                            std::mem::take(&mut chunker.chunkbuf),
                                        );
                                        // reset chunk buffer state and continue to find the next chunk
                                        chunker.yield_queue.send(Some(res)).await.expect("Send chunk to channel error");

                                        chunker.chunkbuf.clear();
                                        chunker.cur_hash_index = 0;
                                        chunker.cur_hasher = HasherPointerBox(chunker.hash.as_mut_ptr());
                                    }
                                    chunker.cur_chunk_len = 0;
                                }
                            }
                        }
                    },
                    Some(BufferItem::Completed) => {
                        complete = true;
                    },
                    None => (),
                }
            }

            // main loop complete
            if !chunker.chunkbuf.is_empty() {
                let res = (
                    Chunk {
                        length: chunker.chunkbuf.len(),
                        hash: compute_data_hash(&chunker.chunkbuf[..]),
                    },
                    std::mem::take(&mut chunker.chunkbuf),
                );
                chunker.yield_queue.send(Some(res)).await.expect("Send chunk to channel error");
            }

            // signal finish
            chunker.yield_queue.send(None).await.expect("Send chunk to channel error");
        })
    }
}

lazy_static! {
    /// The static gearhash seed table.
    static ref HASHER_SEED_TABLE: Vec<[u64; 256]> = {
        let mut tables: Vec<[u64; 256]> = Vec::new();
        for i in 0..N_LOW_VARIANCE_CDC_CHUNKERS {
            let mut rng = ChaChaRng::seed_from_u64(HASH_SEED + i as u64);
            let mut bytehash: [u64; 256] = [0; 256];
            #[allow(clippy::needless_range_loop)]
            for i in 0..256 {
                bytehash[i] = rng.next_u64();
            }
            tables.push(bytehash);
        }
        tables
    };
}

fn low_variance_chunk_target(
    target_chunk_size: usize,
    num_hashers: usize,
    data: Receiver<BufferItem<Vec<u8>>>,
    yield_queue: Sender<Option<ChunkYieldType>>,
) -> Pin<Box<LowVarianceChunker>> {
    // We require the type to be Pinned since we do have a n
    // internal pointer. (cur_hasher).

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
    assert!(num_hashers <= HASHER_SEED_TABLE.len());
    for t in HASHER_SEED_TABLE.chunks(1) {
        hashers.push(gearhash::Hasher::new(&t[0]));
        if hashers.len() == num_hashers {
            break;
        }
    }

    assert!(maximum_chunk > minimum_chunk);
    assert!(!hashers.is_empty());
    let num_hashes = hashers.len();
    let mut res = Box::pin(LowVarianceChunker {
        hash: hashers,
        minimum_chunk: minimum_chunk / num_hashes,
        maximum_chunk: maximum_chunk / num_hashes,
        mask,
        // generator state init
        chunkbuf: Vec::with_capacity(maximum_chunk),
        cur_chunk_len: 0,
        cur_hasher: HasherPointerBox(std::ptr::null_mut()),
        cur_hash_index: 0,
        data_queue: data,
        yield_queue,
    });
    // initialize cur_hasher
    unsafe {
        let mut_ref: Pin<&mut _> = Pin::as_mut(&mut res);
        let mut_ref = Pin::get_unchecked_mut(mut_ref);
        mut_ref.cur_hasher = HasherPointerBox(mut_ref.hash.as_mut_ptr());
    }

    res
}

pub fn chunk_target_default(
    data: Receiver<BufferItem<Vec<u8>>>,
    yield_queue: Sender<Option<ChunkYieldType>>,
) -> JoinHandle<()> {
    let chunker = low_variance_chunk_target(TARGET_CDC_CHUNK_SIZE, N_LOW_VARIANCE_CDC_CHUNKERS, data, yield_queue);

    LowVarianceChunker::run(Mutex::new(chunker))
}
