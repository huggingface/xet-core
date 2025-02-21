use std::cmp::min;
use std::sync::Arc;

use merkledb::constants::{MAXIMUM_CHUNK_MULTIPLIER, MINIMUM_CHUNK_DIVISOR, TARGET_CDC_CHUNK_SIZE};
use merkledb::Chunk;
use merklehash::compute_data_hash;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use xet_threadpool::ThreadPool;

use super::file_cleaner::BufferItem;
use crate::errors::{DataProcessingError, Result};

pub type ChunkYieldType = (Chunk, Vec<u8>);

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
    data_queue: Receiver<BufferItem<Vec<u8>>>,
    yield_queue: Sender<Option<ChunkYieldType>>,
}

impl Chunker {
    pub fn run(chunker: Mutex<Self>, threadpool: Arc<ThreadPool>) -> JoinHandle<Result<()>> {
        const MAX_WINDOW_SIZE: usize = 64;

        threadpool.spawn(async move {
            let mut chunker = chunker.lock().await;
            let mask = chunker.mask;

            loop {
                match chunker.data_queue.recv().await {
                    Some(BufferItem::Value(readbuf)) => {
                        let read_bytes = readbuf.len();
                        // 0 byte read is assumed EOF
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
                                    let res = (
                                        Chunk {
                                            length: chunker.chunkbuf.len(),
                                            hash: compute_data_hash(&chunker.chunkbuf[..]),
                                        },
                                        std::mem::take(&mut chunker.chunkbuf),
                                    );
                                    // reset chunk buffer state and continue to find the next chunk
                                    chunker
                                        .yield_queue
                                        .send(Some(res))
                                        .await
                                        .map_err(|e| DataProcessingError::InternalError(format!("Send Error: {e}")))?;

                                    chunker.chunkbuf.clear();
                                    chunker.cur_chunk_len = 0;

                                    chunker.hash.set_hash(0);
                                }
                            }
                        }
                    },
                    Some(BufferItem::Completed) => {
                        break;
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
                chunker
                    .yield_queue
                    .send(Some(res))
                    .await
                    .map_err(|e| DataProcessingError::InternalError(format!("Send Error: {e}")))?;
            }

            // signal finish
            chunker
                .yield_queue
                .send(None)
                .await
                .map_err(|e| DataProcessingError::InternalError(format!("Send Error: {e}")))?;

            Ok(())
        })
    }
}

// A version of chunker where a default hasher is used and parameters
// automatically determined given a target chunk size in bytes.
// target_chunk_size should be a power of 2, and no larger than 2^31
// Gearhash is the default since it has good perf tradeoffs
pub fn gearhash_chunk_target(
    target_chunk_size: usize,
    data: Receiver<BufferItem<Vec<u8>>>,
    yield_queue: Sender<Option<ChunkYieldType>>,
) -> Chunker {
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
        data_queue: data,
        yield_queue,
    }
}

pub fn chunk_target_default(
    data: Receiver<BufferItem<Vec<u8>>>,
    yield_queue: Sender<Option<ChunkYieldType>>,
    threadpool: Arc<ThreadPool>,
) -> JoinHandle<Result<()>> {
    let chunker = gearhash_chunk_target(TARGET_CDC_CHUNK_SIZE, data, yield_queue);

    Chunker::run(Mutex::new(chunker), threadpool)
}
