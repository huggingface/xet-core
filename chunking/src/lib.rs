use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChunkingError {
    #[error("Internal Error {0}")]
    Internal(String),
}

const MAX_WINDOW_SIZE: usize = 64;

/// Describes a chunk within the provided data buffer
/// hash being the hash of the chunk and the boundaries of the chunk within the buffer referred
/// to by the offset i.e. the index of the first byte of the chunk and the length of the chunk
#[cfg(target_family = "wasm")]
pub mod wasm {
    use crate::{Chunker, ChunkingError, MAX_WINDOW_SIZE};
    use constants::merkledb::{MAXIMUM_CHUNK_MULTIPLIER, MINIMUM_CHUNK_DIVISOR, TARGET_CDC_CHUNK_SIZE};
    use merklehash::{compute_data_hash, MerkleHash};
    use serde::{Deserialize, Serialize};
    use std::cmp::min;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Chunk {
        #[serde(with = "merklehash::data_hash::hex::serde")]
        hash: MerkleHash,
        length: u32,
        offset: u64,
    }

    pub async fn chunk(data: Vec<u8>) -> Result<Vec<Chunk>, ChunkingError> {
        let mut res: Vec<Chunk> = Vec::new();
        let target_chunk_size = TARGET_CDC_CHUNK_SIZE;

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

        let mut chunker = Chunker {
            hash,
            minimum_chunk,
            maximum_chunk,
            mask,
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
    #[cfg(not(target_family = "wasm"))]
    data_queue: tokio::sync::mpsc::Receiver<BufferItem<Vec<u8>>>,
    #[cfg(not(target_family = "wasm"))]
    yield_queue: tokio::sync::mpsc::Sender<Option<ChunkYieldType>>,
}

#[cfg(not(target_family = "wasm"))]
pub use standard::*;

#[cfg(not(target_family = "wasm"))]
mod standard {
    use crate::{Chunker, ChunkingError};
    use constants::merkledb::{MAXIMUM_CHUNK_MULTIPLIER, MINIMUM_CHUNK_DIVISOR, TARGET_CDC_CHUNK_SIZE};
    use merkledb::Chunk;
    use merklehash::compute_data_hash;
    use std::cmp::min;
    use std::sync::Arc;
    use tokio::{
        sync::mpsc::{Receiver, Sender},
        sync::Mutex,
        task::JoinHandle,
    };
    use xet_threadpool::ThreadPool;

    pub enum BufferItem<T: Send + Sync + 'static> {
        Value(T),
        Completed,
    }

    pub type ChunkYieldType = (Chunk, Vec<u8>);

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
    ) -> JoinHandle<Result<(), ChunkingError>> {
        let chunker = gearhash_chunk_target(TARGET_CDC_CHUNK_SIZE, data, yield_queue);

        Chunker::run(Mutex::new(chunker), threadpool)
    }

    impl Chunker {
        pub fn run(chunker: Mutex<Self>, threadpool: Arc<ThreadPool>) -> JoinHandle<Result<(), ChunkingError>> {
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
                                    let read_end =
                                        read_bytes.min(cur_pos + chunker.maximum_chunk - chunker.cur_chunk_len);

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
                                            .map_err(|e| ChunkingError::Internal(format!("Send Error: {e}")))?;

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
                        .map_err(|e| ChunkingError::Internal(format!("Send Error: {e}")))?;
                }

                // signal finish
                chunker
                    .yield_queue
                    .send(None)
                    .await
                    .map_err(|e| ChunkingError::Internal(format!("Send Error: {e}")))?;

                Ok(())
            })
        }
    }
}
