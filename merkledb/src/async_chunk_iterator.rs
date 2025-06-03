use std::cmp::min;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;

use async_trait::async_trait;
use lazy_static::lazy_static;
use merklehash::*;
use rand_chacha::rand_core::{RngCore, SeedableRng};
use rand_chacha::ChaChaRng;
use utils::async_iterator::AsyncIterator;

use super::constants::*;
use crate::chunk_iterator::HASH_SEED;
use crate::ChunkInfo;

type ChunkYieldType = (ChunkInfo, Vec<u8>);

/// Chunk Generator given an input stream. Do not use directly.
/// Use `async_chunk_target`.
pub struct AsyncChunker<T: AsyncIterator<E>, E: Send + Sync + 'static>
where
    T::Item: AsRef<[u8]>,
{
    iter: T,
    hash: gearhash::Hasher<'static>,
    minimum_chunk: usize,
    maximum_chunk: usize,
    mask: u64,
    // generator state
    chunkbuf: Vec<u8>,
    cur_chunk_len: usize,
    yield_queue: VecDeque<ChunkYieldType>,
    complete_after_queue: bool,
    _e: PhantomData<E>,
}

#[async_trait]
impl<T: AsyncIterator<E>, E: Send + Sync + 'static> AsyncIterator<E> for AsyncChunker<T, E>
where
    T::Item: AsRef<[u8]>,
{
    type Item = (ChunkInfo, Vec<u8>);

    /// Returns GenType::Yielded((Chunk, Vec<u8>)) when there is a chunk.
    /// call again for the next chunk.
    /// returns GenType::Complete(io::Result<()>) on completion.
    ///
    /// If any errors are encountered, Complete(Err(io::Err)) will be
    /// returned.
    ///
    /// ```ignore
    /// loop {
    ///     match generator.next().await {
    ///         GenType::Yielded((chunk, bytes)) => {
    ///             chunks.push(chunk);
    ///         }
    ///         GenType::Complete(Err(e)) => {
    ///             // error condition
    ///             break;
    ///         }
    ///         GenType::Complete(Ok(())) => {
    ///             // generator done
    ///             break;
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// Note that the std::ops::Generator trait calls this resume().
    /// We can implement the Generator trait in the future when it stabilizes.
    async fn next(&mut self) -> Result<Option<Self::Item>, E> {
        const MAX_WINDOW_SIZE: usize = 64;
        if let Some(res) = self.yield_queue.pop_front() {
            return Ok(Some(res));
        }
        while !self.complete_after_queue && self.yield_queue.is_empty() {
            match self.iter.next().await? {
                Some(readbuf) => {
                    let readbuf = readbuf.as_ref();
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
                            if self.cur_chunk_len < self.minimum_chunk - MAX_WINDOW_SIZE {
                                let max_advance = min(
                                    self.minimum_chunk - self.cur_chunk_len - MAX_WINDOW_SIZE - 1,
                                    read_bytes - cur_pos,
                                );
                                cur_pos += max_advance;
                                self.cur_chunk_len += max_advance;
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
                            if consume_len + self.cur_chunk_len >= self.maximum_chunk {
                                consume_len = self.maximum_chunk - self.cur_chunk_len;
                                create_chunk = true;
                            }
                            self.cur_chunk_len += consume_len;
                            cur_pos += consume_len;
                            self.chunkbuf.extend_from_slice(&readbuf[chunk_buf_copy_start..cur_pos]);
                            if create_chunk {
                                let res = (
                                    ChunkInfo {
                                        length: self.chunkbuf.len(),
                                        hash: compute_data_hash(&self.chunkbuf[..]),
                                    },
                                    std::mem::take(&mut self.chunkbuf),
                                );
                                self.yield_queue.push_back(res);

                                // reset chunk buffer state and continue to find the next chunk
                                self.chunkbuf.clear();
                                self.hash.set_hash(0);
                                self.cur_chunk_len = 0;
                            }
                        }
                    }
                },
                None => {
                    self.complete_after_queue = true;
                },
            }
        }
        if let Some(res) = self.yield_queue.pop_front() {
            return Ok(Some(res));
        }
        // main loop complete
        self.complete_after_queue = true;
        if !self.chunkbuf.is_empty() {
            let res = (
                ChunkInfo {
                    length: self.chunkbuf.len(),
                    hash: compute_data_hash(&self.chunkbuf[..]),
                },
                std::mem::take(&mut self.chunkbuf),
            );
            return Ok(Some(res));
        }
        Ok(None)
    }
}

// A version of chunk iter where a default hasher is used and parameters
// automatically determined given a target chunk size in bytes.
// target_chunk_size should be a power of 2, and no larger than 2^31
// Gearhash is the default since it has good perf tradeoffs
pub fn async_chunk_target<T: AsyncIterator<E>, E: Send + Sync + 'static>(
    iter: T,
    target_chunk_size: usize,
) -> AsyncChunker<T, E>
where
    T::Item: AsRef<[u8]>,
{
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
    AsyncChunker {
        iter,
        hash,
        minimum_chunk,
        maximum_chunk,
        mask,
        // generator state init
        chunkbuf: Vec::with_capacity(maximum_chunk),
        cur_chunk_len: 0,
        yield_queue: VecDeque::new(),
        complete_after_queue: false,
        _e: Default::default(),
    }
}

struct HasherPointerBox<'a>(*mut gearhash::Hasher<'a>);

unsafe impl Send for HasherPointerBox<'_> {}
unsafe impl Sync for HasherPointerBox<'_> {}

/// low Variance Chunk Generator given an input stream. Do not use directly.
/// Use `async_chunk_target_default` or `async_low_variance_chunk_target`.
pub struct AsyncLowVarianceChunker<T: AsyncIterator<E>, E: Send + Sync + 'static>
where
    T::Item: AsRef<[u8]>,
{
    iter: T,
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
    yield_queue: VecDeque<ChunkYieldType>,
    complete_after_queue: bool,
    _e: PhantomData<E>,
}

#[async_trait]
impl<T: AsyncIterator<E>, E: Send + Sync + 'static> AsyncIterator<E> for AsyncLowVarianceChunker<T, E>
where
    T::Item: AsRef<[u8]>,
{
    type Item = ChunkYieldType;

    /// Returns GenType::Yielded((Chunk, Vec<u8>)) when there is a chunk.
    /// call again for the next chunk.
    /// returns GenType::Complete(io::Result<()>) on completion.
    ///
    /// If any errors are encountered, Complete(Err(io::Err)) will be
    /// returned.
    ///
    /// ```ignore
    /// loop {
    ///     match generator.next().await {
    ///         GenType::Yielded((chunk, bytes)) => {
    ///             chunks.push(chunk);
    ///         }
    ///         GenType::Complete(Err(e)) => {
    ///             // error condition
    ///             break;
    ///         }
    ///         GenType::Complete(Ok(())) => {
    ///             // generator done
    ///             break;
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// Note that the std::ops::Generator trait calls this resume().
    /// We can implement the Generator trait in the future when it stabilizes.
    async fn next(&mut self) -> Result<Option<Self::Item>, E> {
        const MAX_WINDOW_SIZE: usize = 64;

        if let Some(res) = self.yield_queue.pop_front() {
            return Ok(Some(res));
        }
        while !self.complete_after_queue && self.yield_queue.is_empty() {
            match self.iter.next().await? {
                Some(readbuf) => {
                    let readbuf: &[u8] = readbuf.as_ref();
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
                            if self.cur_chunk_len < self.minimum_chunk - MAX_WINDOW_SIZE {
                                let max_advance = min(
                                    self.minimum_chunk - self.cur_chunk_len - MAX_WINDOW_SIZE - 1,
                                    read_bytes - cur_pos,
                                );
                                cur_pos += max_advance;
                                self.cur_chunk_len += max_advance;
                            }
                            let mut consume_len;
                            let mut create_chunk = false;
                            // find a chunk boundary after minimum chunk

                            // If we have a lot of data, don't read all the way to the end when we'll stop reading
                            // at the maximum chunk boundary.
                            let read_end = read_bytes.min(cur_pos + self.maximum_chunk - self.cur_chunk_len);

                            if let Some(boundary) =
                                unsafe { (*self.cur_hasher.0).next_match(&readbuf[cur_pos..read_end], self.mask) }
                            {
                                consume_len = boundary;
                                create_chunk = true;
                            } else {
                                consume_len = read_end - cur_pos;
                            }

                            // if we hit maximum chunk we must create a chunk
                            if consume_len + self.cur_chunk_len >= self.maximum_chunk {
                                consume_len = self.maximum_chunk - self.cur_chunk_len;
                                create_chunk = true;
                            }
                            self.cur_chunk_len += consume_len;
                            cur_pos += consume_len;
                            self.chunkbuf.extend_from_slice(&readbuf[chunk_buf_copy_start..cur_pos]);
                            if create_chunk {
                                // advance the current hash index.
                                // we actually create a chunk when we run out of hashers
                                unsafe { (*self.cur_hasher.0).set_hash(0) };
                                self.cur_hash_index += 1;
                                unsafe {
                                    self.cur_hasher = HasherPointerBox(self.hash.as_mut_ptr().add(self.cur_hash_index));
                                }
                                if self.cur_hash_index >= self.hash.len() {
                                    let res = (
                                        ChunkInfo {
                                            length: self.chunkbuf.len(),
                                            hash: compute_data_hash(&self.chunkbuf[..]),
                                        },
                                        std::mem::take(&mut self.chunkbuf),
                                    );
                                    // reset chunk buffer state and continue to find the next chunk
                                    self.yield_queue.push_back(res);

                                    self.chunkbuf.clear();
                                    self.cur_hash_index = 0;
                                    self.cur_hasher = HasherPointerBox(self.hash.as_mut_ptr());
                                }
                                self.cur_chunk_len = 0;
                            }
                        }
                    }
                },
                None => {
                    self.complete_after_queue = true;
                },
            }
        }
        if let Some(res) = self.yield_queue.pop_front() {
            return Ok(Some(res));
        }
        // main loop complete
        if !self.chunkbuf.is_empty() {
            let res = (
                ChunkInfo {
                    length: self.chunkbuf.len(),
                    hash: compute_data_hash(&self.chunkbuf[..]),
                },
                std::mem::take(&mut self.chunkbuf),
            );
            return Ok(Some(res));
        }
        Ok(None)
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

// Annoying that we have to explicitly declare this, but that is the cost of using async_trait
#[async_trait]
impl<E: Send + Sync + 'static, T: AsyncIterator<E>> AsyncIterator<E> for Pin<Box<AsyncLowVarianceChunker<T, E>>>
where
    T::Item: AsRef<[u8]>,
{
    type Item = ChunkYieldType;

    async fn next(&mut self) -> Result<Option<Self::Item>, E> {
        unsafe {
            let mut_ref: Pin<&mut _> = Pin::as_mut(self);
            let mut_ref = Pin::get_unchecked_mut(mut_ref);
            mut_ref.next().await
        }
    }
}

/// A version of low_variance_chunk_iter where a default hasher is used and parameters
/// automatically determined given a target chunk size in bytes.
/// target_chunk_size should be a power of 2, and no larger than 2^31
/// num_hashers must be a power of 2 and smaller than target_chunk_size.
/// Gearhash is the default since it has good perf tradeoffs.
///
/// num_hashers cannot be larger than N_LOW_VARIANCE_CDC_CHUNKERS
///
/// Returns a Generator. See `AsyncLowVarianceChunker`
#[allow(clippy::needless_lifetimes, dead_code)]
pub fn async_low_variance_chunk_target<T: AsyncIterator<E> + 'static, E: Send + Sync + 'static>(
    iter: T,
    target_chunk_size: usize,
    num_hashers: usize,
) -> Pin<Box<AsyncLowVarianceChunker<T, E>>>
where
    T::Item: AsRef<[u8]>,
{
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
    let mut res = Box::pin(AsyncLowVarianceChunker {
        iter,
        hash: hashers,
        minimum_chunk: minimum_chunk / num_hashes,
        maximum_chunk: maximum_chunk / num_hashes,
        mask,
        // generator state init
        chunkbuf: Vec::with_capacity(maximum_chunk),
        cur_chunk_len: 0,
        cur_hasher: HasherPointerBox(std::ptr::null_mut()),
        cur_hash_index: 0,
        yield_queue: VecDeque::new(),
        complete_after_queue: false,
        _e: Default::default(),
    });
    // initialize cur_hasher
    unsafe {
        let mut_ref: Pin<&mut _> = Pin::as_mut(&mut res);
        let mut_ref = Pin::get_unchecked_mut(mut_ref);
        mut_ref.cur_hasher = HasherPointerBox(mut_ref.hash.as_mut_ptr());
    }

    res
}

/// Chunks an input stream with the default low variance configuration.
/// Returns a Generator. See `AsyncLowVarianceChunker`
pub fn async_chunk_target_default<T: AsyncIterator<E> + 'static, E: Send + Sync + 'static>(
    iter: T,
) -> Pin<Box<AsyncLowVarianceChunker<T, E>>>
where
    T::Item: AsRef<[u8]>,
{
    async_low_variance_chunk_target(iter, TARGET_CDC_CHUNK_SIZE, N_LOW_VARIANCE_CDC_CHUNKERS)
}
