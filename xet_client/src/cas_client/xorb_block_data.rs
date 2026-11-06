//! Decompressed data of one xorb block, shared between the task that downloads it and the readers of its terms.
//!
//! A block is one fetch: a chunk range of a xorb (or several, for a multi-range fetch) retrieved by one HTTP request.
//! [`XorbBlockData`] holds its decompressed bytes in a single allocation sized when the block is planned. The
//! downloading client appends each chunk as soon as it is decompressed and moves a "ready" mark forward; readers can
//! take any range below that mark as a zero-copy [`Bytes`] while the rest of the block is still being downloaded.
//!
//! Chunks are appended strictly in order, so everything below the ready mark is final and never written again.

use std::ptr::NonNull;
#[cfg(not(target_family = "wasm"))]
use std::sync::Condvar;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use bytes::Bytes;
use tokio::sync::watch;
use xet_core_structures::xorb_object::{XORB_CHUNK_HEADER_LENGTH, XorbChunkHeader, decompress_chunk_into};

use crate::error::{ClientError, Result};

/// A byte range of a block, anchored at a chunk: `len` bytes starting `offset_into_first_chunk` bytes after the start
/// of chunk `first_chunk` (an index into the block's chunks, in fetch order).
///
/// Anchoring at a chunk lets a reader be created before the block's chunk sizes are known; its absolute position
/// becomes known once all chunks before `first_chunk` have been appended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockByteRange {
    pub first_chunk: usize,
    pub offset_into_first_chunk: usize,
    pub len: usize,
}

/// How much of a requested range must be ready before a read returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadAmount {
    /// Return as soon as at least one byte is ready, with everything ready so far.
    Available,
    /// Return only once the whole requested range is ready.
    All,
}

/// Decompressed data of one xorb block; see the [module docs](self).
///
/// Lifecycle: created empty with a capacity, then filled by one writer through the `append_*` methods, then either
/// [`finish`](Self::finish)ed or [`fail`](Self::fail)ed. Readers wait on the ready mark and see the failure, if any.
pub struct XorbBlockData {
    /// Size of the allocation, in bytes. Appends beyond it are rejected.
    capacity: usize,
    /// Whether `capacity` is the block's exact decompressed size (checked by `finish`), or only an upper bound.
    capacity_is_exact: bool,
    /// The allocation; created on first append so that planned-but-unfetched blocks take no memory.
    storage: OnceLock<Arc<Storage>>,
    /// Serializes writers, so that only one ever writes past the ready mark.
    write_lock: Mutex<()>,
    state: Mutex<State>,
    /// Wakes blocking readers when `state` changes.
    #[cfg(not(target_family = "wasm"))]
    state_changed: Condvar,
    /// Wakes async readers when `state` changes.
    updates: watch::Sender<()>,
}

struct State {
    /// Bytes below this offset are written and final.
    ready_bytes: usize,
    /// Start offset of every appended chunk, followed by the end of the last one: `[0, end_0, end_1, ...]`.
    chunk_offsets: Vec<u32>,
    status: Status,
}

enum Status {
    Filling,
    Complete,
    Failed(Arc<ClientError>),
}

impl XorbBlockData {
    /// Creates an empty block whose decompressed size is exactly `size` bytes.
    pub fn with_exact_size(size: usize) -> Self {
        Self::new(size, true)
    }

    /// Creates an empty block whose decompressed size is at most `max_size` bytes.
    pub fn with_size_bound(max_size: usize) -> Self {
        Self::new(max_size, false)
    }

    fn new(capacity: usize, capacity_is_exact: bool) -> Self {
        Self {
            capacity,
            capacity_is_exact,
            storage: OnceLock::new(),
            write_lock: Mutex::new(()),
            state: Mutex::new(State {
                ready_bytes: 0,
                chunk_offsets: vec![0],
                status: Status::Filling,
            }),
            #[cfg(not(target_family = "wasm"))]
            state_changed: Condvar::new(),
            updates: watch::Sender::new(()),
        }
    }

    /// The size of the allocation: the block's exact decompressed size, or an upper bound on it.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The number of chunks appended so far.
    pub fn num_chunks(&self) -> usize {
        self.lock_state().chunk_offsets.len() - 1
    }

    /// The number of decompressed bytes appended so far.
    pub fn ready_bytes(&self) -> usize {
        self.lock_state().ready_bytes
    }

    /// Appends already decompressed chunks. `chunk_offsets` are the chunk boundaries within `data`, in the format
    /// returned by the chunk deserializers: `[0, end_0, ..., data.len()]`.
    pub fn append_chunks(&self, data: &[u8], chunk_offsets: &[u32]) -> Result<()> {
        let valid_offsets = chunk_offsets.first() == Some(&0)
            && chunk_offsets.last().map(|&end| end as usize) == Some(data.len())
            && chunk_offsets.windows(2).all(|w| w[0] <= w[1]);
        if !valid_offsets {
            return Err(ClientError::Other(format!(
                "invalid chunk offsets for {} bytes of block data: {chunk_offsets:?}",
                data.len()
            )));
        }

        self.append(data.len(), &chunk_offsets[1..], |out| {
            out.copy_from_slice(data);
            Ok(())
        })
    }

    /// Decompresses one serialized chunk (its header and the `compressed` payload after it) and appends it.
    pub fn append_compressed_chunk(&self, header: &XorbChunkHeader, compressed: &[u8]) -> Result<()> {
        let len = header.get_uncompressed_length() as usize;
        self.append(len, &[len as u32], |out| Ok(decompress_chunk_into(header, compressed, out)?))
    }

    /// Decompresses and appends the serialized chunks in `serialized`, skipping the first `skip_chunks` of them.
    ///
    /// Used to resume a block after a retry: a retried request covers the same chunks again, and the ones already
    /// appended are skipped without being decompressed.
    pub fn append_serialized_chunks(&self, mut serialized: &[u8], skip_chunks: &mut usize) -> Result<()> {
        while !serialized.is_empty() {
            if serialized.len() < XORB_CHUNK_HEADER_LENGTH {
                return Err(ClientError::Other("truncated chunk header in xorb block data".to_string()));
            }
            let header = xet_core_structures::xorb_object::deserialize_chunk_header(&mut serialized)?;
            let compressed_len = header.get_compressed_length() as usize;
            if serialized.len() < compressed_len {
                return Err(ClientError::Other("truncated chunk data in xorb block data".to_string()));
            }
            let (compressed, rest) = serialized.split_at(compressed_len);
            serialized = rest;

            if *skip_chunks > 0 {
                *skip_chunks -= 1;
                continue;
            }
            self.append_compressed_chunk(&header, compressed)?;
        }
        Ok(())
    }

    /// Writes `len` bytes past the ready mark with `fill`, then publishes them as chunks ending at `relative_ends`
    /// (offsets relative to the start of the appended bytes; the last one must equal `len`).
    fn append(&self, len: usize, relative_ends: &[u32], fill: impl FnOnce(&mut [u8]) -> Result<()>) -> Result<()> {
        debug_assert_eq!(relative_ends.last().map(|&e| e as usize), Some(len));

        let _writer = self.write_lock.lock().unwrap_or_else(|e| e.into_inner());

        let start = {
            let state = self.lock_state();
            match &state.status {
                Status::Filling => {},
                Status::Complete => return Err(ClientError::Other("append to a finished xorb block".to_string())),
                Status::Failed(err) => return Err(ClientError::Other(format!("append to a failed xorb block: {err}"))),
            }
            state.ready_bytes
        };

        let end = start.checked_add(len).filter(|&end| end <= self.capacity).ok_or_else(|| {
            ClientError::Other(format!(
                "xorb block data exceeds its expected size of {} bytes ({start} + {len})",
                self.capacity
            ))
        })?;
        u32::try_from(end).map_err(|_| ClientError::Other(format!("xorb block of {end} bytes is too large")))?;

        let storage = self.storage.get_or_init(|| Arc::new(Storage::zeroed(self.capacity)));
        // SAFETY: `start..end` is within the allocation (checked against `capacity` above). It lies at or past the
        // ready mark, so no reader has a reference into it, and `write_lock` ensures no other writer does either.
        let out = unsafe { std::slice::from_raw_parts_mut(storage.ptr.as_ptr().add(start), len) };
        fill(out)?;

        let mut state = self.lock_state();
        state.chunk_offsets.extend(relative_ends.iter().map(|&e| start as u32 + e));
        state.ready_bytes = end;
        drop(state);
        self.notify();
        Ok(())
    }

    /// Marks the block complete. Fails (and marks the block failed) if its size is not the exact size it was created
    /// with.
    pub fn finish(&self) -> Result<()> {
        let _writer = self.write_lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut state = self.lock_state();
        match &state.status {
            Status::Filling => {},
            Status::Complete => return Ok(()),
            Status::Failed(err) => return Err(ClientError::Other(format!("xorb block failed: {err}"))),
        }

        let result = if self.capacity_is_exact && state.ready_bytes != self.capacity {
            let err = ClientError::Other(format!(
                "xorb block: expected {} decompressed bytes, got {}",
                self.capacity, state.ready_bytes
            ));
            state.status = Status::Failed(Arc::new(ClientError::Other(err.to_string())));
            Err(err)
        } else {
            state.status = Status::Complete;
            Ok(())
        };
        drop(state);
        self.notify();
        result
    }

    /// Marks the block failed with `err`, waking every reader. Does nothing if the block is already complete or
    /// failed.
    pub fn fail(&self, err: Arc<ClientError>) {
        let mut state = self.lock_state();
        if matches!(state.status, Status::Filling) {
            state.status = Status::Failed(err);
            drop(state);
            self.notify();
        }
    }

    /// The whole block and its chunk offsets (`[0, end_0, ...]`), once it is complete.
    pub fn contents(&self) -> Option<(Bytes, Vec<u32>)> {
        let state = self.lock_state();
        matches!(state.status, Status::Complete)
            .then(|| (self.ready_slice(0, state.ready_bytes), state.chunk_offsets.clone()))
    }

    /// Reads `range` starting `position` bytes into it, without waiting. Returns `None` if not enough of it is ready
    /// yet.
    pub fn try_read(
        &self,
        range: &BlockByteRange,
        position: usize,
        amount: ReadAmount,
    ) -> std::result::Result<Option<Bytes>, Arc<ClientError>> {
        self.read_locked(&self.lock_state(), range, position, amount)
    }

    /// Reads `range` starting `position` bytes into it, waiting until enough of it is ready.
    pub async fn read(
        &self,
        range: &BlockByteRange,
        position: usize,
        amount: ReadAmount,
    ) -> std::result::Result<Bytes, Arc<ClientError>> {
        // Subscribe before checking, so that an append between the check and the wait still wakes us.
        let mut updates = self.updates.subscribe();
        loop {
            if let Some(data) = self.try_read(range, position, amount)? {
                return Ok(data);
            }
            // The sender lives in `self`, so this only errors if `self` is gone, which `&self` rules out.
            let _ = updates.changed().await;
        }
    }

    /// Reads `range` starting `position` bytes into it, blocking the thread until enough of it is ready.
    #[cfg(not(target_family = "wasm"))]
    pub fn read_blocking(
        &self,
        range: &BlockByteRange,
        position: usize,
        amount: ReadAmount,
    ) -> std::result::Result<Bytes, Arc<ClientError>> {
        let mut state = self.lock_state();
        loop {
            if let Some(data) = self.read_locked(&state, range, position, amount)? {
                return Ok(data);
            }
            state = self.state_changed.wait(state).unwrap_or_else(|e| e.into_inner());
        }
    }

    fn read_locked(
        &self,
        state: &State,
        range: &BlockByteRange,
        position: usize,
        amount: ReadAmount,
    ) -> std::result::Result<Option<Bytes>, Arc<ClientError>> {
        if let Status::Failed(err) = &state.status {
            return Err(err.clone());
        }
        if position >= range.len {
            return Ok(Some(Bytes::new()));
        }
        let complete = matches!(state.status, Status::Complete);
        let out_of_range = || {
            Arc::new(ClientError::Other(format!(
                "range {range:?} is beyond the {} bytes and {} chunks of its xorb block",
                state.ready_bytes,
                state.chunk_offsets.len() - 1
            )))
        };

        // The range's start is known once its first chunk's start offset is recorded.
        let Some(&chunk_start) = state.chunk_offsets.get(range.first_chunk) else {
            return if complete { Err(out_of_range()) } else { Ok(None) };
        };
        let range_start = chunk_start as usize + range.offset_into_first_chunk;
        let start = range_start + position;
        let end = range_start + range.len;

        if complete && end > state.ready_bytes {
            return Err(out_of_range());
        }
        let ready_end = end.min(state.ready_bytes);
        let enough = match amount {
            ReadAmount::Available => ready_end > start,
            ReadAmount::All => ready_end == end,
        };
        Ok(enough.then(|| self.ready_slice(start, ready_end)))
    }

    /// A zero-copy view of `start..end`, which must be below the ready mark.
    fn ready_slice(&self, start: usize, end: usize) -> Bytes {
        if start == end {
            return Bytes::new();
        }
        let storage = self.storage.get().expect("ready bytes imply an allocation").clone();
        Bytes::from_owner(ReadySlice { storage, start, end })
    }

    fn lock_state(&self) -> MutexGuard<'_, State> {
        // State updates are single assignments, so a panic elsewhere cannot leave it inconsistent.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn notify(&self) {
        #[cfg(not(target_family = "wasm"))]
        self.state_changed.notify_all();
        self.updates.send_replace(());
    }
}

/// A fixed-size heap allocation written through raw pointers: the writer fills the part past the ready mark while
/// readers hold slices of the part below it, which `&mut [u8]` / `&[u8]` over the whole buffer could not express.
struct Storage {
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: `Storage` owns its allocation. Concurrent access follows the protocol in `XorbBlockData`: bytes are written
// once, by the single writer, before they are published below the ready mark (under the state mutex, which orders
// the write before any read); readers only read published bytes.
unsafe impl Send for Storage {}
unsafe impl Sync for Storage {}

impl Storage {
    fn zeroed(len: usize) -> Self {
        let raw = Box::into_raw(vec![0u8; len].into_boxed_slice());
        Self {
            // SAFETY: `Box::into_raw` never returns null.
            ptr: unsafe { NonNull::new_unchecked(raw as *mut u8) },
            len,
        }
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        // SAFETY: `ptr` and `len` came from `Box::into_raw` of a boxed slice in `zeroed`, and are freed only here.
        drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(self.ptr.as_ptr(), self.len)) });
    }
}

/// Owner of a [`Bytes`] view of published block bytes; keeps the allocation alive.
struct ReadySlice {
    storage: Arc<Storage>,
    start: usize,
    end: usize,
}

impl AsRef<[u8]> for ReadySlice {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: `start..end` is within the allocation and below the ready mark, so it is never written again.
        unsafe { std::slice::from_raw_parts(self.storage.ptr.as_ptr().add(self.start), self.end - self.start) }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use xet_core_structures::xorb_object::{CompressionScheme, serialize_chunk};

    use super::*;

    fn range(first_chunk: usize, offset_into_first_chunk: usize, len: usize) -> BlockByteRange {
        BlockByteRange {
            first_chunk,
            offset_into_first_chunk,
            len,
        }
    }

    fn serialized(chunks: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        for chunk in chunks {
            serialize_chunk(chunk, &mut out, CompressionScheme::LZ4).unwrap();
        }
        out
    }

    #[test]
    fn test_read_follows_ready_mark() {
        let data = XorbBlockData::with_exact_size(10);
        let term = range(0, 2, 6);

        assert_eq!(data.try_read(&term, 0, ReadAmount::Available).unwrap(), None);

        data.append_chunks(b"abcd", &[0, 4]).unwrap();
        assert_eq!(data.try_read(&term, 0, ReadAmount::Available).unwrap().unwrap(), Bytes::from_static(b"cd"));
        assert_eq!(data.try_read(&term, 0, ReadAmount::All).unwrap(), None);

        data.append_chunks(b"efghij", &[0, 3, 6]).unwrap();
        assert_eq!(data.try_read(&term, 2, ReadAmount::Available).unwrap().unwrap(), Bytes::from_static(b"efgh"));
        assert_eq!(data.try_read(&term, 0, ReadAmount::All).unwrap().unwrap(), Bytes::from_static(b"cdefgh"));
        assert_eq!(data.num_chunks(), 3);

        data.finish().unwrap();
        let (all, offsets) = data.contents().unwrap();
        assert_eq!(all, Bytes::from_static(b"abcdefghij"));
        assert_eq!(offsets, vec![0, 4, 7, 10]);
    }

    #[test]
    fn test_range_anchored_at_unknown_chunk_waits() {
        let data = XorbBlockData::with_exact_size(6);
        // Starts at chunk 2, whose offset is unknown until chunks 0 and 1 are in.
        let term = range(2, 0, 2);
        data.append_chunks(b"ab", &[0, 2]).unwrap();
        assert_eq!(data.try_read(&term, 0, ReadAmount::Available).unwrap(), None);
        data.append_chunks(b"cd", &[0, 2]).unwrap();
        assert_eq!(data.try_read(&term, 0, ReadAmount::Available).unwrap(), None);
        data.append_chunks(b"ef", &[0, 2]).unwrap();
        assert_eq!(data.try_read(&term, 0, ReadAmount::All).unwrap().unwrap(), Bytes::from_static(b"ef"));
    }

    #[test]
    fn test_serialized_chunks_with_skip() {
        let chunks: [&[u8]; 3] = [b"first chunk ", b"second chunk ", b"third"];
        let bytes = serialized(&chunks);

        let data = XorbBlockData::with_exact_size(chunks.iter().map(|c| c.len()).sum());
        let mut skip = 0;
        data.append_serialized_chunks(&bytes, &mut skip).unwrap();
        data.finish().unwrap();
        assert_eq!(data.contents().unwrap().0, Bytes::from_static(b"first chunk second chunk third"));

        // Resume after the first chunk: the retried data covers all three again.
        let data = XorbBlockData::with_exact_size(30);
        data.append_chunks(b"first chunk ", &[0, 12]).unwrap();
        let mut skip = 1;
        data.append_serialized_chunks(&bytes, &mut skip).unwrap();
        assert_eq!(skip, 0);
        data.finish().unwrap();
        assert_eq!(data.contents().unwrap().0, Bytes::from_static(b"first chunk second chunk third"));
        assert_eq!(data.contents().unwrap().1, vec![0, 12, 25, 30]);
    }

    #[test]
    fn test_size_checks() {
        let data = XorbBlockData::with_size_bound(4);
        assert!(data.append_chunks(b"abcde", &[0, 5]).is_err());
        data.append_chunks(b"abc", &[0, 3]).unwrap();
        data.finish().unwrap();
        assert_eq!(data.ready_bytes(), 3);

        let data = XorbBlockData::with_exact_size(4);
        data.append_chunks(b"abc", &[0, 3]).unwrap();
        assert!(data.finish().is_err());
        assert!(data.try_read(&range(0, 0, 1), 0, ReadAmount::Available).is_err());
    }

    #[test]
    fn test_range_past_end_of_complete_block_errors() {
        let data = XorbBlockData::with_size_bound(8);
        data.append_chunks(b"abcd", &[0, 4]).unwrap();
        data.finish().unwrap();
        assert!(data.try_read(&range(0, 2, 4), 0, ReadAmount::All).is_err());
        assert!(data.try_read(&range(5, 0, 1), 0, ReadAmount::Available).is_err());
    }

    #[test]
    fn test_failure_wakes_blocking_reader() {
        let data = Arc::new(XorbBlockData::with_exact_size(4));
        let reader = {
            let data = data.clone();
            std::thread::spawn(move || data.read_blocking(&range(0, 0, 4), 0, ReadAmount::All))
        };
        std::thread::sleep(Duration::from_millis(20));
        data.fail(Arc::new(ClientError::Other("boom".to_string())));
        assert!(reader.join().unwrap().is_err());
    }

    #[tokio::test]
    async fn test_async_reader_wakes_per_append() {
        let data = Arc::new(XorbBlockData::with_exact_size(4));
        let writer = {
            let data = data.clone();
            tokio::spawn(async move {
                for piece in [b"ab", b"cd"] {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    data.append_chunks(piece, &[0, 2]).unwrap();
                }
                data.finish().unwrap();
            })
        };

        let term = range(0, 0, 4);
        let first = data.read(&term, 0, ReadAmount::Available).await.unwrap();
        assert_eq!(first, Bytes::from_static(b"ab"));
        let rest = data.read(&term, first.len(), ReadAmount::All).await.unwrap();
        assert_eq!(rest, Bytes::from_static(b"cd"));
        writer.await.unwrap();
    }
}
