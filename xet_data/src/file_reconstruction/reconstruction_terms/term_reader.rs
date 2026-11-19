use std::sync::Arc;

use bytes::Bytes;
use xet_client::ClientError;
use xet_client::cas_client::{BlockByteRange, ReadAmount, XorbBlockData};

use super::super::error::{FileReconstructionError, Result};

/// Reads one file term's bytes from its xorb block's shared data as the block is downloaded.
///
/// A reader is a position within the term's byte range of the block; it holds no data of its own, and reads are
/// zero-copy views of the block. The block's download task, started when the term was scheduled, fills the data
/// independently of when (or whether) the term is read. If the download fails, every read returns the error.
pub struct TermReader {
    data: Arc<XorbBlockData>,
    range: BlockByteRange,
    /// Bytes of the term already returned.
    position: usize,
}

impl TermReader {
    pub(crate) fn new(data: Arc<XorbBlockData>, range: BlockByteRange) -> Self {
        Self {
            data,
            range,
            position: 0,
        }
    }

    /// The term's total length in bytes.
    pub fn len(&self) -> u64 {
        self.range.len as u64
    }

    /// Whether the term has no bytes.
    pub fn is_empty(&self) -> bool {
        self.range.len == 0
    }

    /// Whether every byte of the term has been returned.
    pub fn is_finished(&self) -> bool {
        self.position == self.range.len
    }

    /// Returns the rest of the term if all of it is ready, without waiting; `None` if it is not ready yet.
    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn try_read_rest(&mut self) -> Result<Option<Bytes>> {
        let data = self
            .data
            .try_read(&self.range, self.position, ReadAmount::All)
            .map_err(read_error)?;
        Ok(data.map(|data| self.advance(data)))
    }

    /// Returns the rest of the term, blocking the thread until all of it is ready.
    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn read_rest_blocking(&mut self) -> Result<Bytes> {
        let data = self
            .data
            .read_blocking(&self.range, self.position, ReadAmount::All)
            .map_err(read_error)?;
        Ok(self.advance(data))
    }

    /// Returns the term's next ready bytes, waiting until at least one is ready; `None` once the whole term has
    /// been returned.
    pub(crate) async fn read_next(&mut self) -> Result<Option<Bytes>> {
        if self.is_finished() {
            return Ok(None);
        }
        let data = self
            .data
            .read(&self.range, self.position, ReadAmount::Available)
            .await
            .map_err(read_error)?;
        Ok(Some(self.advance(data)))
    }

    /// Blocking version of [`read_next`](Self::read_next).
    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn read_next_blocking(&mut self) -> Result<Option<Bytes>> {
        if self.is_finished() {
            return Ok(None);
        }
        let data = self
            .data
            .read_blocking(&self.range, self.position, ReadAmount::Available)
            .map_err(read_error)?;
        Ok(Some(self.advance(data)))
    }

    fn advance(&mut self, data: Bytes) -> Bytes {
        self.position += data.len();
        debug_assert!(self.position <= self.range.len);
        data
    }
}

fn read_error(err: Arc<ClientError>) -> FileReconstructionError {
    FileReconstructionError::ClientError(err)
}

#[cfg(test)]
pub(crate) mod test_utils {
    use std::time::Duration;

    use super::*;

    fn whole_range(len: usize) -> BlockByteRange {
        BlockByteRange {
            first_chunk: 0,
            offset_into_first_chunk: 0,
            len,
        }
    }

    /// A reader whose data is already complete.
    pub(crate) fn ready_reader(data: impl Into<Bytes>) -> TermReader {
        let data: Bytes = data.into();
        let block = Arc::new(XorbBlockData::with_exact_size(data.len()));
        block.append_chunks(&data, &[0, data.len() as u32]).unwrap();
        block.finish().unwrap();
        TermReader::new(block, whole_range(data.len()))
    }

    /// A reader whose data arrives after `delay`.
    pub(crate) fn delayed_reader(data: impl Into<Bytes>, delay: Duration) -> TermReader {
        let data: Bytes = data.into();
        let block = Arc::new(XorbBlockData::with_exact_size(data.len()));
        let writer = block.clone();
        let len = data.len();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            writer.append_chunks(&data, &[0, data.len() as u32]).unwrap();
            writer.finish().unwrap();
        });
        TermReader::new(block, whole_range(len))
    }

    /// A reader of `len` bytes whose block download fails with `message`.
    pub(crate) fn failing_reader(len: usize, message: &str) -> TermReader {
        let block = Arc::new(XorbBlockData::with_exact_size(len));
        block.fail(Arc::new(ClientError::Other(message.to_string())));
        TermReader::new(block, whole_range(len))
    }
}
