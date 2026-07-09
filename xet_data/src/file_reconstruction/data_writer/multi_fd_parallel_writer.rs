use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::task::JoinSet;
use xet_client::cas_types::FileRange;
use xet_runtime::core::XetContext;
use xet_runtime::utils::adjustable_semaphore::AdjustableSemaphorePermit;

use super::super::data_writer::{DataFuture, DataWriter};
use super::super::run_state::RunState;
use super::super::{FileReconstructionError, Result};

/// Write `buf` at `offset` by opening a *fresh* handle to `path`, seeking, writing,
/// flushing, and closing it. This is the pre-#603 multi-handle approach: unlike the
/// positioned-write path (`ParallelWriter`), it does not share a file descriptor and
/// pays an open/seek/flush/close per term. Concurrent calls are safe because each has
/// its own descriptor and thus its own cursor.
fn open_seek_write_close(path: &Path, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    // No `create`/`truncate`: the destination file already exists (created and, for a
    // full download, truncated by `reconstruct_to_file` before the writer is built).
    let mut file = OpenOptions::new().write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(buf)?;
    file.flush()?;
    // `file` is dropped here, closing the descriptor.
    Ok(())
}

/// Writes reconstruction terms to a file in parallel using one fresh file handle per
/// term (open, seek, write, flush, close).
///
/// Structurally identical to [`ParallelWriter`](super::ParallelWriter) except for the
/// per-term open/seek/close instead of a shared `Arc<File>` + positioned write. Used
/// only for the filesystem path (`reconstruct_to_file`) on non-wasm, and only when
/// `reconstruction.write_multi_fd` is set -- it exists to benchmark the multi-handle
/// approach against positioned writes.
pub struct MultiFdParallelWriter {
    ctx: XetContext,
    /// Destination path; a fresh handle is opened to it for every term.
    path: Arc<PathBuf>,
    /// Absolute file offset that relative (0-based) term ranges are added to.
    base_offset: u64,
    run_state: Arc<RunState>,
    bytes_written: Arc<AtomicU64>,
    /// Expected start of the next term; validates that inputs arrive contiguous and in
    /// order. Does NOT constrain the order writes hit disk.
    next_position: u64,
    active_tasks: JoinSet<Result<()>>,
    finished: bool,
}

impl MultiFdParallelWriter {
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(
        ctx: &XetContext,
        path: PathBuf,
        base_offset: u64,
        run_state: Arc<RunState>,
    ) -> Box<dyn DataWriter> {
        Box::new(Self {
            ctx: ctx.clone(),
            path: Arc::new(path),
            base_offset,
            run_state,
            bytes_written: Arc::new(AtomicU64::new(0)),
            next_position: 0,
            active_tasks: JoinSet::new(),
            finished: false,
        })
    }
}

impl Drop for MultiFdParallelWriter {
    fn drop(&mut self) {
        if !self.finished {
            self.run_state.cancel();
        }
    }
}

#[async_trait::async_trait]
impl DataWriter for MultiFdParallelWriter {
    async fn set_next_term_data_source(
        &mut self,
        byte_range: FileRange,
        permit: Option<AdjustableSemaphorePermit>,
        data_future: DataFuture,
    ) -> Result<()> {
        self.run_state.check_error()?;

        // Surface errors from already-finished term tasks promptly.
        while let Some(result) = self.active_tasks.try_join_next() {
            result.map_err(|e| FileReconstructionError::InternalError(format!("Task join error: {e}")))??;
        }

        if self.finished {
            return Err(FileReconstructionError::InternalWriterError("Writer has already finished".to_string()));
        }

        // Input-stream contiguity check (order of scheduling, not order of writes).
        if byte_range.start != self.next_position {
            return Err(FileReconstructionError::InternalWriterError(format!(
                "Byte range not sequential: expected start at {}, got {}",
                self.next_position, byte_range.start
            )));
        }
        self.next_position = byte_range.end;

        let expected_size = byte_range.end - byte_range.start;
        let offset = self.base_offset + byte_range.start;

        let ctx = self.ctx.clone();
        let path = self.path.clone();
        let run_state = self.run_state.clone();
        let bytes_written = self.bytes_written.clone();

        let task = async move {
            // `permit` is held for the whole task and dropped when it ends, releasing
            // download-buffer capacity only after the bytes are written.
            let _permit = permit;
            let result = async {
                run_state.check_error()?;

                let data = data_future.await?;
                if data.len() as u64 != expected_size {
                    return Err(FileReconstructionError::InternalWriterError(format!(
                        "Data size mismatch: expected {} bytes, got {} bytes",
                        expected_size,
                        data.len()
                    )));
                }

                ctx.runtime
                    .spawn_blocking(move || {
                        #[cfg(feature = "write-timing")]
                        let write_start = std::time::Instant::now();
                        let result = open_seek_write_close(&path, &data, offset);
                        #[cfg(feature = "write-timing")]
                        if result.is_ok() {
                            crate::file_reconstruction::write_timing::record(
                                write_start.elapsed().as_nanos() as u64,
                                data.len() as u64,
                            );
                        }
                        result
                    })
                    .await??;

                bytes_written.fetch_add(expected_size, Ordering::Relaxed);
                run_state.report_bytes_written(expected_size);
                Ok(())
            }
            .await;

            if let Err(ref e) = result {
                run_state.set_error(e.clone());
            }
            result
        };

        self.active_tasks.spawn(task);

        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> Result<u64> {
        self.run_state.check_error()?;

        if self.finished {
            return Err(FileReconstructionError::InternalWriterError("Writer has already finished".to_string()));
        }
        self.finished = true;

        let expected_bytes = self.next_position;

        while let Some(result) = self.active_tasks.join_next().await {
            result.map_err(|e| FileReconstructionError::InternalError(format!("Task join error: {e}")))??;
        }

        self.run_state.check_error()?;

        // Each term already flushed and closed its own handle, so there is no shared
        // handle to flush here (unlike ParallelWriter/SequentialWriter).

        let actual_bytes = self.bytes_written.load(Ordering::Relaxed);
        if actual_bytes != expected_bytes {
            return Err(FileReconstructionError::InternalWriterError(format!(
                "Bytes written mismatch: expected {} bytes, but wrote {} bytes",
                expected_bytes, actual_bytes
            )));
        }

        Ok(actual_bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;

    use super::*;

    fn test_context() -> XetContext {
        XetContext::default().unwrap()
    }

    /// Creates the destination file empty (mirrors reconstruct_to_file's truncating open)
    /// and returns the temp dir plus the path.
    fn temp_path() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");
        OpenOptions::new().write(true).create(true).truncate(true).open(&path).unwrap();
        (dir, path)
    }

    fn immediate_future(data: Bytes) -> DataFuture {
        Box::pin(async move { Ok(data) })
    }

    fn delayed_future(data: Bytes, delay_ms: u64) -> DataFuture {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            Ok(data)
        })
    }

    fn err_future() -> DataFuture {
        Box::pin(async move { Err(FileReconstructionError::InternalError("boom".to_string())) })
    }

    #[test]
    fn open_seek_write_close_out_of_order_fills_correctly() {
        let (_dir, path) = temp_path();
        open_seek_write_close(&path, b"world", 6).unwrap();
        open_seek_write_close(&path, b"hello ", 0).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"hello world");
    }

    #[tokio::test]
    async fn writes_terms_in_order() {
        let (_dir, path) = temp_path();
        let mut w = MultiFdParallelWriter::new(&test_context(), path.clone(), 0, RunState::new_for_test());

        w.set_next_term_data_source(FileRange::new(0, 6), None, immediate_future(Bytes::from("hello ")))
            .await
            .unwrap();
        w.set_next_term_data_source(FileRange::new(6, 11), None, immediate_future(Bytes::from("world")))
            .await
            .unwrap();

        let n = w.finish().await.unwrap();
        assert_eq!(n, 11);
        assert_eq!(std::fs::read(&path).unwrap(), b"hello world");
    }

    #[tokio::test]
    async fn writes_correctly_when_data_resolves_out_of_order() {
        let (_dir, path) = temp_path();
        let mut w = MultiFdParallelWriter::new(&test_context(), path.clone(), 0, RunState::new_for_test());

        w.set_next_term_data_source(FileRange::new(0, 4), None, delayed_future(Bytes::from("AAAA"), 50))
            .await
            .unwrap();
        w.set_next_term_data_source(FileRange::new(4, 8), None, immediate_future(Bytes::from("BBBB")))
            .await
            .unwrap();

        let n = w.finish().await.unwrap();
        assert_eq!(n, 8);
        assert_eq!(std::fs::read(&path).unwrap(), b"AAAABBBB");
    }

    #[tokio::test]
    async fn writes_at_base_offset() {
        let (_dir, path) = temp_path();
        let mut w = MultiFdParallelWriter::new(&test_context(), path.clone(), 3, RunState::new_for_test());

        w.set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("hello")))
            .await
            .unwrap();

        let n = w.finish().await.unwrap();
        assert_eq!(n, 5);
        let content = std::fs::read(&path).unwrap();
        assert_eq!(content.len(), 8);
        assert_eq!(&content[3..8], b"hello");
    }

    #[tokio::test]
    async fn size_mismatch_fails() {
        let (_dir, path) = temp_path();
        let mut w = MultiFdParallelWriter::new(&test_context(), path, 0, RunState::new_for_test());

        w.set_next_term_data_source(FileRange::new(0, 10), None, immediate_future(Bytes::from("Hi")))
            .await
            .unwrap();

        assert!(w.finish().await.is_err());
    }

    #[tokio::test]
    async fn non_sequential_input_range_fails() {
        let (_dir, path) = temp_path();
        let mut w = MultiFdParallelWriter::new(&test_context(), path, 0, RunState::new_for_test());

        w.set_next_term_data_source(FileRange::new(0, 4), None, immediate_future(Bytes::from("AAAA")))
            .await
            .unwrap();
        let result = w
            .set_next_term_data_source(FileRange::new(8, 12), None, immediate_future(Bytes::from("BBBB")))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn term_error_propagates_from_finish() {
        let (_dir, path) = temp_path();
        let mut w = MultiFdParallelWriter::new(&test_context(), path, 0, RunState::new_for_test());

        w.set_next_term_data_source(FileRange::new(0, 4), None, err_future())
            .await
            .unwrap();

        assert!(w.finish().await.is_err());
    }
}
