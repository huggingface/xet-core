use std::fs::File;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::task::JoinSet;
use xet_client::cas_types::FileRange;
use xet_runtime::core::XetContext;
use xet_runtime::utils::adjustable_semaphore::AdjustableSemaphorePermit;

use super::super::data_writer::{DataFuture, DataWriter};
use super::super::run_state::RunState;
use super::super::{FileReconstructionError, Result};

/// Write `buf` at absolute byte `offset` in `file` without using or disturbing the
/// file's cursor. Safe to call concurrently on clones of the same `Arc<File>`.
#[cfg(unix)]
fn positioned_write(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}

/// Windows equivalent: `seek_write` may write fewer bytes than requested, so loop.
#[cfg(windows)]
fn positioned_write(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut written = 0usize;
    while written < buf.len() {
        let n = file.seek_write(&buf[written..], offset + written as u64)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "seek_write wrote 0 bytes"));
        }
        written += n;
    }
    Ok(())
}

/// Writes reconstruction terms to a file in parallel using positioned writes.
///
/// Each term is written to its final offset as soon as its data future resolves,
/// concurrently and without ordering, on a shared `Arc<File>`. Positioned writes
/// (`write_all_at` / `seek_write`) ignore the fd cursor, so no locking or seeking is
/// required. Used only for the filesystem path (`reconstruct_to_file`) on non-wasm.
pub struct ParallelWriter {
    ctx: XetContext,
    file: Arc<File>,
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

impl ParallelWriter {
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(
        ctx: &XetContext,
        file: Arc<File>,
        base_offset: u64,
        run_state: Arc<RunState>,
    ) -> Box<dyn DataWriter> {
        Box::new(Self {
            ctx: ctx.clone(),
            file,
            base_offset,
            run_state,
            bytes_written: Arc::new(AtomicU64::new(0)),
            next_position: 0,
            active_tasks: JoinSet::new(),
            finished: false,
        })
    }
}

impl Drop for ParallelWriter {
    fn drop(&mut self) {
        if !self.finished {
            self.run_state.cancel();
        }
    }
}

#[async_trait::async_trait]
impl DataWriter for ParallelWriter {
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
        let file = self.file.clone();
        let run_state = self.run_state.clone();
        let bytes_written = self.bytes_written.clone();

        let task = async move {
            // `permit` is held for the whole task and dropped when it ends, releasing
            // download-buffer capacity only after the bytes are durably written.
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
                    .spawn_blocking(move || positioned_write(&file, &data, offset))
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

        // Parity with SequentialWriter: flush (a no-op for File; positioned writes already
        // reached the fd). Intentionally no fsync -- matches the sequential path's durability.
        {
            use std::io::Write;
            let mut file_ref: &File = &self.file;
            file_ref.flush()?;
        }

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
    use std::fs::OpenOptions;
    use std::path::PathBuf;
    use std::time::Duration;

    use bytes::Bytes;

    // `use super::*` brings in everything the module already imports: File, io, Arc,
    // AtomicU64/Ordering, JoinSet, FileRange, XetContext, AdjustableSemaphorePermit,
    // DataFuture, DataWriter, RunState, FileReconstructionError, Result, ParallelWriter.
    use super::*;

    fn test_context() -> XetContext {
        XetContext::default().unwrap()
    }

    fn temp_file() -> (tempfile::TempDir, Arc<File>, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        (dir, Arc::new(file), path)
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
    fn positioned_write_out_of_order_fills_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        positioned_write(&file, b"world", 6).unwrap();
        positioned_write(&file, b"hello ", 0).unwrap();

        drop(file);
        assert_eq!(std::fs::read(&path).unwrap(), b"hello world");
    }

    #[tokio::test]
    async fn writes_terms_in_order() {
        let (_dir, file, path) = temp_file();
        let mut w = ParallelWriter::new(&test_context(), file, 0, RunState::new_for_test());

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
        let (_dir, file, path) = temp_file();
        let mut w = ParallelWriter::new(&test_context(), file, 0, RunState::new_for_test());

        // First term resolves last; second term resolves immediately.
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
        let (_dir, file, path) = temp_file();
        // base_offset = 3: bytes land at absolute offset 3 onward.
        let mut w = ParallelWriter::new(&test_context(), file, 3, RunState::new_for_test());

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
        let (_dir, file, _path) = temp_file();
        let mut w = ParallelWriter::new(&test_context(), file, 0, RunState::new_for_test());

        // Range says 10 bytes but the future yields 2.
        w.set_next_term_data_source(FileRange::new(0, 10), None, immediate_future(Bytes::from("Hi")))
            .await
            .unwrap();

        assert!(w.finish().await.is_err());
    }

    #[tokio::test]
    async fn non_sequential_input_range_fails() {
        let (_dir, file, _path) = temp_file();
        let mut w = ParallelWriter::new(&test_context(), file, 0, RunState::new_for_test());

        w.set_next_term_data_source(FileRange::new(0, 4), None, immediate_future(Bytes::from("AAAA")))
            .await
            .unwrap();
        // Gap: next range must start at 4.
        let result = w
            .set_next_term_data_source(FileRange::new(8, 12), None, immediate_future(Bytes::from("BBBB")))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn term_error_propagates_from_finish() {
        let (_dir, file, _path) = temp_file();
        let mut w = ParallelWriter::new(&test_context(), file, 0, RunState::new_for_test());

        w.set_next_term_data_source(FileRange::new(0, 4), None, err_future())
            .await
            .unwrap();

        assert!(w.finish().await.is_err());
    }
}
