use std::collections::VecDeque;
use std::io::{IoSlice, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use cas_types::FileRange;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{Mutex, OwnedSemaphorePermit, oneshot};
use tokio::task::{JoinHandle, JoinSet, spawn_blocking};

use crate::data_writer::{DataFuture, DataWriter};
use crate::{ErrorState, FileReconstructionError, Result};

/// Items sent to the background writer thread.
enum QueueItem {
    Data {
        receiver: oneshot::Receiver<Bytes>,
        permit: Option<OwnedSemaphorePermit>,
    },
    Finish,
}

/// Pending write data with its associated permit.
type PendingWrite = (Bytes, Option<OwnedSemaphorePermit>);

/// Background writer thread that processes queue items and writes to the output.
struct WriterThread<W: Write + Send + 'static> {
    rx: UnboundedReceiver<QueueItem>,
    writer: W,
    bytes_written: Arc<AtomicU64>,
    pending: Option<QueueItem>,
    finished: bool,
}

impl<W: Write + Send + 'static> WriterThread<W> {
    fn new(rx: UnboundedReceiver<QueueItem>, writer: W, bytes_written: Arc<AtomicU64>) -> Self {
        Self {
            rx,
            writer,
            bytes_written,
            pending: None,
            finished: false,
        }
    }

    /// Get the next write data, optionally blocking to receive it.
    /// Returns Some((data, permit)) if data is available, None if finished or channel closed.
    /// Sets self.finished = true when Finish is received.
    ///
    /// If should_block is false and data isn't ready yet, the QueueItem is put back
    /// in pending and None is returned.
    #[inline]
    fn next_write(&mut self, should_block: bool) -> Result<Option<PendingWrite>> {
        // First, check if we have a pending item.
        if self.pending.is_none() {
            // Try to get from channel.
            self.pending = if should_block {
                self.rx.blocking_recv()
            } else {
                self.rx.try_recv().ok()
            };
        }

        // Process the pending item if we have one.
        match self.pending.take() {
            Some(QueueItem::Data { mut receiver, permit }) => {
                if should_block {
                    let data = receiver.blocking_recv().map_err(|_| {
                        FileReconstructionError::InternalWriterError(
                            "Data sender was dropped before sending data.".to_string(),
                        )
                    })?;
                    Ok(Some((data, permit)))
                } else {
                    // Non-blocking: try to receive data.
                    match receiver.try_recv() {
                        Ok(data) => Ok(Some((data, permit))),
                        Err(oneshot::error::TryRecvError::Empty) => {
                            // Data not ready - put the item back in pending.
                            self.pending = Some(QueueItem::Data { receiver, permit });
                            Ok(None)
                        },
                        Err(oneshot::error::TryRecvError::Closed) => Err(FileReconstructionError::InternalWriterError(
                            "Data sender was dropped before sending data.".to_string(),
                        )),
                    }
                }
            },
            Some(QueueItem::Finish) => {
                self.finished = true;
                Ok(None)
            },
            None => Ok(None),
        }
    }

    /// Run the non-vectorized writer loop.
    fn run(mut self) -> Result<()> {
        while let Some((data, permit)) = self.next_write(true)? {
            let len = data.len() as u64;
            self.writer.write_all(&data)?;
            self.bytes_written.fetch_add(len, Ordering::Relaxed);
            drop(permit);

            if self.finished {
                break;
            }
        }

        debug_assert!(self.finished);

        self.writer.flush()?;
        Ok(())
    }

    /// Run the vectorized writer loop.
    fn run_vectorized(mut self) -> Result<()> {
        let mut pending_writes: VecDeque<PendingWrite> = VecDeque::new();

        while !self.finished || !pending_writes.is_empty() {
            // If no pending writes, block to get at least one.
            if pending_writes.is_empty() {
                let Some(write) = self.next_write(true)? else {
                    break;
                };

                pending_writes.push_back(write);
            }

            // Try to get more data non-blocking to batch writes.
            while let Some(write) = self.next_write(false)? {
                pending_writes.push_back(write);
            }

            // Build IoSlice vector from all pending writes.
            let io_slices: Vec<IoSlice<'_>> = pending_writes.iter().map(|(data, _)| IoSlice::new(data)).collect();

            // Call write_vectored.
            let written = match self.writer.write_vectored(&io_slices) {
                Ok(0) if !io_slices.is_empty() => {
                    return Err(FileReconstructionError::IoError(Arc::new(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "write_vectored returned 0 with non-empty buffers",
                    ))));
                },
                Ok(n) => n,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(FileReconstructionError::IoError(Arc::new(e))),
            };

            self.bytes_written.fetch_add(written as u64, Ordering::Relaxed);

            // Pop completed writes, releasing permits. For partial writes, slice the Bytes.
            let mut remaining = written;
            while remaining > 0 && !pending_writes.is_empty() {
                let front_len = pending_writes.front().unwrap().0.len();
                if remaining >= front_len {
                    remaining -= front_len;
                    pending_writes.pop_front();
                } else {
                    let front = pending_writes.front_mut().unwrap();
                    front.0 = front.0.slice(remaining..);
                    remaining = 0;
                }
            }
        }

        self.writer.flush()?;
        Ok(())
    }
}

/// Mutable state for the writing queue, protected by a mutex.
struct WritingQueueState {
    sender: UnboundedSender<QueueItem>,
    next_position: u64,
    finished: bool,
}

/// Writes data sequentially to an output stream from async data futures.
/// Spawns async tasks to resolve futures and a background thread to perform
/// blocking writes, allowing out-of-order future resolution with in-order writes.
pub struct SequentialWriter {
    queue_state: Mutex<WritingQueueState>,
    background_handle: Mutex<Option<JoinHandle<()>>>,
    error_state: Arc<ErrorState>,
    bytes_written: Arc<AtomicU64>,
    active_tasks: Arc<Mutex<JoinSet<Result<()>>>>,
}

#[async_trait::async_trait]
impl DataWriter for SequentialWriter {
    /// Sets the source for the next block of data; this is a future that
    /// can be executing in the background.  This must be the next one sequentially,
    /// otherwise it will error out.
    async fn set_next_term_data_source(
        &self,
        byte_range: FileRange,
        permit: Option<OwnedSemaphorePermit>,
        data_future: DataFuture,
    ) -> Result<()> {
        self.error_state.check()?;

        // Check for any errors from previously spawned tasks.
        {
            let mut tasks = self.active_tasks.lock().await;
            while let Some(result) = tasks.try_join_next() {
                result.map_err(|e| FileReconstructionError::InternalError(format!("Task join error: {e}")))??;
            }
        }

        let (sender, expected_size) = {
            let mut state = self.queue_state.lock().await;

            if state.finished {
                return Err(FileReconstructionError::InternalWriterError("Writer has already finished".to_string()));
            }

            if byte_range.start != state.next_position {
                return Err(FileReconstructionError::InternalWriterError(format!(
                    "Byte range not sequential: expected start at {}, got {}",
                    state.next_position, byte_range.start
                )));
            }

            let expected_size = byte_range.end - byte_range.start;
            state.next_position = byte_range.end;

            let (sender, receiver) = oneshot::channel();

            state.sender.send(QueueItem::Data { receiver, permit }).map_err(|_| {
                FileReconstructionError::InternalWriterError("Background writer channel closed".to_string())
            })?;

            (sender, expected_size)
        };

        // Spawn a task to evaluate the future and send the result.
        let error_state = self.error_state.clone();
        let task = async move {
            error_state.check()?;

            let data = data_future.await?;

            if data.len() as u64 != expected_size {
                return Err(FileReconstructionError::InternalWriterError(format!(
                    "Data size mismatch: expected {} bytes, got {} bytes",
                    expected_size,
                    data.len()
                )));
            }

            sender.send(data).map_err(|_| {
                FileReconstructionError::InternalWriterError("Failed to send data: receiver dropped".to_string())
            })?;

            Ok(())
        };

        {
            let mut tasks = self.active_tasks.lock().await;
            tasks.spawn(task);
        }

        Ok(())
    }

    /// Wait for the background writer to finish and all tasks to complete.
    /// Returns the number of bytes written.
    async fn finish(&self) -> Result<u64> {
        self.error_state.check()?;

        let expected_bytes = {
            let mut state = self.queue_state.lock().await;

            if state.finished {
                return Err(FileReconstructionError::InternalWriterError("Writer has already finished".to_string()));
            }

            state.finished = true;

            state.sender.send(QueueItem::Finish).map_err(|_| {
                FileReconstructionError::InternalWriterError("Background writer channel closed".to_string())
            })?;

            state.next_position
        };

        // Wait for all spawned data-fetching tasks to complete.
        {
            let mut tasks = self.active_tasks.lock().await;
            while let Some(result) = tasks.join_next().await {
                result.map_err(|e| FileReconstructionError::InternalError(format!("Task join error: {e}")))??;
            }
        }

        let handle =
            self.background_handle.lock().await.take().ok_or_else(|| {
                FileReconstructionError::InternalWriterError("Background writer not running".to_string())
            })?;

        handle
            .await
            .map_err(|e| FileReconstructionError::InternalWriterError(format!("Background writer task failed: {e}")))?;

        self.error_state.check()?;

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

impl SequentialWriter {
    /// Creates a new sequential writer using standard single-buffer writes.
    ///
    /// The writer type `W` can be any type implementing `Write + Send + 'static`.
    /// The writer is moved to a background thread for blocking I/O operations.
    pub fn new<W: Write + Send + 'static>(writer: W) -> Self {
        Self::new_internal(writer, false)
    }

    /// Creates a new sequential writer that uses vectorized writes (write_vectored).
    ///
    /// Use this when the underlying writer benefits from batched writes, such as:
    /// - Network sockets
    /// - Buffered file writers
    /// - Writers that implement efficient write_vectored
    ///
    /// The vectorized writer batches multiple pending writes and uses write_vectored
    /// to write them in a single system call when possible.
    ///
    /// The writer type `W` can be any type implementing `Write + Send + 'static`.
    /// The writer is moved to a background thread for blocking I/O operations.
    pub fn new_vectorized<W: Write + Send + 'static>(writer: W) -> Self {
        Self::new_internal(writer, true)
    }

    fn new_internal<W: Write + Send + 'static>(writer: W, use_vectorized: bool) -> Self {
        let (tx, rx) = unbounded_channel::<QueueItem>();
        let error_state = Arc::new(ErrorState::new());
        let bytes_written = Arc::new(AtomicU64::new(0));

        let error_state_clone = error_state.clone();
        let bytes_written_clone = bytes_written.clone();

        let handle = spawn_blocking(move || {
            let writer_thread = WriterThread::new(rx, writer, bytes_written_clone);
            let result = if use_vectorized {
                writer_thread.run_vectorized()
            } else {
                writer_thread.run()
            };
            if let Err(err) = result {
                error_state_clone.set(err);
            }
        });

        let writing_queue_state = WritingQueueState {
            sender: tx,
            next_position: 0,
            finished: false,
        };

        Self {
            queue_state: Mutex::new(writing_queue_state),
            background_handle: Mutex::new(Some(handle)),
            error_state,
            bytes_written,
            active_tasks: Arc::new(Mutex::new(JoinSet::new())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::time::Duration;

    use tokio::sync::Semaphore;

    use super::*;

    struct SharedBuffer(Arc<std::sync::Mutex<Vec<u8>>>);

    impl Write for SharedBuffer {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Configuration for the TestWriter behavior.
    #[derive(Clone, Default)]
    struct TestWriterConfig {
        /// Maximum bytes to write per write call (simulates partial writes).
        max_write_size: Option<usize>,
        /// Maximum bytes to write per write_vectored call.
        max_vectored_write_size: Option<usize>,
        /// If true, occasionally return Interrupted error.
        simulate_interrupts: bool,
        /// Counter for how many writes before an interrupt (cycles).
        interrupt_frequency: usize,
    }

    impl TestWriterConfig {
        fn vectorized() -> Self {
            Self::default()
        }

        fn vectorized_partial(max_size: usize) -> Self {
            Self {
                max_vectored_write_size: Some(max_size),
                ..Default::default()
            }
        }

        fn partial(max_size: usize) -> Self {
            Self {
                max_write_size: Some(max_size),
                ..Default::default()
            }
        }

        fn vectorized_with_interrupts() -> Self {
            Self {
                simulate_interrupts: true,
                interrupt_frequency: 2,
                ..Default::default()
            }
        }
    }

    /// A test writer that can simulate various behaviors for testing.
    ///
    /// Features:
    /// - Configurable partial writes (max bytes per call)
    /// - Configurable interrupt simulation
    struct TestWriter {
        buffer: Arc<std::sync::Mutex<Vec<u8>>>,
        config: TestWriterConfig,
        write_count: Arc<AtomicU64>,
        vectored_write_count: Arc<AtomicU64>,
        interrupt_counter: Arc<AtomicU64>,
    }

    impl TestWriter {
        fn new(config: TestWriterConfig) -> Self {
            Self {
                buffer: Arc::new(std::sync::Mutex::new(Vec::new())),
                config,
                write_count: Arc::new(AtomicU64::new(0)),
                vectored_write_count: Arc::new(AtomicU64::new(0)),
                interrupt_counter: Arc::new(AtomicU64::new(0)),
            }
        }

        fn should_interrupt(&self) -> bool {
            if !self.config.simulate_interrupts {
                return false;
            }
            let count = self.interrupt_counter.fetch_add(1, Ordering::Relaxed);
            count % self.config.interrupt_frequency as u64 == 0
        }
    }

    impl Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.should_interrupt() {
                return Err(io::Error::new(io::ErrorKind::Interrupted, "simulated interrupt"));
            }

            self.write_count.fetch_add(1, Ordering::Relaxed);

            let bytes_to_write = match self.config.max_write_size {
                Some(max) => buf.len().min(max),
                None => buf.len(),
            };

            self.buffer.lock().unwrap().extend_from_slice(&buf[..bytes_to_write]);
            Ok(bytes_to_write)
        }

        fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
            if self.should_interrupt() {
                return Err(io::Error::new(io::ErrorKind::Interrupted, "simulated interrupt"));
            }

            self.vectored_write_count.fetch_add(1, Ordering::Relaxed);

            let total_len: usize = bufs.iter().map(|b| b.len()).sum();
            let max_write = self.config.max_vectored_write_size.unwrap_or(total_len);
            let bytes_to_write = total_len.min(max_write);

            let mut remaining = bytes_to_write;
            let mut buffer = self.buffer.lock().unwrap();

            for buf in bufs {
                if remaining == 0 {
                    break;
                }
                let to_write = buf.len().min(remaining);
                buffer.extend_from_slice(&buf[..to_write]);
                remaining -= to_write;
            }

            Ok(bytes_to_write)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn immediate_future(data: Bytes) -> DataFuture {
        Box::pin(async move { Ok(data) })
    }

    #[tokio::test]
    async fn test_sequential_writes() {
        let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let buffer_clone = buffer.clone();

        let writer = Arc::new(SequentialWriter::new(Box::new(SharedBuffer(buffer_clone))));

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(5, 6), None, immediate_future(Bytes::from(" ")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(6, 11), None, immediate_future(Bytes::from("World")))
            .await
            .unwrap();

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"Hello World");
    }

    #[tokio::test]
    async fn test_delayed_future() {
        let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let buffer_clone = buffer.clone();

        let writer = Arc::new(SequentialWriter::new(Box::new(SharedBuffer(buffer_clone))));

        // Create futures that resolve with delays
        let f0: DataFuture = Box::pin(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(Bytes::from("Hello"))
        });
        let f1: DataFuture = Box::pin(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(Bytes::from(" "))
        });
        let f2: DataFuture = Box::pin(async { Ok(Bytes::from("World")) });

        writer.set_next_term_data_source(FileRange::new(0, 5), None, f0).await.unwrap();
        writer.set_next_term_data_source(FileRange::new(5, 6), None, f1).await.unwrap();
        writer.set_next_term_data_source(FileRange::new(6, 11), None, f2).await.unwrap();

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"Hello World");
    }

    #[tokio::test]
    async fn test_size_mismatch_error() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer)));

        writer
            .set_next_term_data_source(FileRange::new(0, 10), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();

        let result = writer.finish().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_background_writer_error_propagates() {
        struct FailingWriter;
        impl Write for FailingWriter {
            fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
                Err(io::Error::new(io::ErrorKind::Other, "Simulated write failure"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let writer = Arc::new(SequentialWriter::new(Box::new(FailingWriter)));

        writer
            .set_next_term_data_source(FileRange::new(0, 4), None, immediate_future(Bytes::from("Test")))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;

        let result = writer
            .set_next_term_data_source(FileRange::new(4, 8), None, immediate_future(Bytes::from("More")))
            .await;

        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::IoError(_))));
    }

    #[tokio::test]
    async fn test_finish_twice_returns_error() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer)));

        writer.finish().await.unwrap();
        let result = writer.finish().await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_write_after_finish_returns_error() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer)));

        writer.finish().await.unwrap();
        let result = writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_flush_error_propagates() {
        struct FlushFailingWriter;
        impl Write for FlushFailingWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Err(io::Error::new(io::ErrorKind::Other, "Simulated flush failure"))
            }
        }

        let writer = Arc::new(SequentialWriter::new(Box::new(FlushFailingWriter)));
        let result = writer.finish().await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::IoError(_))));
    }

    #[tokio::test]
    async fn test_future_error_propagates() {
        let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let buffer_clone = buffer.clone();

        let writer = Arc::new(SequentialWriter::new(Box::new(SharedBuffer(buffer_clone))));

        let failing_future: DataFuture =
            Box::pin(async { Err(FileReconstructionError::InternalError("Simulated future error".to_string())) });

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, failing_future)
            .await
            .unwrap();

        let result = writer.finish().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_size_mismatch_too_small() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer)));

        writer
            .set_next_term_data_source(FileRange::new(0, 10), None, immediate_future(Bytes::from("Hi")))
            .await
            .unwrap();

        let result = writer.finish().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_size_mismatch_too_large() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer)));

        writer
            .set_next_term_data_source(FileRange::new(0, 2), None, immediate_future(Bytes::from("Hello World")))
            .await
            .unwrap();

        let result = writer.finish().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bytes_written_tracking() {
        let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let buffer_clone = buffer.clone();

        let writer = Arc::new(SequentialWriter::new(Box::new(SharedBuffer(buffer_clone))));

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(5, 11), None, immediate_future(Bytes::from(" World")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(11, 16), None, immediate_future(Bytes::from("!!!!!")))
            .await
            .unwrap();

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"Hello World!!!!!");
        assert_eq!(result.len(), 16);
    }

    #[tokio::test]
    async fn test_non_sequential_range_returns_error() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer)));

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();

        let result = writer
            .set_next_term_data_source(FileRange::new(10, 15), None, immediate_future(Bytes::from("World")))
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_first_range_must_start_at_zero() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer)));

        let result = writer
            .set_next_term_data_source(FileRange::new(5, 10), None, immediate_future(Bytes::from("Hello")))
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_semaphore_permit_released_after_write() {
        let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let buffer_clone = buffer.clone();
        let semaphore = Arc::new(Semaphore::new(2));

        let writer = Arc::new(SequentialWriter::new(Box::new(SharedBuffer(buffer_clone))));

        let permit1 = semaphore.clone().acquire_owned().await.unwrap();
        let permit2 = semaphore.clone().acquire_owned().await.unwrap();

        assert_eq!(semaphore.available_permits(), 0);

        writer
            .set_next_term_data_source(FileRange::new(0, 5), Some(permit1), immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(semaphore.available_permits(), 1);

        writer
            .set_next_term_data_source(FileRange::new(5, 6), Some(permit2), immediate_future(Bytes::from(" ")))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(semaphore.available_permits(), 2);

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"Hello ");
    }

    // ==================== Vectorized Writer Tests ====================

    #[tokio::test]
    async fn test_vectorized_basic_writes() {
        let test_writer = TestWriter::new(TestWriterConfig::vectorized());
        let buffer = test_writer.buffer.clone();
        let vectored_count = test_writer.vectored_write_count.clone();

        let writer = Arc::new(SequentialWriter::new_vectorized(Box::new(test_writer)));

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(5, 6), None, immediate_future(Bytes::from(" ")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(6, 11), None, immediate_future(Bytes::from("World")))
            .await
            .unwrap();

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"Hello World");
        assert!(vectored_count.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn test_vectorized_partial_writes() {
        let test_writer = TestWriter::new(TestWriterConfig::vectorized_partial(3));
        let buffer = test_writer.buffer.clone();

        let writer = Arc::new(SequentialWriter::new_vectorized(Box::new(test_writer)));

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(5, 6), None, immediate_future(Bytes::from(" ")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(6, 11), None, immediate_future(Bytes::from("World")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(11, 12), None, immediate_future(Bytes::from("!")))
            .await
            .unwrap();

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"Hello World!");
    }

    #[tokio::test]
    async fn test_vectorized_with_delays() {
        let test_writer = TestWriter::new(TestWriterConfig::vectorized());
        let buffer = test_writer.buffer.clone();

        let writer = Arc::new(SequentialWriter::new_vectorized(Box::new(test_writer)));

        // Create futures that resolve with different delays
        let f0: DataFuture = Box::pin(async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            Ok(Bytes::from("A"))
        });
        let f1: DataFuture = Box::pin(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(Bytes::from("B"))
        });
        let f2: DataFuture = Box::pin(async { Ok(Bytes::from("C")) });

        writer.set_next_term_data_source(FileRange::new(0, 1), None, f0).await.unwrap();
        writer.set_next_term_data_source(FileRange::new(1, 2), None, f1).await.unwrap();
        writer.set_next_term_data_source(FileRange::new(2, 3), None, f2).await.unwrap();

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"ABC");
    }

    #[tokio::test]
    async fn test_vectorized_many_small_writes() {
        let expected: Vec<u8> = (0..100u8).collect();
        let test_writer = TestWriter::new(TestWriterConfig::vectorized());
        let buffer = test_writer.buffer.clone();
        let vectored_count = test_writer.vectored_write_count.clone();

        let writer = Arc::new(SequentialWriter::new_vectorized(Box::new(test_writer)));

        // Write 100 single-byte chunks
        for i in 0..100u8 {
            writer
                .set_next_term_data_source(
                    FileRange::new(i as u64, i as u64 + 1),
                    None,
                    immediate_future(Bytes::from(vec![i])),
                )
                .await
                .unwrap();
        }

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, &expected);

        // Should have batched writes (fewer vectored calls than individual writes)
        let vectored_calls = vectored_count.load(Ordering::Relaxed);
        assert!(vectored_calls < 100);
    }

    #[tokio::test]
    async fn test_vectorized_with_interrupts() {
        let test_writer = TestWriter::new(TestWriterConfig::vectorized_with_interrupts());
        let buffer = test_writer.buffer.clone();

        let writer = Arc::new(SequentialWriter::new_vectorized(Box::new(test_writer)));

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(5, 6), None, immediate_future(Bytes::from(" ")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(6, 11), None, immediate_future(Bytes::from("World")))
            .await
            .unwrap();

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"Hello World");
    }

    #[tokio::test]
    async fn test_vectorized_permit_release() {
        let test_writer = TestWriter::new(TestWriterConfig::vectorized());
        let buffer = test_writer.buffer.clone();
        let semaphore = Arc::new(Semaphore::new(2));

        let writer = Arc::new(SequentialWriter::new_vectorized(Box::new(test_writer)));

        let permit1 = semaphore.clone().acquire_owned().await.unwrap();
        let permit2 = semaphore.clone().acquire_owned().await.unwrap();

        assert_eq!(semaphore.available_permits(), 0);

        writer
            .set_next_term_data_source(FileRange::new(0, 5), Some(permit1), immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(semaphore.available_permits(), 1);

        writer
            .set_next_term_data_source(FileRange::new(5, 6), Some(permit2), immediate_future(Bytes::from(" ")))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(semaphore.available_permits(), 2);

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"Hello ");
    }

    #[tokio::test]
    async fn test_vectorized_partial_permit_release() {
        let test_writer = TestWriter::new(TestWriterConfig::vectorized_partial(2));
        let buffer = test_writer.buffer.clone();
        let semaphore = Arc::new(Semaphore::new(3));

        let writer = Arc::new(SequentialWriter::new_vectorized(Box::new(test_writer)));

        let permit1 = semaphore.clone().acquire_owned().await.unwrap();
        let permit2 = semaphore.clone().acquire_owned().await.unwrap();
        let permit3 = semaphore.clone().acquire_owned().await.unwrap();

        assert_eq!(semaphore.available_permits(), 0);

        writer
            .set_next_term_data_source(FileRange::new(0, 5), Some(permit1), immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(5, 11), Some(permit2), immediate_future(Bytes::from(" World")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(11, 12), Some(permit3), immediate_future(Bytes::from("!")))
            .await
            .unwrap();

        writer.finish().await.unwrap();

        assert_eq!(semaphore.available_permits(), 3);

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"Hello World!");
    }

    #[tokio::test]
    async fn test_non_vectorized_basic_writes() {
        let test_writer = TestWriter::new(TestWriterConfig::default());
        let buffer = test_writer.buffer.clone();
        let write_count = test_writer.write_count.clone();
        let vectored_count = test_writer.vectored_write_count.clone();

        let writer = Arc::new(SequentialWriter::new(Box::new(test_writer)));

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(5, 6), None, immediate_future(Bytes::from(" ")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(6, 11), None, immediate_future(Bytes::from("World")))
            .await
            .unwrap();

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"Hello World");
        assert!(write_count.load(Ordering::Relaxed) > 0);
        assert_eq!(vectored_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_non_vectorized_partial_writes() {
        let test_writer = TestWriter::new(TestWriterConfig::partial(3));
        let buffer = test_writer.buffer.clone();

        let writer = Arc::new(SequentialWriter::new(Box::new(test_writer)));

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(5, 6), None, immediate_future(Bytes::from(" ")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(6, 11), None, immediate_future(Bytes::from("World")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(11, 12), None, immediate_future(Bytes::from("!")))
            .await
            .unwrap();

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"Hello World!");
    }

    #[tokio::test]
    async fn test_vectorized_single_byte_partial() {
        let test_writer = TestWriter::new(TestWriterConfig::vectorized_partial(1));
        let buffer = test_writer.buffer.clone();

        let writer = Arc::new(SequentialWriter::new_vectorized(Box::new(test_writer)));

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("ABCDE")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(5, 10), None, immediate_future(Bytes::from("FGHIJ")))
            .await
            .unwrap();

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"ABCDEFGHIJ");
    }

    #[tokio::test]
    async fn test_vectorized_large_data() {
        let expected: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        let test_writer = TestWriter::new(TestWriterConfig::vectorized());
        let buffer = test_writer.buffer.clone();

        let writer = Arc::new(SequentialWriter::new_vectorized(Box::new(test_writer)));

        // Write in chunks of 1000 bytes
        for i in 0..10 {
            let start = i * 1000;
            let end = start + 1000;
            let chunk: Vec<u8> = (start..end).map(|j| (j % 256) as u8).collect();
            writer
                .set_next_term_data_source(
                    FileRange::new(start as u64, end as u64),
                    None,
                    immediate_future(Bytes::from(chunk)),
                )
                .await
                .unwrap();
        }

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, &expected);
    }

    #[tokio::test]
    async fn test_vectorized_large_data_partial() {
        let expected: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();
        let test_writer = TestWriter::new(TestWriterConfig::vectorized_partial(100));
        let buffer = test_writer.buffer.clone();

        let writer = Arc::new(SequentialWriter::new_vectorized(Box::new(test_writer)));

        // Write in chunks of 500 bytes
        for i in 0..10 {
            let start = i * 500;
            let end = start + 500;
            let chunk: Vec<u8> = (start..end).map(|j| (j % 256) as u8).collect();
            writer
                .set_next_term_data_source(
                    FileRange::new(start as u64, end as u64),
                    None,
                    immediate_future(Bytes::from(chunk)),
                )
                .await
                .unwrap();
        }

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, &expected);
    }
}
