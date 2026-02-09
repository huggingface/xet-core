use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use cas_types::FileRange;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{Mutex, OwnedSemaphorePermit, oneshot};
use tokio::task::{JoinHandle, JoinSet};

use crate::data_writer::{DataFuture, DataWriter};
use crate::{ErrorState, FileReconstructionError, Result};

/// Items sent to the background async task for ordered processing.
enum QueueItem {
    Data {
        receiver: oneshot::Receiver<Bytes>,
        permit: Option<OwnedSemaphorePermit>,
    },
    Finish,
}

/// Mutable state for the streaming queue, protected by a mutex.
struct QueueState {
    sender: UnboundedSender<QueueItem>,
    next_position: u64,
    finished: bool,
}

/// Writes data as a stream of `Bytes` chunks through an async channel.
///
/// Unlike `SequentialWriter`, this does not use a blocking thread or sync `Write`.
/// Data futures are resolved in parallel and their `Bytes` results are sent directly
/// through a bounded `mpsc::Sender<Bytes>`, preserving sequential order with zero
/// extra copies.
pub struct StreamingWriter {
    queue_state: Mutex<QueueState>,
    background_handle: Mutex<Option<JoinHandle<Result<()>>>>,
    error_state: Arc<ErrorState>,
    bytes_written: Arc<AtomicU64>,
    active_tasks: Arc<Mutex<JoinSet<Result<()>>>>,
}

impl StreamingWriter {
    /// Creates a new streaming writer that sends `Bytes` through the given channel.
    pub fn new(output: mpsc::Sender<Bytes>) -> Self {
        let (tx, rx) = unbounded_channel::<QueueItem>();
        let error_state = Arc::new(ErrorState::new());
        let bytes_written = Arc::new(AtomicU64::new(0));

        let bytes_written_clone = bytes_written.clone();

        let handle = tokio::spawn(Self::background_task(rx, output, bytes_written_clone));

        Self {
            queue_state: Mutex::new(QueueState {
                sender: tx,
                next_position: 0,
                finished: false,
            }),
            background_handle: Mutex::new(Some(handle)),
            error_state,
            bytes_written,
            active_tasks: Arc::new(Mutex::new(JoinSet::new())),
        }
    }

    /// Background async task that processes queue items in order and sends
    /// resolved `Bytes` through the bounded output channel.
    async fn background_task(
        mut rx: UnboundedReceiver<QueueItem>,
        tx: mpsc::Sender<Bytes>,
        bytes_written: Arc<AtomicU64>,
    ) -> Result<()> {
        while let Some(item) = rx.recv().await {
            match item {
                QueueItem::Data { receiver, permit } => {
                    let data = receiver.await.map_err(|_| {
                        FileReconstructionError::InternalWriterError(
                            "Data sender was dropped before sending data.".to_string(),
                        )
                    })?;
                    let len = data.len() as u64;
                    tx.send(data).await.map_err(|_| {
                        FileReconstructionError::InternalWriterError("Output channel closed".to_string())
                    })?;
                    bytes_written.fetch_add(len, Ordering::Relaxed);
                    drop(permit);
                },
                QueueItem::Finish => break,
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl DataWriter for StreamingWriter {
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

        handle.await.map_err(|e| {
            FileReconstructionError::InternalWriterError(format!("Background writer task failed: {e}"))
        })??;

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use cas_types::FileRange;
    use tokio::sync::{Semaphore, mpsc};

    use super::*;

    fn immediate_future(data: Bytes) -> DataFuture {
        Box::pin(async move { Ok(data) })
    }

    /// Helper: create a StreamingWriter + receiver, run the test body, then
    /// collect all received chunks into a Vec<u8>. The receiver is drained
    /// concurrently with `finish()` to avoid deadlocking on a full channel.
    async fn run_streaming<F, Fut>(buffer_size: usize, f: F) -> (Vec<u8>, u64)
    where
        F: FnOnce(Arc<StreamingWriter>) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let (tx, mut rx) = mpsc::channel(buffer_size);
        let writer = Arc::new(StreamingWriter::new(tx));

        f(writer.clone()).await;

        let collect_handle = tokio::spawn(async move {
            let mut collected = Vec::new();
            while let Some(chunk) = rx.recv().await {
                collected.extend_from_slice(&chunk);
            }
            collected
        });

        let bytes_written = writer.finish().await.unwrap();
        let collected = collect_handle.await.unwrap();
        (collected, bytes_written)
    }

    #[tokio::test]
    async fn test_sequential_writes() {
        let (result, n) = run_streaming(16, |w| async move {
            w.set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
                .await
                .unwrap();
            w.set_next_term_data_source(FileRange::new(5, 6), None, immediate_future(Bytes::from(" ")))
                .await
                .unwrap();
            w.set_next_term_data_source(FileRange::new(6, 11), None, immediate_future(Bytes::from("World")))
                .await
                .unwrap();
        })
        .await;

        assert_eq!(result, b"Hello World");
        assert_eq!(n, 11);
    }

    #[tokio::test]
    async fn test_delayed_futures_preserve_order() {
        let (tx, mut rx) = mpsc::channel(16);
        let writer = Arc::new(StreamingWriter::new(tx));

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

        let mut collected = Vec::new();
        while let Some(chunk) = rx.recv().await {
            collected.extend_from_slice(&chunk);
        }
        assert_eq!(collected, b"Hello World");
    }

    #[tokio::test]
    async fn test_non_sequential_range_returns_error() {
        let (tx, _rx) = mpsc::channel(16);
        let writer = Arc::new(StreamingWriter::new(tx));

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();

        let result = writer
            .set_next_term_data_source(FileRange::new(10, 15), None, immediate_future(Bytes::from("World")))
            .await;
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_first_range_must_start_at_zero() {
        let (tx, _rx) = mpsc::channel(16);
        let writer = Arc::new(StreamingWriter::new(tx));

        let result = writer
            .set_next_term_data_source(FileRange::new(5, 10), None, immediate_future(Bytes::from("Hello")))
            .await;
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_size_mismatch_error() {
        let (tx, _rx) = mpsc::channel(16);
        let writer = Arc::new(StreamingWriter::new(tx));

        writer
            .set_next_term_data_source(FileRange::new(0, 10), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();

        let result = writer.finish().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_finish_twice_returns_error() {
        let (tx, _rx) = mpsc::channel(16);
        let writer = Arc::new(StreamingWriter::new(tx));

        writer.finish().await.unwrap();
        let result = writer.finish().await;
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_write_after_finish_returns_error() {
        let (tx, _rx) = mpsc::channel(16);
        let writer = Arc::new(StreamingWriter::new(tx));

        writer.finish().await.unwrap();
        let result = writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
            .await;
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_future_error_propagates() {
        let (tx, _rx) = mpsc::channel(16);
        let writer = Arc::new(StreamingWriter::new(tx));

        let failing: DataFuture = Box::pin(async { Err(FileReconstructionError::InternalError("boom".to_string())) });

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, failing)
            .await
            .unwrap();

        let result = writer.finish().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bytes_written_tracking() {
        let (result, n) = run_streaming(16, |w| async move {
            w.set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
                .await
                .unwrap();
            w.set_next_term_data_source(FileRange::new(5, 11), None, immediate_future(Bytes::from(" World")))
                .await
                .unwrap();
            w.set_next_term_data_source(FileRange::new(11, 16), None, immediate_future(Bytes::from("!!!!!")))
                .await
                .unwrap();
        })
        .await;

        assert_eq!(result, b"Hello World!!!!!");
        assert_eq!(n, 16);
    }

    #[tokio::test]
    async fn test_semaphore_permit_released_after_write() {
        let (tx, mut rx) = mpsc::channel(16);
        let writer = Arc::new(StreamingWriter::new(tx));
        let semaphore = Arc::new(Semaphore::new(2));

        let permit1 = semaphore.clone().acquire_owned().await.unwrap();
        let permit2 = semaphore.clone().acquire_owned().await.unwrap();

        assert_eq!(semaphore.available_permits(), 0);

        writer
            .set_next_term_data_source(FileRange::new(0, 5), Some(permit1), immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();

        // Drain the channel so the background task can make progress.
        let _ = rx.recv().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(semaphore.available_permits(), 1);

        writer
            .set_next_term_data_source(FileRange::new(5, 6), Some(permit2), immediate_future(Bytes::from(" ")))
            .await
            .unwrap();

        let _ = rx.recv().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(semaphore.available_permits(), 2);

        writer.finish().await.unwrap();
    }

    #[tokio::test]
    async fn test_many_small_writes() {
        let expected: Vec<u8> = (0..100u8).collect();

        let (result, n) = run_streaming(16, |w| async move {
            for i in 0..100u8 {
                w.set_next_term_data_source(
                    FileRange::new(i as u64, i as u64 + 1),
                    None,
                    immediate_future(Bytes::from(vec![i])),
                )
                .await
                .unwrap();
            }
        })
        .await;

        assert_eq!(result, expected);
        assert_eq!(n, 100);
    }

    #[tokio::test]
    async fn test_chunks_arrive_on_receiver() {
        let (tx, mut rx) = mpsc::channel(2);
        let writer = Arc::new(StreamingWriter::new(tx));

        writer
            .set_next_term_data_source(FileRange::new(0, 3), None, immediate_future(Bytes::from("abc")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(3, 6), None, immediate_future(Bytes::from("def")))
            .await
            .unwrap();

        // Chunks should be receivable before finish.
        let chunk1 = rx.recv().await.unwrap();
        let chunk2 = rx.recv().await.unwrap();
        assert_eq!(chunk1, Bytes::from("abc"));
        assert_eq!(chunk2, Bytes::from("def"));

        writer.finish().await.unwrap();
    }
}
