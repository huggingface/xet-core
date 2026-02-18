use bytes::Bytes;
use cas_types::FileRange;
use tokio::sync::mpsc::{self, UnboundedSender, unbounded_channel};
use tokio::sync::{Mutex, OwnedSemaphorePermit};
use tokio::task::JoinHandle;

use crate::data_writer::{DataFuture, DataWriter};
use crate::{FileReconstructionError, Result};

/// Item queued for ordered processing by the background task.
struct QueueItem {
    handle: JoinHandle<Result<Bytes>>,
    permit: Option<OwnedSemaphorePermit>,
}

/// All mutable state, behind a single mutex.
struct Inner {
    /// Sending side of the ordering queue. `None` after `finish()`.
    queue_tx: Option<UnboundedSender<QueueItem>>,
    /// Expected start position of the next term.
    next_position: u64,
    /// The background task that forwards chunks in order.
    background: Option<JoinHandle<Result<()>>>,
}

/// Writes data as a stream of `Bytes` chunks through an async channel.
///
/// Unlike `SequentialWriter`, this does not use a blocking thread or sync `Write`.
/// Data futures are resolved in parallel and their `Bytes` results are sent directly
/// through a bounded `mpsc::Sender<Bytes>`, preserving sequential order with zero
/// extra copies.
pub struct StreamingWriter {
    inner: Mutex<Inner>,
}

impl StreamingWriter {
    /// Creates a new streaming writer that sends `Bytes` through the given channel.
    pub fn new(output: mpsc::Sender<Bytes>) -> Self {
        let (queue_tx, queue_rx) = unbounded_channel::<QueueItem>();

        let handle = tokio::spawn(Self::background_task(queue_rx, output));

        Self {
            inner: Mutex::new(Inner {
                queue_tx: Some(queue_tx),
                next_position: 0,
                background: Some(handle),
            }),
        }
    }

    /// Background task that awaits each spawned data future in queue order
    /// and forwards the resolved `Bytes` through the bounded output channel.
    async fn background_task(mut rx: mpsc::UnboundedReceiver<QueueItem>, tx: mpsc::Sender<Bytes>) -> Result<()> {
        while let Some(item) = rx.recv().await {
            let data = item
                .handle
                .await
                .map_err(|e| FileReconstructionError::InternalError(format!("Task join error: {e}")))??;
            tx.send(data)
                .await
                .map_err(|_| FileReconstructionError::InternalWriterError("Output channel closed".to_string()))?;
            drop(item.permit);
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
        let mut inner = self.inner.lock().await;

        if inner.queue_tx.is_none() {
            return Err(FileReconstructionError::InternalWriterError("Writer has already finished".to_string()));
        }

        if byte_range.start != inner.next_position {
            return Err(FileReconstructionError::InternalWriterError(format!(
                "Byte range not sequential: expected start at {}, got {}",
                inner.next_position, byte_range.start
            )));
        }

        let expected_size = byte_range.end - byte_range.start;
        inner.next_position = byte_range.end;

        let handle = tokio::spawn(async move {
            let data = data_future.await?;
            if data.len() as u64 != expected_size {
                return Err(FileReconstructionError::InternalWriterError(format!(
                    "Data size mismatch: expected {} bytes, got {} bytes",
                    expected_size,
                    data.len()
                )));
            }
            Ok(data)
        });

        inner
            .queue_tx
            .as_ref()
            .unwrap()
            .send(QueueItem { handle, permit })
            .map_err(|_| {
                FileReconstructionError::InternalWriterError("Background writer channel closed".to_string())
            })?;

        Ok(())
    }

    async fn finish(&self) -> Result<u64> {
        let (handle, total_bytes) = {
            let mut inner = self.inner.lock().await;

            if inner.queue_tx.is_none() {
                return Err(FileReconstructionError::InternalWriterError("Writer has already finished".to_string()));
            }

            // Drop the sender — the background task will drain remaining items then exit.
            inner.queue_tx.take();

            (inner.background.take().expect("background task missing"), inner.next_position)
        };

        // The background task awaits each JoinHandle in order, so joining it
        // implicitly waits for all spawned data-fetching tasks to complete.
        handle.await.map_err(|e| {
            FileReconstructionError::InternalWriterError(format!("Background writer task failed: {e}"))
        })??;

        Ok(total_bytes)
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
