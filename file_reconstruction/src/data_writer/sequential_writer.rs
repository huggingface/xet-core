use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use cas_types::FileRange;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{Mutex, OwnedSemaphorePermit, oneshot};
use tokio::task::{JoinHandle, spawn_blocking};

use crate::data_writer::{DataReceptacle, DataWriter};
use crate::{ErrorState, FileReconstructionError, Result};

enum QueueItem {
    Data {
        receiver: oneshot::Receiver<Bytes>,
        permit: Option<OwnedSemaphorePermit>,
    },
    Finish,
}

struct WritingQueueState {
    sender: UnboundedSender<QueueItem>,
    next_position: u64,
    finished: bool,
}

pub struct SequentialWriterDataReceptacle {
    expected_size: u64,
    sender: oneshot::Sender<Bytes>,
}

#[async_trait::async_trait]
impl DataReceptacle for SequentialWriterDataReceptacle {
    async fn write(self: Box<Self>, data: Bytes) -> Result<()> {
        if data.len() as u64 != self.expected_size {
            return Err(FileReconstructionError::InternalWriterError(format!(
                "Data size mismatch: expected {} bytes, got {} bytes",
                self.expected_size,
                data.len()
            )));
        }

        self.sender.send(data).map_err(|_| {
            FileReconstructionError::InternalWriterError("Failed to send data: receiver dropped".to_string())
        })
    }
}

pub struct SequentialWriter {
    queue_state: Mutex<WritingQueueState>,
    background_handle: Mutex<Option<JoinHandle<()>>>,
    error_state: Arc<ErrorState>,
    bytes_written: Arc<AtomicU64>,
}

impl SequentialWriter {
    pub fn new(writer: Box<dyn Write + Send>) -> Self {
        let (tx, rx) = unbounded_channel::<QueueItem>();
        let error_state = Arc::new(ErrorState::new());
        let bytes_written = Arc::new(AtomicU64::new(0));

        let error_state_clone = error_state.clone();
        let bytes_written_clone = bytes_written.clone();
        let handle = spawn_blocking(move || {
            Self::background_writer(rx, writer, error_state_clone, bytes_written_clone);
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
        }
    }

    /// The background writer task that reads data from the channel synchronously and
    /// writes it to the output writer.
    fn background_writer(
        mut rx: UnboundedReceiver<QueueItem>,
        mut writer: Box<dyn Write + Send>,
        error_state: Arc<ErrorState>,
        bytes_written: Arc<AtomicU64>,
    ) {
        let mut run = move || -> Result<()> {
            while let Some(item) = rx.blocking_recv() {
                match item {
                    QueueItem::Data { receiver, permit } => match receiver.blocking_recv() {
                        Ok(data) => {
                            let len = data.len() as u64;
                            writer.write_all(&data)?;
                            bytes_written.fetch_add(len, Ordering::Relaxed);
                            drop(permit);
                        },
                        Err(_) => {
                            return Err(FileReconstructionError::InternalWriterError(
                                "Data sender was dropped before sending data.".to_string(),
                            ));
                        },
                    },
                    QueueItem::Finish => {
                        writer.flush()?;
                        return Ok(());
                    },
                }
            }
            Ok(())
        };

        if let Err(err) = run() {
            error_state.set(err);
        }
    }
}

#[async_trait::async_trait]
impl DataWriter for SequentialWriter {
    async fn next_data_receptacle(
        &self,
        byte_range: FileRange,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Result<Box<dyn DataReceptacle>> {
        self.error_state.check()?;

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

        Ok(Box::new(SequentialWriterDataReceptacle { expected_size, sender }))
    }

    async fn finish(&self) -> Result<()> {
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

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::time::Duration;

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

    #[tokio::test]
    async fn test_sequential_writes() {
        let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let buffer_clone = buffer.clone();

        let writer = Arc::new(SequentialWriter::new(Box::new(SharedBuffer(buffer_clone))));

        let r0 = writer.next_data_receptacle(FileRange::new(0, 5), None).await.unwrap();
        let r1 = writer.next_data_receptacle(FileRange::new(5, 6), None).await.unwrap();
        let r2 = writer.next_data_receptacle(FileRange::new(6, 11), None).await.unwrap();

        r0.write(Bytes::from("Hello")).await.unwrap();
        r1.write(Bytes::from(" ")).await.unwrap();
        r2.write(Bytes::from("World")).await.unwrap();

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"Hello World");
    }

    #[tokio::test]
    async fn test_out_of_order_fill() {
        let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let buffer_clone = buffer.clone();

        let writer = Arc::new(SequentialWriter::new(Box::new(SharedBuffer(buffer_clone))));

        let r0 = writer.next_data_receptacle(FileRange::new(0, 5), None).await.unwrap();
        let r1 = writer.next_data_receptacle(FileRange::new(5, 6), None).await.unwrap();
        let r2 = writer.next_data_receptacle(FileRange::new(6, 11), None).await.unwrap();

        r2.write(Bytes::from("World")).await.unwrap();
        r0.write(Bytes::from("Hello")).await.unwrap();
        r1.write(Bytes::from(" ")).await.unwrap();

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"Hello World");
    }

    #[tokio::test]
    async fn test_size_mismatch_error() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer)));

        let r0 = writer.next_data_receptacle(FileRange::new(0, 10), None).await.unwrap();

        let result = r0.write(Bytes::from("Hello")).await;
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

        let r0 = writer.next_data_receptacle(FileRange::new(0, 4), None).await.unwrap();
        r0.write(Bytes::from("Test")).await.unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;

        let result = writer.next_data_receptacle(FileRange::new(4, 8), None).await;

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
    async fn test_get_receptacle_after_finish_returns_error() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer)));

        writer.finish().await.unwrap();
        let result = writer.next_data_receptacle(FileRange::new(0, 5), None).await;
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
    async fn test_dropped_receptacle_causes_error() {
        let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let buffer_clone = buffer.clone();

        let writer = Arc::new(SequentialWriter::new(Box::new(SharedBuffer(buffer_clone))));

        let r0 = writer.next_data_receptacle(FileRange::new(0, 5), None).await.unwrap();
        drop(r0);

        let result = writer.finish().await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_error_propagates_to_finish() {
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

        let r0 = writer.next_data_receptacle(FileRange::new(0, 4), None).await.unwrap();
        r0.write(Bytes::from("Test")).await.unwrap();

        let result = writer.finish().await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::IoError(_))));
    }

    #[tokio::test]
    async fn test_size_mismatch_too_small() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer)));

        let r0 = writer.next_data_receptacle(FileRange::new(0, 10), None).await.unwrap();
        let result = r0.write(Bytes::from("Hi")).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_size_mismatch_too_large() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer)));

        let r0 = writer.next_data_receptacle(FileRange::new(0, 2), None).await.unwrap();
        let result = r0.write(Bytes::from("Hello World")).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_bytes_written_tracking() {
        let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let buffer_clone = buffer.clone();

        let writer = Arc::new(SequentialWriter::new(Box::new(SharedBuffer(buffer_clone))));

        let r0 = writer.next_data_receptacle(FileRange::new(0, 5), None).await.unwrap();
        let r1 = writer.next_data_receptacle(FileRange::new(5, 11), None).await.unwrap();
        let r2 = writer.next_data_receptacle(FileRange::new(11, 16), None).await.unwrap();

        r0.write(Bytes::from("Hello")).await.unwrap();
        r1.write(Bytes::from(" World")).await.unwrap();
        r2.write(Bytes::from("!!!!!")).await.unwrap();

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"Hello World!!!!!");
        assert_eq!(result.len(), 16);
    }

    #[tokio::test]
    async fn test_non_sequential_range_returns_error() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer)));

        writer.next_data_receptacle(FileRange::new(0, 5), None).await.unwrap();

        let result = writer.next_data_receptacle(FileRange::new(10, 15), None).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_first_range_must_start_at_zero() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer)));

        let result = writer.next_data_receptacle(FileRange::new(5, 10), None).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_semaphore_permit_released_after_write() {
        use tokio::sync::Semaphore;

        let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let buffer_clone = buffer.clone();
        let semaphore = Arc::new(Semaphore::new(2));

        let writer = Arc::new(SequentialWriter::new(Box::new(SharedBuffer(buffer_clone))));

        let permit1 = semaphore.clone().acquire_owned().await.unwrap();
        let permit2 = semaphore.clone().acquire_owned().await.unwrap();

        assert_eq!(semaphore.available_permits(), 0);

        let r0 = writer.next_data_receptacle(FileRange::new(0, 5), Some(permit1)).await.unwrap();
        let r1 = writer.next_data_receptacle(FileRange::new(5, 6), Some(permit2)).await.unwrap();

        r0.write(Bytes::from("Hello")).await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(semaphore.available_permits(), 1);

        r1.write(Bytes::from(" ")).await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(semaphore.available_permits(), 2);

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"Hello ");
    }
}
