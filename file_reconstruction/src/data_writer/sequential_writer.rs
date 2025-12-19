use std::io::Write;
use std::sync::Arc;

use bytes::Bytes;
use cas_types::FileRange;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, oneshot};
use tokio::task::{JoinHandle, spawn_blocking};

use crate::data_writer::{DataReceptacle, DataWriter};
use crate::{ErrorState, FileReconstructionError, Result};

enum QueueItem {
    Data {
        #[allow(dead_code)]
        write_position: u64,
        receiver: oneshot::Receiver<Bytes>,
    },
    Finish,
}

struct WritingQueueState {
    sender: UnboundedSender<QueueItem>,
    next_term_index: usize,
    next_write_position: u64,
    finished: bool,
}

pub struct SequentialWriterDataReceptacle {
    term_index: usize,
    expected_size: u64,
    sender: oneshot::Sender<Bytes>,
}

#[async_trait::async_trait]
impl DataReceptacle for SequentialWriterDataReceptacle {
    async fn write(self: Box<Self>, data: Bytes) -> Result<()> {
        if data.len() as u64 != self.expected_size {
            return Err(FileReconstructionError::InternalWriterError(format!(
                "Data size mismatch for term {}: expected {} bytes, got {} bytes",
                self.term_index,
                self.expected_size,
                data.len()
            )));
        }

        self.sender.send(data).map_err(|_| {
            FileReconstructionError::InternalWriterError(format!(
                "Failed to send data for term {}: receiver dropped",
                self.term_index
            ))
        })
    }
}

pub struct SequentialWriter {
    queue_state: Mutex<WritingQueueState>,
    background_handle: Mutex<Option<JoinHandle<()>>>,
    error_state: Arc<ErrorState>,
}

impl SequentialWriter {
    pub fn new(writer: Box<dyn Write + Send>, start_write_position: u64, start_term_index: usize) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<QueueItem>();
        let error_state = Arc::new(ErrorState::new());

        let error_state_clone = error_state.clone();
        let handle = spawn_blocking(move || {
            Self::background_writer(rx, writer, error_state_clone);
        });

        let writing_queue_state = WritingQueueState {
            sender: tx,
            next_term_index: start_term_index,
            next_write_position: start_write_position,
            finished: false,
        };

        Self {
            queue_state: Mutex::new(writing_queue_state),
            background_handle: Mutex::new(Some(handle)),
            error_state,
        }
    }

    fn background_writer(
        mut rx: UnboundedReceiver<QueueItem>,
        mut writer: Box<dyn Write + Send>,
        error_state: Arc<ErrorState>,
    ) {
        let mut run = move || -> Result<()> {
            while let Some(item) = rx.blocking_recv() {
                match item {
                    QueueItem::Data { receiver, .. } => match receiver.blocking_recv() {
                        Ok(data) => {
                            writer.write_all(&data)?;
                        },
                        Err(_) => {
                            return Err(FileReconstructionError::InternalWriterError(
                                "Data sender was dropped before sending finish message.".to_string(),
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
    async fn get_data_receptacle(
        &self,
        term_index: usize,
        term_byte_range: FileRange,
    ) -> Result<Box<dyn DataReceptacle>> {
        self.error_state.check()?;

        let mut state = self.queue_state.lock().await;

        if state.finished {
            return Err(FileReconstructionError::InternalWriterError("Writer has already finished".to_string()));
        }

        let expected_term_index = state.next_term_index;
        if term_index != expected_term_index {
            return Err(FileReconstructionError::InternalWriterError(format!(
                "Term indices must be sequential: expected {}, got {}",
                expected_term_index, term_index
            )));
        }

        let expected_size = term_byte_range.end - term_byte_range.start;
        let write_position = state.next_write_position;

        state.next_term_index += 1;
        state.next_write_position += expected_size;

        let (sender, receiver) = oneshot::channel();

        state
            .sender
            .send(QueueItem::Data {
                write_position,
                receiver,
            })
            .map_err(|_| {
                FileReconstructionError::InternalWriterError("Background writer channel closed".to_string())
            })?;

        Ok(Box::new(SequentialWriterDataReceptacle {
            term_index,
            expected_size,
            sender,
        }))
    }

    async fn finish(&self) -> Result<()> {
        self.error_state.check()?;

        {
            let mut state = self.queue_state.lock().await;

            if state.finished {
                return Err(FileReconstructionError::InternalWriterError("Writer has already finished".to_string()));
            }

            state.finished = true;

            state.sender.send(QueueItem::Finish).map_err(|_| {
                FileReconstructionError::InternalWriterError("Background writer channel closed".to_string())
            })?;
        }

        let handle =
            self.background_handle.lock().await.take().ok_or_else(|| {
                FileReconstructionError::InternalWriterError("Background writer not running".to_string())
            })?;

        handle
            .await
            .map_err(|e| FileReconstructionError::InternalWriterError(format!("Background writer task failed: {e}")))?;

        self.error_state.check()?;

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

        let writer = Arc::new(SequentialWriter::new(Box::new(SharedBuffer(buffer_clone)), 0, 0));

        let r0 = writer.get_data_receptacle(0, FileRange::new(0, 5)).await.unwrap();
        let r1 = writer.get_data_receptacle(1, FileRange::new(5, 6)).await.unwrap();
        let r2 = writer.get_data_receptacle(2, FileRange::new(6, 11)).await.unwrap();

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

        let writer = Arc::new(SequentialWriter::new(Box::new(SharedBuffer(buffer_clone)), 0, 0));

        let r0 = writer.get_data_receptacle(0, FileRange::new(0, 5)).await.unwrap();
        let r1 = writer.get_data_receptacle(1, FileRange::new(5, 6)).await.unwrap();
        let r2 = writer.get_data_receptacle(2, FileRange::new(6, 11)).await.unwrap();

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
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer), 0, 0));

        let r0 = writer.get_data_receptacle(0, FileRange::new(0, 10)).await.unwrap();

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

        let writer = Arc::new(SequentialWriter::new(Box::new(FailingWriter), 0, 0));

        let r0 = writer.get_data_receptacle(0, FileRange::new(0, 4)).await.unwrap();
        r0.write(Bytes::from("Test")).await.unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;

        let result = writer.get_data_receptacle(1, FileRange::new(4, 8)).await;

        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::IoError(_))));
    }

    #[tokio::test]
    async fn test_sequential_term_index_required() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer), 0, 0));

        let result = writer.get_data_receptacle(1, FileRange::new(0, 5)).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_start_from_nonzero() {
        let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let buffer_clone = buffer.clone();

        let writer = Arc::new(SequentialWriter::new(Box::new(SharedBuffer(buffer_clone)), 100, 5));

        let r0 = writer.get_data_receptacle(5, FileRange::new(100, 105)).await.unwrap();
        let r1 = writer.get_data_receptacle(6, FileRange::new(105, 110)).await.unwrap();

        r0.write(Bytes::from("Hello")).await.unwrap();
        r1.write(Bytes::from("World")).await.unwrap();

        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"HelloWorld");
    }

    #[tokio::test]
    async fn test_finish_twice_returns_error() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer), 0, 0));

        writer.finish().await.unwrap();
        let result = writer.finish().await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_get_receptacle_after_finish_returns_error() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer), 0, 0));

        writer.finish().await.unwrap();
        let result = writer.get_data_receptacle(0, FileRange::new(0, 5)).await;
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

        let writer = Arc::new(SequentialWriter::new(Box::new(FlushFailingWriter), 0, 0));
        let result = writer.finish().await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::IoError(_))));
    }

    #[tokio::test]
    async fn test_dropped_receptacle_causes_error() {
        let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let buffer_clone = buffer.clone();

        let writer = Arc::new(SequentialWriter::new(Box::new(SharedBuffer(buffer_clone)), 0, 0));

        let r0 = writer.get_data_receptacle(0, FileRange::new(0, 5)).await.unwrap();
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

        let writer = Arc::new(SequentialWriter::new(Box::new(FailingWriter), 0, 0));

        let r0 = writer.get_data_receptacle(0, FileRange::new(0, 4)).await.unwrap();
        r0.write(Bytes::from("Test")).await.unwrap();

        let result = writer.finish().await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::IoError(_))));
    }

    #[tokio::test]
    async fn test_size_mismatch_too_small() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer), 0, 0));

        let r0 = writer.get_data_receptacle(0, FileRange::new(0, 10)).await.unwrap();
        let result = r0.write(Bytes::from("Hi")).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_size_mismatch_too_large() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = Arc::new(SequentialWriter::new(Box::new(buffer), 0, 0));

        let r0 = writer.get_data_receptacle(0, FileRange::new(0, 2)).await.unwrap();
        let result = r0.write(Bytes::from("Hello World")).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }
}
