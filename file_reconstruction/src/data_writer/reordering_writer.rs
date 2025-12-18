use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU64};

use bytes::Bytes;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::data_writer::DataWriter;

struct QueuedWriteObject {
    position: u64,
    data: Bytes,
    is_final: bool,
}

impl Ord for QueuedWriteObject {
    fn cmp(&self, other: &Self) -> Ordering {
        other.position.cmp(&self.position)
    }
}

impl PartialOrd for QueuedWriteObject {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for QueuedWriteObject {
    fn eq(&self, other: &Self) -> bool {
        self.position == other.position
    }
}

impl Eq for QueuedWriteObject {}

struct ReorderingState {
    reorder_buffer: BinaryHeap<QueuedWriteObject>,
    next_write_position: u64,
    ordered_queue_tx: UnboundedSender<QueuedWriteObject>,
}

impl ReorderingState {
    fn new(ordered_queue_tx: UnboundedSender<QueuedWriteObject>) -> Self {
        Self {
            reorder_buffer: BinaryHeap::new(),
            next_write_position: 0,
            ordered_queue_tx,
        }
    }

    fn enqueue(&mut self, obj: QueuedWriteObject) -> io::Result<()> {
        if obj.position == self.next_write_position {
            self.next_write_position += obj.data.len() as u64;
            self.ordered_queue_tx
                .send(obj)
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Background writer channel closed"))?;
            self.flush_reorder_buffer()?;
        } else {
            self.reorder_buffer.push(obj);
        }
        Ok(())
    }

    fn flush_reorder_buffer(&mut self) -> io::Result<()> {
        while let Some(next) = self.reorder_buffer.peek() {
            if next.position == self.next_write_position {
                let obj = self.reorder_buffer.pop().unwrap();
                self.next_write_position += obj.data.len() as u64;
                self.ordered_queue_tx
                    .send(obj)
                    .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Background writer channel closed"))?;
            } else {
                break;
            }
        }
        Ok(())
    }
}

pub struct ReorderingWriter {
    state: Mutex<Option<ReorderingState>>,
    writer: Mutex<Option<Box<dyn Write + Send>>>,
    background_handle: Mutex<Option<tokio::task::JoinHandle<io::Result<()>>>>,
    finished: AtomicBool,
    total_bytes_written: AtomicU64,
}

impl ReorderingWriter {
    pub fn new(writer: Box<dyn Write + Send>) -> Self {
        Self {
            state: Mutex::new(None),
            writer: Mutex::new(Some(writer)),
            background_handle: Mutex::new(None),
            finished: AtomicBool::new(false),
            total_bytes_written: AtomicU64::new(0),
        }
    }

    async fn spawn_background_writer(
        mut rx: UnboundedReceiver<QueuedWriteObject>,
        mut writer: Box<dyn Write + Send>,
    ) -> io::Result<()> {
        tokio::task::spawn_blocking(move || {
            while let Some(obj) = rx.blocking_recv() {
                writer.write_all(&obj.data)?;
                if obj.is_final {
                    writer.flush()?;
                    break;
                }
            }
            Ok(())
        })
        .await
        .map_err(io::Error::other)?
    }
}

#[async_trait::async_trait]
impl DataWriter for ReorderingWriter {
    async fn begin(&self) -> io::Result<()> {
        if self.finished.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Writer has already finished"));
        }

        let mut state_guard = self.state.lock().await;
        if state_guard.is_some() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Writer has already begun"));
        }

        let (tx, rx) = mpsc::unbounded_channel::<QueuedWriteObject>();

        let writer = self
            .writer
            .lock()
            .await
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Writer has already been taken"))?;

        let handle = tokio::spawn(Self::spawn_background_writer(rx, writer));

        *state_guard = Some(ReorderingState::new(tx));
        *self.background_handle.lock().await = Some(handle);

        Ok(())
    }

    async fn enqueue_write(&self, position: u64, data: Bytes) -> io::Result<()> {
        if self.finished.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Writer has already finished"));
        }

        let mut state_guard = self.state.lock().await;
        let state = state_guard.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Writer has not been initialized with begin()")
        })?;

        self.total_bytes_written
            .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);

        state.enqueue(QueuedWriteObject {
            position,
            data,
            is_final: false,
        })
    }

    async fn finish(&self) -> io::Result<()> {
        if self.finished.swap(true, std::sync::atomic::Ordering::Relaxed) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Writer has already finished"));
        }

        let mut state_guard = self.state.lock().await;
        let state = state_guard.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "Writer has not been initialized with begin()")
        })?;

        if !state.reorder_buffer.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Gaps in write operations: expected position {}, but reorder buffer still has {} items",
                    state.next_write_position,
                    state.reorder_buffer.len()
                ),
            ));
        }

        let final_marker = QueuedWriteObject {
            position: state.next_write_position,
            data: Bytes::new(),
            is_final: true,
        };
        state
            .ordered_queue_tx
            .send(final_marker)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Background writer channel closed"))?;

        drop(state);

        let handle = self
            .background_handle
            .lock()
            .await
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Background writer not running"))?;

        handle.await.map_err(io::Error::other)??;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sequential_writes() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = ReorderingWriter::new(Box::new(buffer));

        writer.begin().await.unwrap();
        writer.enqueue_write(0, Bytes::from("Hello")).await.unwrap();
        writer.enqueue_write(5, Bytes::from(" ")).await.unwrap();
        writer.enqueue_write(6, Bytes::from("World")).await.unwrap();
        writer.finish().await.unwrap();
    }

    #[tokio::test]
    async fn test_out_of_order_writes() {
        let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let buffer_clone = buffer.clone();

        struct SharedBuffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl Write for SharedBuffer {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let writer = ReorderingWriter::new(Box::new(SharedBuffer(buffer_clone)));

        writer.begin().await.unwrap();
        writer.enqueue_write(6, Bytes::from("World")).await.unwrap();
        writer.enqueue_write(5, Bytes::from(" ")).await.unwrap();
        writer.enqueue_write(0, Bytes::from("Hello")).await.unwrap();
        writer.finish().await.unwrap();

        let result = buffer.lock().unwrap();
        assert_eq!(&*result, b"Hello World");
    }

    #[tokio::test]
    async fn test_gap_detection() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = ReorderingWriter::new(Box::new(buffer));

        writer.begin().await.unwrap();
        writer.enqueue_write(0, Bytes::from("Hello")).await.unwrap();
        writer.enqueue_write(10, Bytes::from("World")).await.unwrap();

        let result = writer.finish().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cannot_write_after_finish() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = ReorderingWriter::new(Box::new(buffer));

        writer.begin().await.unwrap();
        writer.enqueue_write(0, Bytes::from("Test")).await.unwrap();
        writer.finish().await.unwrap();

        let result = writer.enqueue_write(4, Bytes::from("More")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cannot_begin_twice() {
        let buffer = std::io::Cursor::new(Vec::new());
        let writer = ReorderingWriter::new(Box::new(buffer));

        writer.begin().await.unwrap();
        let result = writer.begin().await;
        assert!(result.is_err());
    }
}
