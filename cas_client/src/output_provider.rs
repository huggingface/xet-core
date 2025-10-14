use std::io::{Cursor, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::AsyncWrite;

use crate::CasClientError;
use crate::error::Result;

pub enum SequentialOutput {
    Sync(Option<Box<dyn Write + Send>>),
    Async(Box<dyn AsyncWrite + Send + Unpin>),
}

impl AsyncWrite for SequentialOutput {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            SequentialOutput::Sync(Some(w)) => Poll::Ready(w.write(buf)),
            // doneness is signaled by the writer being dropped.
            SequentialOutput::Sync(None) => Poll::Ready(Ok(0)),
            SequentialOutput::Async(w) => std::pin::pin!(w).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            SequentialOutput::Sync(Some(w)) => Poll::Ready(w.flush()),
            // doneness is signaled by the writer being dropped.
            SequentialOutput::Sync(None) => Poll::Ready(Ok(())),
            SequentialOutput::Async(w) => std::pin::pin!(w).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            SequentialOutput::Sync(s) => {
                let taken = std::mem::take(s);
                // dropping the sync writer will cause the writer to be closed (at least for files).
                drop(taken);
                Poll::Ready(Ok(()))
            },
            SequentialOutput::Async(w) => std::pin::pin!(w).poll_close(cx),
        }
    }
}

impl Write for SequentialOutput {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let SequentialOutput::Sync(w_option) = self else {
            return Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "writer is async"));
        };
        let Some(w) = w_option else {
            return Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer is closed"));
        };
        w.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let SequentialOutput::Sync(w_option) = self else {
            return Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "writer is async"));
        };
        let Some(w) = w_option else {
            return Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "writer is closed"));
        };
        w.flush()
    }
}

/// Enum of different output formats to write reconstructed files
/// where the result writer can be set at a specific position and new handles can be created
#[derive(Debug, Clone)]
pub enum SeekingOutputProvider {
    File(FileProvider),
    #[cfg(test)]
    Buffer(BufferProvider),
}

impl SeekingOutputProvider {
    /// Create a new writer to start writing at the indicated start location.
    pub(crate) fn get_writer_at(&self, start: u64) -> Result<Box<dyn Write + Send>> {
        match self {
            SeekingOutputProvider::File(fp) => fp.get_writer_at(start),
            #[cfg(test)]
            SeekingOutputProvider::Buffer(bp) => bp.get_writer_at(start),
        }
    }
}

impl TryFrom<SeekingOutputProvider> for SequentialOutput {
    type Error = CasClientError;

    fn try_from(value: SeekingOutputProvider) -> std::result::Result<Self, Self::Error> {
        let inner = value.get_writer_at(0)?;
        Ok(SequentialOutput::Sync(Some(inner)))
    }
}

pub enum OutputProvider {
    Seeking(SeekingOutputProvider),
    Sequential(SequentialOutput),
}

impl OutputProvider {
    pub fn is_seeking(&self) -> bool {
        matches!(self, Self::Seeking(_))
    }

    pub fn is_sequential(&self) -> bool {
        matches!(self, Self::Sequential(_))
    }

    pub fn as_seeking(&self) -> Option<&SeekingOutputProvider> {
        match self {
            Self::Seeking(p) => Some(p),
            _ => None,
        }
    }

    pub fn as_sequential(&self) -> Option<&SequentialOutput> {
        match self {
            Self::Sequential(p) => Some(p),
            _ => None,
        }
    }

    pub fn new_file_seeking(filepath: PathBuf) -> Self {
        Self::Seeking(SeekingOutputProvider::File(FileProvider::new(filepath)))
    }

    #[cfg(test)]
    pub fn new_buffer_seeking(buf: ThreadSafeBuffer) -> Self {
        Self::Seeking(SeekingOutputProvider::Buffer(BufferProvider { buf }))
    }

    pub fn from_writer<T: Write + Send + 'static>(writer: T) -> Self {
        Self::Sequential(SequentialOutput::Sync(Some(Box::new(writer))))
    }

    pub fn from_async_writer<T: AsyncWrite + Send + Unpin + 'static>(writer: T) -> Self {
        Self::Sequential(SequentialOutput::Async(Box::new(writer)))
    }
}

impl TryFrom<OutputProvider> for SequentialOutput {
    type Error = CasClientError;

    fn try_from(value: OutputProvider) -> std::result::Result<Self, Self::Error> {
        match value {
            OutputProvider::Seeking(s) => s.try_into(),
            OutputProvider::Sequential(s) => Ok(s),
        }
    }
}

/// Provides new Writers to a file located at a particular location
#[derive(Debug, Clone)]
pub struct FileProvider {
    filename: PathBuf,
}

/// FileProvider may be Seeking or Sequential
impl FileProvider {
    pub fn new(filename: PathBuf) -> Self {
        Self { filename }
    }

    fn get_writer_at(&self, start: u64) -> Result<Box<dyn Write + Send>> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(false)
            .create(true)
            .open(&self.filename)?;
        file.seek(SeekFrom::Start(start))?;
        Ok(Box::new(file))
    }
}

/// BufferProvider may be Seeking or Sequential
/// only used in testing
#[derive(Debug, Clone)]
pub struct BufferProvider {
    pub buf: ThreadSafeBuffer,
}

impl BufferProvider {
    pub fn get_writer_at(&self, start: u64) -> Result<Box<dyn Write + Send>> {
        let mut buffer = self.buf.clone();
        buffer.idx = start;
        Ok(Box::new(buffer))
    }
}

#[derive(Debug, Default, Clone)]
/// Thread-safe in-memory buffer that implements [Write](Write) trait at some position
/// within an underlying buffer and allows access to the inner buffer.
/// Thread-safe in-memory buffer that implements [Write](Write) trait and allows
/// access to the inner buffer
pub struct ThreadSafeBuffer {
    idx: u64,
    inner: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl ThreadSafeBuffer {
    pub fn value(&self) -> Vec<u8> {
        self.inner.lock().unwrap().get_ref().clone()
    }
}

impl Write for ThreadSafeBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut guard = self.inner.lock().map_err(|e| std::io::Error::other(format!("{e}")))?;
        guard.set_position(self.idx);
        let num_written = guard.write(buf)?;
        self.idx = guard.position();
        Ok(num_written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
