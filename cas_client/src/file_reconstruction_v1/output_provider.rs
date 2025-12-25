use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncWrite;

use crate::CasClientError;
use crate::error::Result;

/// type that represents all acceptable sequential output mechanisms
/// To convert something that is Write rather than AsyncWrite uses the AsyncWriteFromWrite adapter
pub type SequentialOutput = Box<dyn AsyncWrite + Send + Unpin>;

pub fn sequential_output_from_filepath(filename: impl AsRef<Path>) -> Result<SequentialOutput> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(false)
        .create(true)
        .open(&filename)?;
    Ok(Box::new(AsyncWriteFromWrite(Some(Box::new(file)))))
}

/// Creates a SequentialOutput from an existing Write implementation.
pub fn sequential_output_from_writer(writer: Box<dyn Write + Send>) -> SequentialOutput {
    Box::new(AsyncWriteFromWrite(Some(writer)))
}

/// Enum of different output formats to write reconstructed files
/// where the result writer can be set at a specific position and new handles can be created
#[derive(Debug, Clone)]
pub enum SeekingOutputProvider {
    File(FileProvider),
    #[cfg(test)]
    Buffer(buffer_provider::BufferProvider),
}

impl SeekingOutputProvider {
    // shortcut to create a new FileProvider variant from filename
    pub fn new_file_provider(filename: PathBuf) -> Self {
        Self::File(FileProvider::new(filename))
    }

    /// Create a new writer to start writing at the indicated start location.
    pub(crate) fn get_writer_at(&self, start: u64) -> Result<Box<dyn Write + Send>> {
        match self {
            SeekingOutputProvider::File(fp) => fp.get_writer_at(start),
            #[cfg(test)]
            SeekingOutputProvider::Buffer(bp) => bp.get_writer_at(start),
        }
    }
}

// Adapter used to create an AsyncWrite from a Writer.
struct AsyncWriteFromWrite(Option<Box<dyn Write + Send>>);

impl AsyncWrite for AsyncWriteFromWrite {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let Some(inner) = self.0.as_mut() else {
            return Poll::Ready(Ok(0));
        };
        Poll::Ready(inner.write(buf))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let Some(inner) = self.0.as_mut() else {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "writer closed, already dropped",
            )));
        };
        Poll::Ready(inner.flush())
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let _ = self.0.take();
        Poll::Ready(Ok(()))
    }
}

impl TryFrom<SeekingOutputProvider> for SequentialOutput {
    type Error = CasClientError;

    fn try_from(value: SeekingOutputProvider) -> std::result::Result<Self, Self::Error> {
        let w = value.get_writer_at(0)?;
        Ok(Box::new(AsyncWriteFromWrite(Some(w))))
    }
}

/// Provides new Writers to a file located at a particular location
#[derive(Debug, Clone)]
pub struct FileProvider {
    filename: PathBuf,
}

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

#[cfg(test)]
pub mod buffer_provider {
    use std::io::{Cursor, Write};
    use std::sync::{Arc, Mutex};

    use super::{AsyncWriteFromWrite, SeekingOutputProvider, SequentialOutput};
    use crate::error::Result;

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
            let num_written = Write::write(guard.get_mut(), buf)?;
            self.idx = guard.position();
            Ok(num_written)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl From<ThreadSafeBuffer> for SequentialOutput {
        fn from(value: ThreadSafeBuffer) -> Self {
            Box::new(AsyncWriteFromWrite(Some(Box::new(value))))
        }
    }

    impl From<ThreadSafeBuffer> for SeekingOutputProvider {
        fn from(value: ThreadSafeBuffer) -> Self {
            SeekingOutputProvider::Buffer(BufferProvider { buf: value })
        }
    }
}
