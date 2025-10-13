use std::io::{Cursor, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::error::Result;

pub enum SequentialOneshotOutputProvider {
    File(FileProvider),
    Stdout,
    #[cfg(test)]
    Buffer(BufferProvider),
}

impl SequentialOneshotOutputProvider {
    pub fn get_writer(self) -> Result<Box<dyn Write + Send>> {
        match self {
            SequentialOneshotOutputProvider::File(fp) => fp.get_writer(),
            SequentialOneshotOutputProvider::Stdout => Ok(Box::new(std::io::stdout())),
            #[cfg(test)]
            SequentialOneshotOutputProvider::Buffer(bp) => bp.get_writer(),
        }
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

pub enum OutputProvider {
    Seeking(SeekingOutputProvider),
    Sequential(SequentialOneshotOutputProvider),
}

impl From<SeekingOutputProvider> for OutputProvider {
    fn from(p: SeekingOutputProvider) -> Self {
        Self::Seeking(p)
    }
}

impl From<SequentialOneshotOutputProvider> for OutputProvider {
    fn from(p: SequentialOneshotOutputProvider) -> Self {
        Self::Sequential(p)
    }
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

    pub fn as_sequential(&self) -> Option<&SequentialOneshotOutputProvider> {
        match self {
            Self::Sequential(p) => Some(p),
            _ => None,
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

    pub fn get_writer(self: Self) -> Result<Box<dyn Write + Send>> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(self.filename)?;
        Ok(Box::new(file))
    }
}

/// BufferProvider may be Seeking or Sequential
/// only used in testing
#[derive(Debug, Default, Clone)]
pub struct BufferProvider {
    pub buf: ThreadSafeBuffer,
}

impl BufferProvider {
    pub fn get_writer_at(&self, start: u64) -> Result<Box<dyn Write + Send>> {
        let mut buffer = self.buf.clone();
        buffer.idx = start;
        Ok(Box::new(buffer))
    }

    pub fn get_writer(self: Self) -> Result<Box<dyn Write + Send>> {
        let mut buffer = self.buf;
        buffer.idx = 0;
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
