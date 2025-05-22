use std::io::{Cursor, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::error::Result;

/// Enum of different output formats to write reconstructed files.
#[derive(Debug, Clone)]
pub enum OutputProvider {
    File(FileProvider),
    #[cfg(test)]
    Buffer(BufferProvider),
}

impl OutputProvider {
    /// Create a new writer to start writing at the indicated start location.
    pub(crate) fn get_writer_at(&self, start: u64) -> Result<Box<dyn Write + Send>> {
        match self {
            OutputProvider::File(fp) => fp.get_writer_at(start),
            #[cfg(test)]
            OutputProvider::Buffer(bp) => bp.get_writer_at(start),
        }
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

#[derive(Debug, Default, Clone)]
pub struct BufferProvider {
    pub buf: ThreadSafeBuffer,
}

impl BufferProvider {
    pub fn get_writer_at(&self, start: u64) -> crate::error::Result<Box<dyn std::io::Write + Send>> {
        let mut buffer = self.buf.clone();
        buffer.idx = start;
        Ok(Box::new(buffer))
    }
}

#[derive(Debug, Default, Clone)]
/// Thread-safe in-memory buffer that implements [Write](Write) trait at some position
/// within an underlying buffer and allows access to inner buffer.
/// Thread-safe in-memory buffer that implements [Write](Write) trait and allows
/// access to inner buffer
pub struct ThreadSafeBuffer {
    idx: u64,
    inner: Arc<Mutex<Cursor<Vec<u8>>>>,
}
impl ThreadSafeBuffer {
    pub fn value(&self) -> Vec<u8> {
        self.inner.lock().unwrap().get_ref().clone()
    }
}

impl std::io::Write for ThreadSafeBuffer {
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
