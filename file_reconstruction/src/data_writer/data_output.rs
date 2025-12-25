use std::io::Write;
use std::path::PathBuf;

/// The data output type for the file reconstructor.
pub enum DataOutput {
    /// A custom writer that will receive the reconstructed data.
    /// The writer is expected to handle the data starting from position 0.
    SequentialWriter(Box<dyn Write + Send + 'static>),

    /// A file path where the reconstructed data will be written.
    /// The file will be opened without truncation, allowing multiple concurrent
    /// reconstructions to write to different regions of the same file.
    File {
        path: PathBuf,
        /// If `Some`, seek to this offset before writing.
        /// If `None`, use the byte range start from the reconstruction request.
        offset: Option<u64>,
    },
}

impl DataOutput {
    /// Creates a file output that writes to the given path.
    ///
    /// The write position is determined by the byte range of the reconstruction request.
    /// For a full file reconstruction, this writes from position 0.
    /// For a partial range reconstruction, this seeks to the start of the requested range.
    pub fn write_in_file(path: impl Into<PathBuf>) -> Self {
        Self::File {
            path: path.into(),
            offset: None,
        }
    }

    /// Creates a file output that writes to the given path at a specific offset.
    ///
    /// Use this when you want to control exactly where in the file the data is written,
    /// regardless of the byte range being reconstructed.
    pub fn write_file_at_offset(path: impl Into<PathBuf>, offset: u64) -> Self {
        Self::File {
            path: path.into(),
            offset: Some(offset),
        }
    }

    /// Creates a writer output that sends data to the given writer.
    ///
    /// The writer receives data starting from position 0, regardless of the
    /// byte range being reconstructed.
    pub fn writer(writer: impl Write + Send + 'static) -> Self {
        Self::SequentialWriter(Box::new(writer))
    }
}
