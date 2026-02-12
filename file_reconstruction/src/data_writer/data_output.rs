use std::io::Write;
use std::path::PathBuf;

use bytes::Bytes;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

/// An item sent through the channel output: data bytes paired with an optional
/// semaphore permit for backpressure control. The receiver controls memory
/// pressure by holding permits until it has finished processing each chunk.
pub type DataOutputChannelItem = (Bytes, Option<OwnedSemaphorePermit>);

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

    /// Stream reconstructed data through an unbounded tokio channel.
    ///
    /// Each data chunk is sent as a `(Bytes, Option<OwnedSemaphorePermit>)`.
    /// The semaphore permit is forwarded from the writer so the receiver can
    /// control overall memory pressure by dropping permits when done processing.
    /// Completion is signaled by dropping the sender (the receiver sees `None`).
    Channel(UnboundedSender<DataOutputChannelItem>),
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

    /// Creates a channel output and returns the receiving end.
    ///
    /// Data chunks are sent as `(Bytes, Option<OwnedSemaphorePermit>)` through
    /// an unbounded channel. Memory pressure is controlled by the semaphore
    /// permits passed to `set_next_term_data_source`; the receiver should drop
    /// each permit after processing. The receiver will yield `None` when all
    /// data has been written.
    pub fn channel() -> (Self, UnboundedReceiver<DataOutputChannelItem>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (Self::Channel(tx), rx)
    }
}
