use std::fs::OpenOptions;
use std::future::Future;
use std::io::{Seek, SeekFrom};
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use cas_types::FileRange;
use tokio::sync::OwnedSemaphorePermit;
use xet_config::ReconstructionConfig;

use crate::Result;
use crate::data_writer::DataOutput;
use crate::data_writer::sequential_writer::SequentialWriter;

/// A future that produces the data bytes to be written.
pub type DataFuture = Pin<Box<dyn Future<Output = Result<Bytes>> + Send + 'static>>;

#[async_trait::async_trait]
pub trait DataWriter: Send + Sync + 'static {
    /// Sets the data source for the next sequential term.
    ///
    /// The byte range must be sequential - its start must match the end of the
    /// previous range (or 0 for the first call). The data future will be spawned
    /// as a task and its result will be written when ready.
    ///
    /// SequentialWriter will ensure that the actual writes happen in order.
    ///
    /// An optional semaphore permit can be passed for rate limiting. The permit
    /// will be released by the background writer after the data has been written.
    async fn set_next_term_data_source(
        &self,
        byte_range: FileRange,
        permit: Option<OwnedSemaphorePermit>,
        data_future: DataFuture,
    ) -> Result<()>;

    /// Waits until all data has been written and returns the number of bytes written.
    ///
    /// Once this method is called, further calls to set_next_term_data_source will fail.
    async fn finish(&self) -> Result<u64>;
}

/// Creates a new data writer from the given output specification.
///
/// # Arguments
/// * `output` - The output destination (either a custom writer or a file path)
/// * `default_write_position` - The default byte position where writing should begin. For
///   `DataOutput::SequentialWriter`, this is ignored (the writer handles positioning). For `DataOutput::File`, the file
///   is seeked to either the explicit offset (if provided) or this default position.
/// * `config` - Reconstruction configuration, including whether to use vectorized writes.
///
/// # File Handling
/// When using `DataOutput::File`, the file is opened with read/write access without
/// truncation, allowing multiple concurrent reconstructions to write to different
/// regions of the same file.
pub fn new_data_writer(
    output: DataOutput,
    default_write_position: u64,
    config: &ReconstructionConfig,
) -> Result<Arc<dyn DataWriter>> {
    let use_vectored = config.use_vectored_write;

    match output {
        DataOutput::SequentialWriter(writer) => {
            if use_vectored {
                Ok(Arc::new(SequentialWriter::new_vectorized(writer)))
            } else {
                Ok(Arc::new(SequentialWriter::new(writer)))
            }
        },
        DataOutput::File { path, offset } => {
            let mut file = OpenOptions::new().write(true).create(true).truncate(false).open(&path)?;

            let seek_position = offset.unwrap_or(default_write_position);
            if seek_position > 0 {
                file.seek(SeekFrom::Start(seek_position))?;
            }

            if use_vectored {
                Ok(Arc::new(SequentialWriter::new_vectorized(file)))
            } else {
                Ok(Arc::new(SequentialWriter::new(file)))
            }
        },
    }
}
