use std::sync::Arc;

use bytes::Bytes;
use cas_types::FileRange;
use tokio::sync::OwnedSemaphorePermit;

use crate::Result;
use crate::data_writer::DataOutput;
use crate::data_writer::sequential_writer::SequentialWriter;

#[async_trait::async_trait]
pub trait DataReceptacle: Send + Sync + 'static {
    async fn write(self: Box<Self>, data: Bytes) -> Result<()>;
}

#[async_trait::async_trait]
pub trait DataWriter: Send + Sync + 'static {
    /// Acquires the next receptacle for writing data at the given byte range.
    ///
    /// The byte range must be sequential - its start must match the end of the
    /// previous range (or 0 for the first call). Returns an error if the range
    /// is not sequential. The returned `DataReceptacle` can later be filled
    /// with data by calling `write(data)`.
    ///
    /// An optional semaphore permit can be passed for rate limiting. The permit
    /// will be released by the background writer after the data has been written.
    async fn next_data_receptacle(
        &self,
        byte_range: FileRange,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Result<Box<dyn DataReceptacle>>;

    /// Waits until all data has been written.
    ///
    /// Once this method is called, further calls to next_data_receptacle will fail.
    async fn finish(&self) -> Result<()>;
}

pub fn new_data_writer(output: DataOutput, _config: &xet_config::ReconstructionConfig) -> Result<Arc<dyn DataWriter>> {
    match output {
        DataOutput::SequentialWriter(writer) => Ok(Arc::new(SequentialWriter::new(writer))),
        DataOutput::File(path) => {
            let file = std::fs::File::create(&path)?;
            Ok(Arc::new(SequentialWriter::new(Box::new(file))))
        },
    }
}
