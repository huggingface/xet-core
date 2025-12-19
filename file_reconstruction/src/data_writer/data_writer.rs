use std::sync::Arc;

use bytes::Bytes;
use cas_types::FileRange;

use crate::Result;
use crate::data_writer::DataOutput;
use crate::data_writer::sequential_writer::SequentialWriter;

#[async_trait::async_trait]
pub trait DataReceptacle: Send + Sync + 'static {
    async fn write(self: Box<Self>, data: Bytes) -> Result<()>;
}

#[async_trait::async_trait]
pub trait DataWriter: Send + Sync + 'static {
    /// Acquires a receptacle for writing data at the given term index.
    ///
    /// The term indices must be requested in strictly increasing order.
    /// The returned `DataReceptacle` can later be filled with data by calling `write(data)`.
    async fn get_data_receptacle(
        &self,
        term_index: usize,
        term_byte_range: FileRange,
    ) -> Result<Box<dyn DataReceptacle>>;

    /// Waits until all data has been written.
    ///
    /// Once this method is called, further calls to get_data_receptacle will fail.
    async fn finish(&self) -> Result<()>;
}

pub fn new_data_writer(
    output: DataOutput,
    _config: &xet_config::ReconstructionConfig,
    start_term_index: usize,
    start_file_position: u64,
) -> Result<Arc<dyn DataWriter>> {
    match output {
        DataOutput::SequentialWriter(writer) => {
            Ok(Arc::new(SequentialWriter::new(writer, start_file_position, start_term_index)))
        },
        DataOutput::File(path) => {
            let file = std::fs::File::create(&path)?;
            Ok(Arc::new(SequentialWriter::new(Box::new(file), start_file_position, start_term_index)))
        },
    }
}
