use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use cas_types::FileRange;
use tokio::sync::OwnedSemaphorePermit;

use crate::Result;

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
