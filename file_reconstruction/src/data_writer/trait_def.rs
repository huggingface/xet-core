use std::io::Result;

use bytes::Bytes;

#[async_trait::async_trait]
pub trait DataWriter: Send + Sync + 'static {
    async fn begin(&self) -> Result<()>;

    /// Enqueues a write operation to be performed at a given position in the file.
    ///
    /// Note that this operation may block if the reordering buffer is full of out-of-order
    /// values.
    async fn enqueue_write(&self, position: u64, data: Bytes) -> Result<()>;

    /// Waits until all enqueued write operations are completed.  
    ///
    /// Errors if there are gaps in the enqueued write operations.  
    ///
    /// Once this method is called, further calls to enqueue_write will fail.
    async fn finish(&self) -> Result<()>;
}
