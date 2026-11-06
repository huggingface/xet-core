use xet_client::cas_types::FileRange;
use xet_runtime::utils::adjustable_semaphore::AdjustableSemaphorePermit;

use super::super::Result;
use super::super::reconstruction_terms::TermReader;

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
pub trait DataWriter: Send + 'static {
    /// Sets the data source for the next sequential term.
    ///
    /// The byte range must be sequential - its start must match the end of the
    /// previous range (or 0 for the first call), and its length must match the
    /// reader's. The reader's bytes become available as the term's xorb block is
    /// downloaded; the writer consumes them when ready.
    ///
    /// SequentialWriter will ensure that the actual writes happen in order.
    ///
    /// An optional semaphore permit can be passed for rate limiting. The permit
    /// will be released by the writer after the data has been written.
    async fn set_next_term_data_source(
        &mut self,
        byte_range: FileRange,
        permit: Option<AdjustableSemaphorePermit>,
        reader: TermReader,
    ) -> Result<()>;

    /// Consumes the writer, waits until all data has been written, and returns the
    /// number of bytes written. Dropping the writer without calling `finish` cancels
    /// the reconstruction via the shared run state.
    async fn finish(mut self: Box<Self>) -> Result<u64>;
}
