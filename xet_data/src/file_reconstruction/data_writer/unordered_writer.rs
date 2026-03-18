use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use xet_client::cas_types::FileRange;
use xet_runtime::utils::adjustable_semaphore::AdjustableSemaphorePermit;

use super::super::data_writer::{DataFuture, DataWriter};
use super::super::run_state::RunState;
use super::super::{FileReconstructionError, Result};

/// A completed term ready for consumption. Contains the byte range indicating
/// where this data belongs in the output file, the actual data bytes, and an
/// optional semaphore permit for backpressure control.
pub(crate) struct CompletedTerm {
    pub byte_range: FileRange,
    pub data: Bytes,
    pub permit: Option<AdjustableSemaphorePermit>,
}

/// Atomic progress counters shared between the writer, its spawned tasks,
/// and the consumer stream. Wrapped in an `Arc` so each party can read/update
/// counters without holding a reference to the full `UnorderedWriter`.
pub(crate) struct UnorderedWriterProgress {
    pub terms_in_progress: AtomicU64,
    pub bytes_in_progress: AtomicU64,
    pub bytes_completed: AtomicU64,
    pub total_bytes_expected: AtomicU64,
    pub finished: AtomicBool,
}

impl UnorderedWriterProgress {
    pub fn set_total_bytes_expected(&self, size: u64) {
        self.total_bytes_expected.store(size, Ordering::Release);
    }

    pub fn total_bytes_expected(&self) -> u64 {
        self.total_bytes_expected.load(Ordering::Acquire)
    }

    pub fn terms_in_progress(&self) -> u64 {
        self.terms_in_progress.load(Ordering::Acquire)
    }

    pub fn bytes_in_progress(&self) -> u64 {
        self.bytes_in_progress.load(Ordering::Relaxed)
    }

    pub fn bytes_completed(&self) -> u64 {
        self.bytes_completed.load(Ordering::Relaxed)
    }

    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }
}

/// Lock-free writer that delivers completed data terms in arbitrary order.
///
/// Each call to [`set_next_term_data_source`](DataWriter::set_next_term_data_source)
/// spawns an independent tokio task that resolves the data future and sends the
/// result through an [`mpsc`](tokio::sync::mpsc) channel. The consumer
/// (typically an [`UnorderedDownloadStream`](super::unordered_download_stream::UnorderedDownloadStream))
/// reads from the receiver end and gets items in whatever order tasks complete.
///
/// The consumer stream holds only `Arc<WriterProgress>`, not `Arc<UnorderedWriter>`,
/// so the writer's channel sender is dropped naturally when the reconstruction
/// task finishes and releases its `Arc<dyn DataWriter>`.
pub struct UnorderedWriter {
    result_tx: UnboundedSender<Result<CompletedTerm>>,
    run_state: Arc<RunState>,
    progress: Arc<UnorderedWriterProgress>,
}

impl Drop for UnorderedWriter {
    fn drop(&mut self) {
        if !self.progress.is_finished() {
            self.run_state.cancel();
        }
    }
}

#[async_trait::async_trait]
impl DataWriter for UnorderedWriter {
    async fn set_next_term_data_source(
        &self,
        byte_range: FileRange,
        permit: Option<AdjustableSemaphorePermit>,
        data_future: DataFuture,
    ) -> Result<()> {
        self.run_state.check_error()?;

        if self.progress.is_finished() {
            return Err(FileReconstructionError::InternalWriterError("Writer has already finished".to_string()));
        }

        let expected_size = byte_range.end - byte_range.start;
        self.progress.terms_in_progress.fetch_add(1, Ordering::Relaxed);
        self.progress.bytes_in_progress.fetch_add(expected_size, Ordering::Relaxed);

        let result_tx = self.result_tx.clone();
        let run_state = self.run_state.clone();
        let progress = self.progress.clone();

        tokio::spawn(async move {
            let result = async {
                run_state.check_error()?;

                let data = data_future.await?;

                if data.len() as u64 != expected_size {
                    return Err(FileReconstructionError::InternalWriterError(format!(
                        "Data size mismatch: expected {} bytes, got {} bytes",
                        expected_size,
                        data.len()
                    )));
                }

                Ok(CompletedTerm {
                    byte_range,
                    data,
                    permit,
                })
            }
            .await;

            if let Err(ref e) = result {
                run_state.set_error(e.clone());
            }

            let completed_bytes = result.as_ref().map(|t| t.data.len() as u64).unwrap_or(0);

            // Send through channel FIRST so data is available before we
            // signal completion via counter updates.
            let _ = result_tx.send(result);

            progress.bytes_in_progress.fetch_sub(expected_size, Ordering::Relaxed);
            progress.bytes_completed.fetch_add(completed_bytes, Ordering::Relaxed);
            // Release on this decrement pairs with Acquire in the consumer's
            // completion check, ensuring bytes_completed is visible.
            progress.terms_in_progress.fetch_sub(1, Ordering::Release);
        });

        Ok(())
    }

    async fn finish(&self) -> Result<u64> {
        self.run_state.check_error()?;

        if self.progress.finished.swap(true, Ordering::Release) {
            return Err(FileReconstructionError::InternalWriterError("Writer has already finished".to_string()));
        }

        let total = self.progress.total_bytes_expected.load(Ordering::Acquire);
        if total > 0 {
            Ok(total)
        } else {
            Ok(self.progress.bytes_in_progress.load(Ordering::Relaxed)
                + self.progress.bytes_completed.load(Ordering::Relaxed))
        }
    }
}

impl UnorderedWriter {
    /// Creates an unordered writer for streaming use. Returns the writer (to be
    /// passed to the reconstruction task as `Arc<dyn DataWriter>`), the receiver
    /// end of the channel, and the shared progress counters for the consumer.
    ///
    /// The consumer stream should hold only the `Arc<WriterProgress>`, **not**
    /// the writer itself. This way the channel sender is dropped naturally when
    /// the reconstruction task finishes, closing the channel without explicit
    /// lifetime management.
    pub(crate) fn new_streaming(
        run_state: Arc<RunState>,
    ) -> (Arc<UnorderedWriter>, UnboundedReceiver<Result<CompletedTerm>>, Arc<UnorderedWriterProgress>) {
        let (tx, rx) = unbounded_channel();

        let progress = Arc::new(UnorderedWriterProgress {
            terms_in_progress: AtomicU64::new(0),
            bytes_in_progress: AtomicU64::new(0),
            bytes_completed: AtomicU64::new(0),
            total_bytes_expected: AtomicU64::new(0),
            finished: AtomicBool::new(false),
        });

        let writer = Arc::new(UnorderedWriter {
            result_tx: tx,
            run_state,
            progress: progress.clone(),
        });

        (writer, rx, progress)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use xet_runtime::utils::adjustable_semaphore::AdjustableSemaphore;

    use super::*;

    fn immediate_future(data: Bytes) -> DataFuture {
        Box::pin(async move { Ok(data) })
    }

    fn delayed_future(data: Bytes, delay: Duration) -> DataFuture {
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            Ok(data)
        })
    }

    /// Drains all results from the receiver, returning data sorted by offset.
    /// Requires `finish()` to have been called on the writer before calling.
    async fn drain_sorted(
        rx: &mut UnboundedReceiver<Result<CompletedTerm>>,
        progress: &UnorderedWriterProgress,
    ) -> Result<Vec<(u64, Bytes)>> {
        assert!(progress.is_finished(), "finish() must be called before drain_sorted");
        let mut items = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(Ok(term)) => {
                    items.push((term.byte_range.start, term.data));
                    drop(term.permit);
                },
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    if progress.terms_in_progress() == 0 {
                        break;
                    }
                    match rx.recv().await {
                        Some(Ok(term)) => {
                            items.push((term.byte_range.start, term.data));
                            drop(term.permit);
                        },
                        Some(Err(e)) => return Err(e),
                        None => break,
                    }
                },
            }
        }
        items.sort_by_key(|(offset, _)| *offset);
        Ok(items)
    }

    #[tokio::test]
    async fn test_basic_unordered_writes() {
        let run_state = RunState::new_for_test();
        let (writer, mut rx, progress) = UnorderedWriter::new_streaming(run_state);

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(5, 6), None, immediate_future(Bytes::from(" ")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(6, 11), None, immediate_future(Bytes::from("World")))
            .await
            .unwrap();

        writer.finish().await.unwrap();

        let items = drain_sorted(&mut rx, &progress).await.unwrap();
        let assembled: Vec<u8> = items.into_iter().flat_map(|(_, data)| data.to_vec()).collect();
        assert_eq!(&assembled, b"Hello World");
    }

    #[tokio::test]
    async fn test_delayed_futures_complete_out_of_order() {
        let run_state = RunState::new_for_test();
        let (writer, mut rx, progress) = UnorderedWriter::new_streaming(run_state);

        writer
            .set_next_term_data_source(
                FileRange::new(0, 5),
                None,
                delayed_future(Bytes::from("Hello"), Duration::from_millis(80)),
            )
            .await
            .unwrap();
        writer
            .set_next_term_data_source(
                FileRange::new(5, 6),
                None,
                delayed_future(Bytes::from(" "), Duration::from_millis(40)),
            )
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(6, 11), None, immediate_future(Bytes::from("World")))
            .await
            .unwrap();

        writer.finish().await.unwrap();

        let items = drain_sorted(&mut rx, &progress).await.unwrap();
        let assembled: Vec<u8> = items.into_iter().flat_map(|(_, data)| data.to_vec()).collect();
        assert_eq!(&assembled, b"Hello World");
    }

    #[tokio::test]
    async fn test_size_mismatch_error() {
        let run_state = RunState::new_for_test();
        let (writer, mut rx, _progress) = UnorderedWriter::new_streaming(run_state);

        writer
            .set_next_term_data_source(FileRange::new(0, 10), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();

        writer.finish().await.unwrap();

        let result = rx.recv().await.unwrap();
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_finish_twice_returns_error() {
        let run_state = RunState::new_for_test();
        let (writer, _rx, _progress) = UnorderedWriter::new_streaming(run_state);

        writer.finish().await.unwrap();
        let result = writer.finish().await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_write_after_finish_returns_error() {
        let run_state = RunState::new_for_test();
        let (writer, _rx, _progress) = UnorderedWriter::new_streaming(run_state);

        writer.finish().await.unwrap();
        let result = writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FileReconstructionError::InternalWriterError(_))));
    }

    #[tokio::test]
    async fn test_future_error_propagates() {
        let run_state = RunState::new_for_test();
        let (writer, mut rx, _progress) = UnorderedWriter::new_streaming(run_state);

        let failing_future: DataFuture =
            Box::pin(async { Err(FileReconstructionError::InternalError("Simulated error".to_string())) });

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, failing_future)
            .await
            .unwrap();

        writer.finish().await.unwrap();

        let result = rx.recv().await.unwrap();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_semaphore_permit_released_after_consumption() {
        let run_state = RunState::new_for_test();
        let (writer, mut rx, progress) = UnorderedWriter::new_streaming(run_state);
        let semaphore = AdjustableSemaphore::new(2, (0, 2));

        let permit1 = semaphore.acquire().await.unwrap();
        let permit2 = semaphore.acquire().await.unwrap();
        assert_eq!(semaphore.available_permits(), 0);

        writer
            .set_next_term_data_source(FileRange::new(0, 5), Some(permit1), immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(5, 6), Some(permit2), immediate_future(Bytes::from(" ")))
            .await
            .unwrap();

        writer.finish().await.unwrap();

        let items = drain_sorted(&mut rx, &progress).await.unwrap();
        drop(items);

        assert_eq!(semaphore.available_permits(), 2);
    }

    #[tokio::test]
    async fn test_atomic_counter_accuracy() {
        let run_state = RunState::new_for_test();
        let (writer, mut rx, progress) = UnorderedWriter::new_streaming(run_state);

        writer
            .set_next_term_data_source(
                FileRange::new(0, 5),
                None,
                delayed_future(Bytes::from("Hello"), Duration::from_millis(50)),
            )
            .await
            .unwrap();
        writer
            .set_next_term_data_source(
                FileRange::new(5, 11),
                None,
                delayed_future(Bytes::from(" World"), Duration::from_millis(50)),
            )
            .await
            .unwrap();

        assert_eq!(progress.bytes_completed(), 0);

        writer.finish().await.unwrap();

        let _items = drain_sorted(&mut rx, &progress).await.unwrap();

        assert_eq!(progress.bytes_completed(), 11);
        assert_eq!(progress.bytes_in_progress(), 0);
        assert_eq!(progress.terms_in_progress(), 0);
    }

    #[tokio::test]
    async fn test_total_bytes_expected() {
        let run_state = RunState::new_for_test();
        let (writer, _rx, progress) = UnorderedWriter::new_streaming(run_state);

        assert_eq!(progress.total_bytes_expected(), 0);

        progress.set_total_bytes_expected(1024);
        assert_eq!(progress.total_bytes_expected(), 1024);

        let total = writer.finish().await.unwrap();
        assert_eq!(total, 1024);
    }

    #[tokio::test]
    async fn test_finish_returns_accumulated_when_total_unknown() {
        let run_state = RunState::new_for_test();
        let (writer, mut rx, progress) = UnorderedWriter::new_streaming(run_state);

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(FileRange::new(5, 11), None, immediate_future(Bytes::from(" World")))
            .await
            .unwrap();

        let total = writer.finish().await.unwrap();
        // finish() returns bytes_in_progress + bytes_completed when
        // total_bytes_expected is 0. At finish time, both terms are still
        // in progress (11 bytes total), so finish captures that.
        assert_eq!(total, 11);

        let _items = drain_sorted(&mut rx, &progress).await.unwrap();
    }

    #[tokio::test]
    async fn test_error_propagation_prevents_subsequent_writes() {
        let run_state = RunState::new_for_test();
        let (writer, mut _rx, _progress) = UnorderedWriter::new_streaming(run_state);

        let failing_future: DataFuture =
            Box::pin(async { Err(FileReconstructionError::InternalError("fail".to_string())) });

        writer
            .set_next_term_data_source(FileRange::new(0, 5), None, failing_future)
            .await
            .unwrap();

        // Wait for the error to propagate through run_state.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let result = writer
            .set_next_term_data_source(FileRange::new(5, 10), None, immediate_future(Bytes::from("World")))
            .await;
        assert!(result.is_err());
    }
}
