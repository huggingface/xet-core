use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use cas_client::Client;
use cas_types::FileRange;
use merklehash::MerkleHash;
use more_asserts::*;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, info};
use utils::ExpWeightedMovingAvg;
use xet_config::ReconstructionConfig;

use super::file_term::{FileTerm, retrieve_file_term_block};
use crate::FileReconstructionError;
use crate::error::Result;

type RawFetchedFileTerms = Result<Option<Vec<FileTerm>>>;

/// Manages the iteration over file terms during reconstruction, with adaptive prefetching.
/// Prefetches reconstruction blocks ahead of consumption based on observed completion rates
/// to minimize download latency while controlling memory usage.
pub struct ReconstructionTermManager {
    config: Arc<ReconstructionConfig>,
    client: Arc<dyn Client>,
    file_hash: MerkleHash,
    requested_byte_range: FileRange,
    last_block_info: Option<(Instant, FileRange)>,
    known_final_byte_position: Arc<AtomicU64>,
    prefetched_byte_position: u64,
    current_active_byte_position: u64,
    prefetch_queue: VecDeque<JoinHandle<RawFetchedFileTerms>>,
    completion_rate_estimator: ExpWeightedMovingAvg,
}

impl ReconstructionTermManager {
    pub async fn new(
        config: Arc<ReconstructionConfig>,
        client: Arc<dyn Client>,
        file_hash: MerkleHash,
        file_byte_range: FileRange,
    ) -> Result<Self> {
        let completion_rate_estimator =
            ExpWeightedMovingAvg::new_count_decay(config.completion_rate_estimator_half_life);

        let requested_byte_range = file_byte_range;

        let mut s = Self {
            config,
            client,
            file_hash,
            requested_byte_range,
            last_block_info: None,
            prefetched_byte_position: requested_byte_range.start,
            current_active_byte_position: requested_byte_range.start,
            prefetch_queue: VecDeque::new(),
            known_final_byte_position: Arc::new(AtomicU64::new(requested_byte_range.end)),
            completion_rate_estimator,
        };

        // Start things by prefetching two smaller blocks to get things started.  This way,
        // once the first block is finished, we have a second block to start processing -- and
        // an estimate of the completion time based on the first one.  This helps us to get
        // a better estimate of the completion time.
        let initial_fetch_size = s.config.min_reconstruction_fetch_size.as_u64();
        s.prefetch_block(initial_fetch_size).await?;
        s.prefetch_block(2 * initial_fetch_size).await?;

        debug!(
            %file_hash,
            prefetch_queue_size = s.prefetch_queue.len(),
            "Initial prefetch blocks queued"
        );

        Ok(s)
    }

    /// Returns the next block of file terms, or None if reconstruction is complete.
    /// Updates the completion rate estimator based on the time since the last call.
    pub async fn next_file_terms(&mut self) -> Result<Option<Vec<FileTerm>>> {
        // Update completion rate estimator if we have timing info from a previous block.
        if let Some((block_start_time, block_range)) = self.last_block_info.take() {
            let completion_time = Instant::now().duration_since(block_start_time).as_secs_f64();
            let block_size = block_range.end - block_range.start;

            if block_size != 0 {
                self.completion_rate_estimator
                    .update((block_size as f64) / completion_time.max(1e-6));
            }

            info!(
                file_hash = %self.file_hash,
                block_start = block_range.start,
                block_end = block_range.end,
                block_size = block_size,
                completion_time = completion_time,
                "Updated completion rate estimate based on previous block completion time (seconds)."
            );
        }

        // Check the prefetch buffer to possibly prefetch the next block.
        self.check_prefetch_buffer().await?;

        let Some(next_block_jh) = self.prefetch_queue.pop_front() else {
            // If there are no more prefetched terms then we're done.
            // Note: we check against known_final_byte_position since requested_byte_range.end
            // may be u64::MAX if the full file was requested.
            debug_assert_ge!(self.prefetched_byte_position, self.known_final_byte_position.load(Ordering::Relaxed));
            return Ok(None);
        };

        let maybe_next_block = next_block_jh
            .await
            .map_err(|e| FileReconstructionError::InternalError(format!("Join error: {e}")))??;

        if let Some(file_terms) = maybe_next_block {
            // Calculate the byte range of this block from the file terms.
            let block_start = file_terms.first().map(|t| t.byte_range.start).unwrap_or(0);
            let block_end = file_terms.last().map(|t| t.byte_range.end).unwrap_or(0);

            // Record timing info for the next call.
            self.last_block_info = Some((Instant::now(), FileRange::new(block_start, block_end)));

            // Update the current active byte position.
            self.current_active_byte_position = block_end;

            info!(
                file_hash = %self.file_hash,
                block_start = block_start,
                block_end = block_end,
                block_size = file_terms.len(),
                "Received block of file terms from prefetch queue"
            );

            Ok(Some(file_terms))
        } else {
            // We've completed the iteration, so record the final byte position.
            self.known_final_byte_position
                .store(self.prefetched_byte_position, Ordering::Relaxed);

            info!(
                file_hash = %self.file_hash,
                prefetched_byte_position = self.prefetched_byte_position,
                "Completed prefetch queue; end of file reached."
            );

            Ok(None)
        }
    }

    fn is_done_fetching(&self) -> bool {
        self.prefetched_byte_position >= self.known_final_byte_position.load(Ordering::Relaxed)
    }

    /// Checks the prefetch queue to ensure that we have enough incoming to keep everything happy.
    async fn check_prefetch_buffer(&mut self) -> Result<()> {
        // If we're done, then there's nothing more to do.
        if self.is_done_fetching() {
            return Ok(());
        }

        // How long we expect for a reconstruction block to complete.
        let target_completion_time = self.config.target_block_completion_time.as_secs_f64();

        // We choose a next block size to complete within minutes based on the
        // current observations of how long it takes.
        let completion_rate = self.completion_rate_estimator.value();

        // The target prefetch buffer size.  We want to make sure at least
        // this much has been prefetched.
        let prefetch_buffer_target_size = target_completion_time * completion_rate;

        // We need to maintain a minimum amount in the prefetch buffer.
        let min_prefetch_buffer_size = self.config.min_prefetch_buffer.as_u64() as f64;
        let prefetch_buffer_size = prefetch_buffer_target_size.max(min_prefetch_buffer_size);

        // The current prefetch buffer size; we want to expand this by the target size.
        let current_prefetch_buffer_size = self.prefetched_byte_position - self.current_active_byte_position;

        // If we're already at or above the target prefetch buffer size, then don't prefetch more
        // unless the queue is empty.
        if !self.prefetch_queue.is_empty() && prefetch_buffer_size <= current_prefetch_buffer_size as f64 {
            return Ok(());
        }

        // Let's see what we need to prefetch here.
        let next_prefetch_target_block_size = (prefetch_buffer_size - current_prefetch_buffer_size as f64) as u64;

        let min_fetch_size = self.config.min_reconstruction_fetch_size.as_u64();
        let max_fetch_size = self.config.max_reconstruction_fetch_size.as_u64().max(min_fetch_size);
        let next_prefetch_block_size = next_prefetch_target_block_size.clamp(min_fetch_size, max_fetch_size);

        // Okay, now add this to the prefetch queue.
        self.prefetch_block(next_prefetch_block_size).await
    }

    async fn prefetch_block(&mut self, block_size: u64) -> Result<()> {
        let block_size = block_size.clamp(
            self.config.min_reconstruction_fetch_size.as_u64(),
            self.config.max_reconstruction_fetch_size.as_u64(),
        );

        // First, check the block range to see if we're over the requested range.
        let mut prefetch_block_range =
            FileRange::new(self.prefetched_byte_position, self.prefetched_byte_position + block_size);

        // Get the end of the known range, if it is known.  If it's unknown, this is u64::MAX.
        let last_byte_position = self
            .known_final_byte_position
            .load(Ordering::Relaxed)
            .min(self.requested_byte_range.end);

        // Clamp to the requested range.
        if prefetch_block_range.end > last_byte_position {
            prefetch_block_range.end = last_byte_position;
        }

        // Check if we should extend this one to the end.
        let min_fetch_size = self.config.min_reconstruction_fetch_size.as_u64();
        if prefetch_block_range.end + min_fetch_size > self.requested_byte_range.end {
            prefetch_block_range.end = self.requested_byte_range.end;
        }

        // It's possible that the start is at or past the end of the requested range; in that case, do nothing.
        // This also handles empty files where start >= end.
        if prefetch_block_range.start >= prefetch_block_range.end {
            debug!(
                file_hash = %self.file_hash,
                "Prefetch block skipped - already at or past end of requested range"
            );
            return Ok(());
        }

        let actual_block_size = prefetch_block_range.end - prefetch_block_range.start;
        info!(
            file_hash = %self.file_hash,
            prefetch_range = ?(prefetch_block_range.start, prefetch_block_range.end),
            requested_block_size = block_size,
            actual_block_size,
            queue_depth = self.prefetch_queue.len() + 1,
            "Scheduling prefetch block"
        );

        // Update the prefetched position now.
        self.prefetched_byte_position = prefetch_block_range.end;

        // Add the prefetch task to the queue.
        let known_final_byte_position = self.known_final_byte_position.clone();
        let client = self.client.clone();
        let file_hash = self.file_hash;

        let jh = tokio::task::spawn(async move {
            let result = retrieve_file_term_block(client, file_hash, prefetch_block_range).await;

            // See if we're done with the file.
            if let Ok(Some((ref returned_range, ref file_terms))) = result {
                // See if the returned range is less than the requested range; if so, then
                // we know we've reached the end of the file.
                debug_assert_eq!(returned_range.start, prefetch_block_range.start);

                if returned_range.end < prefetch_block_range.end {
                    known_final_byte_position.store(returned_range.end, Ordering::Relaxed);
                }

                // Return just the file terms.
                Ok(Some(file_terms.clone()))
            } else if let Ok(None) = result {
                // If the returned block is None, then we're beyond the end of the file; update the known final byte
                // position to the start of the prefetch block if it hasn't been set yet (which it might
                // have been in a separate block).
                known_final_byte_position.fetch_min(prefetch_block_range.start, Ordering::Relaxed);
                Ok(None)
            } else {
                result.map(|r| r.map(|(_, file_terms)| file_terms))
            }
        });

        self.prefetch_queue.push_back(jh);

        Ok(())
    }
}
