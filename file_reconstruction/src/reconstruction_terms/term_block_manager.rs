use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::thread::JoinHandle;

use cas_client::Client;
use cas_types::{FileRange, HttpRange};
use merklehash::MerkleHash;
use more_asserts::*;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use utils::{ExpWeightedMovingAvg, RwTaskLock};
use xet_runtime::xet_config;

use super::single_terms::FileTerm;
use super::term_block::{ReconstructionTermBlock, ReconstructionTermBlocks};
use super::xorb_block::XorbBlock;
use crate::FileReconstructionError;
use crate::error::Result;
use crate::unique_key::{UniqueId, unique_id};

type RTBRetrievalWrapper = RwTaskLock<Arc<ReconstructionTermBlock>, FileReconstructionError>;
type RawFetchedBlock = Result<Option<Arc<ReconstructionTermBlock>>>;

struct ReconstructionTermManager {
    // Configuration options for all this.
    config: Arc<xet_config::ReconstructionConfig>,

    // The client used for requesting everything.
    client: Arc<dyn Client>,

    // The file hash of the file being reconstructed.
    file_hash: MerkleHash,

    // If we've requested a specific byte range, then this is set to that range;
    // otherwise it's set to (0, u64::MAX) until the final byte position is known.
    requested_byte_range: FileRange,

    // The active block in the current iteration.
    active_block: Option<(Instant, Arc<ReconstructionTermBlock>)>,

    // The location in the active block of the current iteration.
    next_within_block_term_index: usize,

    // The known final byte position of the file; set to u64::MAX until known.
    // If the reconstruction returns a partial block, then
    known_final_byte_position: Arc<AtomicU64>,

    // The byte position of the last prefetched byte.
    // If this is >= the final file range, then we've requested all the
    // availaable bytes.
    prefetched_byte_position: u64,

    // The queue of prefetch tasks.  We'll keep a specified number here
    // in flight at any given time; default is 2.
    prefetch_queue: VecDeque<JoinHandle<RawFetchedBlock>>,

    // When we reach the iteration of one block, we update this estimator
    // with the observed bytes per ms completion rate.  This then allows us to estimate
    // how large a block we should ask for in the next prefetch to maintain a suitable
    // time buffer.
    completion_rate_estimator: ExpWeightedMovingAvg,
}

impl ReconstructionTermManager {
    pub async fn new(
        config: Arc<xet_config::ReconstructionConfig>,
        client: Arc<dyn Client>,
        file_hash: MerkleHash,
        file_byte_range: Option<FileRange>,
    ) -> Self {
        let completion_rate_estimator =
            ExpWeightedMovingAvg::new_time_decay(config.reconstruction_completion_rate_estimator_half_life);

        let requested_byte_range = file_byte_range.unwrap_or_else(FileRange::full);

        let mut s = Self {
            config,
            client,
            file_hash,
            requested_byte_range,
            active_block: None,
            prefetched_byte_position: 0,
            prefetch_queue: VecDeque::new(),
            next_within_block_term_index: 0,
            known_final_byte_position: Arc::new(AtomicU64::new(requested_byte_range.end)),
            completion_rate_estimator,
        };

        // Start things by prefetching two blocks to get things started.  This way,
        // once the first block is finished, we have a second block to start processing -- and
        // an estimate of the completion time based on the first one.
        s.prefetch_block(s.config.min_reconstruction_fetch_size).await?;
        s.prefetch_block(2 * s.config.min_reconstruction_fetch_size).await?;

        s
    }
    pub async fn next_file_term(&mut self) -> Result<Option<FileTerm>> {
        // First check if the current active block is complete; if so, clear it out and update the completion rate
        // estimator.
        let is_block_complete = self
            .active_block
            .as_ref()
            .map(|t| self.next_within_block_term_index >= t.1.n_file_terms())
            .unwrap_or(false);

        if is_block_complete && let Some((block_start_time, block)) = self.active_block.take() {
            let completion_time = Instant::now().duration_since(block_start_time).as_secs_f64();
            let block_size = block.byte_range.end - block.byte_range.start;
            self.completion_rate_estimator
                .update((block_size as f64) / (completion_time as f64).max(1e-6));
        }

        if self.active_block.is_none() {
            loop {
                // We're moving on to the next block, so check the prefetch buffer to
                // possibly prefetch the next block.
                self.check_prefetch_buffer().await?;

                let Some(next_block_jh) = self.prefetch_queue.pop_front() else {
                    // If there are no more prefetched terms then we're done.
                    debug_assert_ge!(self.prefetched_byte_position, self.file_byte_range.end);

                    return Ok(None);
                };

                let maybe_next_block = next_block_jh.await??;

                if let Some(next_block) = maybe_next_block {
                    // If it's an empty block, then try again.
                    if next_block.n_file_terms() == 0 {
                        debug_assert_eq!(next_block.byte_range.start, next_block.byte_range.end);
                        continue;
                    }

                    self.active_block = Some((Instant::now(), next_block));
                    self.next_within_block_term_index = 0;
                    break;
                } else {
                    // We've completed the iteration, so record the final byte position.
                    self.known_final_byte_position
                        .store(self.processed_byte_position, Ordering::Relaxed);
                    return Ok(None);
                }
            }
        }

        let Some((_, active_block)) = &self.active_block else {
            debug_assert!(false, "Block should be active at this point");
            return Ok(None);
        };

        // This should never be empty here.
        debug_assert_lt!(self.next_within_block_term_index, active_block.n_file_terms());

        if self.next_within_block_term_index < active_block.n_file_terms() {
            let file_term = active_block.get_file_term(self.next_within_block_term_index);
            self.next_within_block_term_index += 1;
            return Ok(Some(file_term));
        }
    }

    fn current_active_bytes(&self) -> u64 {
        self.active_blocks.last().map(|(block, _)| block.byte_range.end).unwrap_or(0)
    }

    /// Checks the prefetch queue to ensure that we have enough incoming to keep everything happy.
    async fn check_prefetch_buffer(&mut self) -> Result<()> {
        // If we're done, then there's nothing more to do.
        if self.final_bytes_fetched {
            return;
        }

        // How long we expect for a reconstruction block to complete.
        let target_completion_time = self.config.target_reconstruction_block_completion_time.as_secs_f64();

        // We choose a next block size to complete within minutes based on the
        // current observations of how long it takes.
        let completion_rate = self.completion_rate_estimator.value();

        // The target prefetch buffer size.  We want to make sure at least
        // this much has been prefetched.
        let prefetch_buffer_target_size = target_completion_time * completion_rate;

        // We need to maintain a minimum amount in the prefetch buffer.
        let min_prefetch_buffersize = self.config.min_reconstruction_prefetch_buffer;
        let prefetch_buffer_size = prefetch_buffer_target_size.max(min_prefetch_buffer_size);

        // The current prefetch buffer size; we want to expand this by the target size.
        let current_prefetch_buffer_size = self.prefetched_byte_position - self.current_active_bytes();

        // If we're already at or above the target prefetch buffer size, then don't prefetch more
        // unless the queue is empty.
        if !self.prefetch_queue.is_empty() && prefetch_buffer_size <= current_prefetch_buffer_size {
            return;
        }

        // Let's see what we need to prefetch here.
        let next_prefetch_target_block_size = prefetch_buffer_size - current_prefetch_buffer_size;

        let min_prefetch_block_size = self.config.min_reconstruction_fetch_size;
        let max_prefetch_block_size = self.config.max_reconstruction_fetch_size;
        let next_prefetch_block_size =
            next_prefetch_target_block_size.clamp(min_prefetch_block_size, max_prefetch_block_size);

        // Okay, now add this to the prefetch queue.
        self.prefetch_block(next_prefetch_block_size).await?
    }

    async fn prefetch_block(&mut self, block_size: u64) -> Result<()> {
        // First, check the block range to see if we're over the requested range.
        let mut prefetch_block_range =
            FileRange::new(self.prefetched_byte_position, self.prefetched_byte_position + block_size);

        // Get the end of the
        let last_byte_position = self
            .known_final_byte_position
            .load(Ordering::Relaxed)
            .min(self.file_byte_range.end);

        // Clamp to the requested range.
        if prefetch_block_range.end > last_byte_position {
            prefetch_block_range.end = last_byte_position;
        }

        // Check if we should extend this one to the end.
        if prefetch_block_range.end + self.config.min_reconstruction_fetch_size > self.file_byte_range.end {
            prefetch_block_range.end = self.file_byte_range.end;
        }

        // It's possible that the start is actually at the end of the requested range; in that case, do nothing.
        if prefetch_block_range.start == prefetch_block_range.end {
            return Ok(());
        }

        // Add the prefetch task to the queue.
        let known_final_byte_position = self.known_final_byte_position.clone();

        let jh = tokio::task::spawn_blocking(async move {
            let result = ReconstructionTermBlock::retrieve_from_client(self.client.clone(), prefetch_block_range).await;

            // See if we're done with the file.
            if let Ok(Some(block)) = &result {
                // See if the returned range is less than the requested range; if so, then
                // we know we've reached the end of the file.
                debug_assert_eq!(block.byte_range.start, prefetch_block_range.start);

                if block.byte_range.end < prefetch_block_range.end {
                    known_final_byte_position.store(block.byte_range.end, Ordering::Relaxed);
                }
            } else if let Ok(None) = &result {
                // If it's None, then we're beyond the end of the file; update the known final byte position
                // to the start of the prefetch block if it hasn't been set yet (which it might have been in a
                // separate block).
                known_final_byte_position.fetch_min(prefetch_block_range.start, Ordering::Relaxed);
            }

            result
        });

        self.prefetch_queue.push_back(jh);

        Ok(())
    }
}

// The real guts of it.
