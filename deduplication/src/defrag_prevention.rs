use std::collections::VecDeque;

use xet_runtime::xet_config;

pub(crate) struct DefragPrevention {
    /// This tracks the number of chunks in each of the last N ranges
    rolling_last_nranges: VecDeque<usize>,

    /// This tracks the total number of chunks
    rolling_nranges_chunks: usize,

    /// Used to provide some hysteresis on the defrag decision
    /// chooses between MIN_N_CHUNKS_PER_RANGE
    /// or MIN_N_CHUNKS_PER_RANGE * HYSTERESIS_FACTOR (hysteresis factor < 1.0)
    defrag_at_low_threshold: bool,

    /// The minimum number of chunks per range to consider deduplication.
    min_chunks_per_range: f32,

    /// The minimum number of chunks per range to consider deduplication.
    min_chunks_per_range_historesis_factor: f32,
}

impl DefragPrevention {
    pub(crate) fn increment_last_range_in_fragmentation_estimate(&mut self, nchunks: usize) {
        if let Some(back) = self.rolling_last_nranges.back_mut() {
            *back += nchunks;
            self.rolling_nranges_chunks += nchunks;
        }
    }
    pub(crate) fn add_range_to_fragmentation_estimate(&mut self, nchunks: usize) {
        self.rolling_last_nranges.push_back(nchunks);
        self.rolling_nranges_chunks += nchunks;
        if self.rolling_last_nranges.len() > xet_config().deduplication.nranges_in_streaming_fragmentation_estimator {
            self.rolling_nranges_chunks -= self.rolling_last_nranges.pop_front().unwrap();
        }
    }
    /// Returns the average number of chunks per range
    /// None if there is is not enough data for an estimate
    pub(crate) fn rolling_chunks_per_range(&self) -> Option<f32> {
        if self.rolling_last_nranges.len() < xet_config().deduplication.nranges_in_streaming_fragmentation_estimator {
            None
        } else {
            Some(self.rolling_nranges_chunks as f32 / self.rolling_last_nranges.len() as f32)
        }
    }

    /// Check to see if we should update against this entry or continue from the previous one?
    pub(crate) fn allow_dedup_on_next_range(&mut self, dedup_range_size: usize) -> bool {
        let Some(chunks_per_range) = self.rolling_chunks_per_range() else {
            return true;
        };

        let target_cpr = if self.defrag_at_low_threshold {
            self.min_chunks_per_range * self.min_chunks_per_range_historesis_factor
        } else {
            self.min_chunks_per_range
        };

        if chunks_per_range < target_cpr {
            // chunks per range is pretty poor, we should not dedupe.
            // However, here we do get to look ahead a little bit
            // and check the size of the next dedupe window.
            // if it is too small, it is not going to improve
            // the chunks per range and so we skip it.
            if (dedup_range_size as f32) < chunks_per_range {
                // once I start skipping dedupe, we try to raise
                // the cpr to the high threshold
                self.defrag_at_low_threshold = false;
                return false;
            }
        } else {
            // once I start deduping again, we lower CPR
            // to the low threshold so we allow for more small
            // fragments.
            self.defrag_at_low_threshold = true;
        }

        true
    }
}

impl Default for DefragPrevention {
    fn default() -> Self {
        Self {
            rolling_last_nranges: VecDeque::with_capacity(
                xet_config().deduplication.nranges_in_streaming_fragmentation_estimator,
            ),
            rolling_nranges_chunks: 0,
            defrag_at_low_threshold: true,
            min_chunks_per_range: xet_config().deduplication.min_n_chunks_per_range,
            min_chunks_per_range_historesis_factor: xet_config().deduplication.min_n_chunks_per_range_hysteresis_factor,
        }
    }
}
