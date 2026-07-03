use std::collections::VecDeque;

use xet_runtime::core::XetContext;

/// The matchable-density bypass only engages once this many chunks have been
/// processed, so a file's first few MB can't flip it on off a tiny sample.
const MATCHABLE_DENSITY_WARMUP_CHUNKS: u64 = 512;

pub(crate) struct DefragPrevention {
    nranges_in_streaming_fragmentation_estimator: usize,

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

    /// Chunks settled so far, and how many of them had a dedup match on offer
    /// (whether the throttle accepted or rejected it). Their ratio is the
    /// matchable density: above `matchable_density_bypass`, the throttle yields
    /// (see `allow_dedup_on_next_range`).
    processed_chunks: u64,
    offered_match_chunks: u64,

    /// Matchable-density threshold above which the throttle is bypassed
    /// (`deduplication.defrag_prevention_matchable_density_bypass`).
    matchable_density_bypass: f64,
}

impl DefragPrevention {
    pub(crate) fn new(ctx: &XetContext) -> Self {
        let d = &ctx.config.deduplication;
        Self {
            nranges_in_streaming_fragmentation_estimator: d.nranges_in_streaming_fragmentation_estimator,
            rolling_last_nranges: VecDeque::with_capacity(d.nranges_in_streaming_fragmentation_estimator),
            rolling_nranges_chunks: 0,
            defrag_at_low_threshold: true,
            min_chunks_per_range: d.min_n_chunks_per_range,
            min_chunks_per_range_historesis_factor: d.min_n_chunks_per_range_hysteresis_factor,
            processed_chunks: 0,
            offered_match_chunks: 0,
            matchable_density_bypass: d.defrag_prevention_matchable_density_bypass as f64,
        }
    }

    pub(crate) fn increment_last_range_in_fragmentation_estimate(&mut self, nchunks: usize) {
        self.processed_chunks += nchunks as u64;
        if let Some(back) = self.rolling_last_nranges.back_mut() {
            *back += nchunks;
            self.rolling_nranges_chunks += nchunks;
        }
    }
    pub(crate) fn add_range_to_fragmentation_estimate(&mut self, nchunks: usize) {
        self.processed_chunks += nchunks as u64;
        self.rolling_last_nranges.push_back(nchunks);
        self.rolling_nranges_chunks += nchunks;
        if self.rolling_last_nranges.len() > self.nranges_in_streaming_fragmentation_estimator {
            self.rolling_nranges_chunks -= self.rolling_last_nranges.pop_front().unwrap();
        }
    }

    /// Record that a range of `nchunks` was settled as a dedup reference. The
    /// range still flows through the fragmentation estimate via the range
    /// methods above; this only feeds the matchable-density numerator.
    pub(crate) fn record_matched_range(&mut self, nchunks: usize) {
        self.offered_match_chunks += nchunks as u64;
    }

    /// Returns the average number of chunks per range
    /// None if there is is not enough data for an estimate
    pub(crate) fn rolling_chunks_per_range(&self) -> Option<f32> {
        if self.rolling_last_nranges.len() < self.nranges_in_streaming_fragmentation_estimator {
            None
        } else {
            Some(self.rolling_nranges_chunks as f32 / self.rolling_last_nranges.len() as f32)
        }
    }

    /// Share of settled chunks that had a dedup match on offer is high enough
    /// that skipping dedup cannot pay off: the re-stored chunks would sit
    /// between other matchable ranges instead of merging with runs of new
    /// data, duplicating storage with no contiguity gain.
    fn matchable_density_high(&self) -> bool {
        self.processed_chunks > MATCHABLE_DENSITY_WARMUP_CHUNKS
            && self.offered_match_chunks as f64 > self.matchable_density_bypass * self.processed_chunks as f64
    }

    /// Check to see if we should update against this entry or continue from the previous one?
    pub(crate) fn allow_dedup_on_next_range(&mut self, dedup_range_size: usize) -> bool {
        // On mostly-matchable content (a re-upload of something already stored)
        // the throttle degenerates into alternating accepted/rejected ranges:
        // storage is duplicated while the layout stays fragmented. Bypass it,
        // leaving the hysteresis state untouched so the throttle resumes from
        // where it was if density falls back below the threshold.
        if self.matchable_density_high() {
            return true;
        }

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
                // The rejected offer settles exactly one chunk as new data (the
                // caller re-offers the rest of the range next iteration); count
                // it here, at the decision, so the density keeps seeing content
                // as matchable while the throttle is re-storing it.
                self.offered_match_chunks += 1;
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
