crate::config_group!({
    /// Number of ranges to use when estimating fragmentation
    ref nranges_in_streaming_fragmentation_estimator: usize = 128;

    /// Minimum number of chunks per range. Used to control fragmentation
    /// This targets an average of 1MB per range.
    /// The hysteresis factor multiplied by the target Chunks Per Range (CPR) controls
    /// the low end of the hysteresis range. Basically, dedupe will stop
    /// when CPR drops below hysteresis * target_cpr, and will start again when
    /// CPR increases above target CPR.
    ref min_n_chunks_per_range_hysteresis_factor: f32 = 0.5;
    ref min_n_chunks_per_range: f32 = 8.0;
});
