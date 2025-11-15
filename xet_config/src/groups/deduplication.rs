crate::config_group!({
    /// The maximum number of chunks to go in a single xorb.
    /// Chunks are targeted at 64K, for ~1024 chunks per xorb, but
    /// can be much higher when there are a lot of small files.
    ref max_xorb_chunks: usize = 8 * 1024;

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
