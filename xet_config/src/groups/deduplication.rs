crate::config_group!({
    /// Enable deduplication; if false, then deduplication is disabled.  Note that
    /// this may result in significantly higher data and memory usage.
    ref enable_deduplication: bool = true;


    /// Number of ranges to use when estimating fragmentation
    ///
    /// The default value is 128.
    ///
    /// Use the environment variable `HF_XET_DEDUPLICATION_NRANGES_IN_STREAMING_FRAGMENTATION_ESTIMATOR` to set this value.
    ref nranges_in_streaming_fragmentation_estimator: usize = 128;

    /// Minimum number of chunks per range. Used to control fragmentation
    /// This targets an average of 1MB per range.
    /// The hysteresis factor multiplied by the target Chunks Per Range (CPR) controls
    /// the low end of the hysteresis range. Basically, dedupe will stop
    /// when CPR drops below hysteresis * target_cpr, and will start again when
    /// CPR increases above target CPR.
    ///
    /// The default value is 0.5.
    ///
    /// Use the environment variable `HF_XET_DEDUPLICATION_MIN_N_CHUNKS_PER_RANGE_HYSTERESIS_FACTOR` to set this value.
    ref min_n_chunks_per_range_hysteresis_factor: f32 = 0.5;

    /// Minimum number of chunks per range.
    ///
    /// The default value is 8.0.
    ///
    /// Use the environment variable `HF_XET_DEDUPLICATION_MIN_N_CHUNKS_PER_RANGE` to set this value.
    ref min_n_chunks_per_range: f32 = 8.0;
});
