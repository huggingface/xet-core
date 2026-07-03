crate::config_group!({
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

    /// Share of processed chunks with a dedup match on offer above which defrag
    /// prevention is bypassed. Skipping dedup only reduces fragmentation when the
    /// re-stored chunks merge with neighboring runs of new data; on mostly-matchable
    /// content (a re-upload of something already stored), skipping buys no contiguity
    /// and only duplicates storage. Set to a value > 1.0 to never bypass (previous
    /// behavior).
    ///
    /// The default value is 0.7.
    ///
    /// Use the environment variable `HF_XET_DEDUPLICATION_DEFRAG_PREVENTION_MATCHABLE_DENSITY_BYPASS` to set this value.
    ref defrag_prevention_matchable_density_bypass: f32 = 0.7;

    /// Whether to enable global deduplication queries to the server.
    /// When enabled, the system will query the server for deduplication shards
    /// based on chunk hashes to enable cross-repository deduplication.
    ///
    /// The default value is true.
    ///
    /// Use the environment variable `HF_XET_DEDUPLICATION_GLOBAL_DEDUP_QUERY_ENABLED` to set this value.
    ref global_dedup_query_enabled: bool = true;
});
