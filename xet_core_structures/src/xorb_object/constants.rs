xet_runtime::test_configurable_constants! {
    /// This will target 1024 chunks per Xorb / CAS block
    ref TARGET_CHUNK_SIZE: usize = 64 * 1024;

    /// TARGET_CDC_CHUNK_SIZE / MINIMUM_CHUNK_DIVISOR is the smallest chunk size
    /// Note that this is not a threshold but a recommendation.
    /// Smaller chunks can be produced if size of a file is smaller than this number.
    ref MINIMUM_CHUNK_DIVISOR: usize = 8;

    /// TARGET_CDC_CHUNK_SIZE * MAXIMUM_CHUNK_MULTIPLIER is the largest chunk size
    /// Note that this is a limit.
    ref MAXIMUM_CHUNK_MULTIPLIER: usize = 2;

    /// The maximum number of bytes to go in a single xorb.
    ref MAX_XORB_BYTES: usize = 64 * 1024 * 1024;

    /// The maximum number of chunks to go in a single xorb.
    /// Chunks are targeted at 64K, for ~1024 chunks per xorb, but
    /// can be much higher when there are a lot of small files.
    ref MAX_XORB_CHUNKS: usize = 8 * 1024;

    /// Target 1024 chunks per XORB block
    ref XORB_BLOCK_SIZE: usize = 64 * 1024 * 1024;
}

lazy_static::lazy_static! {
    /// The maximum chunk size, calculated from the configurable constants above
    pub static ref MAX_CHUNK_SIZE: usize = (*TARGET_CHUNK_SIZE) * (*MAXIMUM_CHUNK_MULTIPLIER);

    /// The byte threshold at which to cut a new xorb during building.
    /// Defaults to MAX_XORB_BYTES, but in simulation builds can be lowered
    /// via the `simulation_max_bytes` xorb config value to produce
    /// smaller (but still valid) xorbs.
    pub static ref XORB_CUT_THRESHOLD_BYTES: usize = {
        #[cfg(feature = "simulation")]
        {
            xet_runtime::core::xet_config()
                .xorb
                .simulation_max_bytes
                .map(|bs| (bs.as_u64() as usize).min(*MAX_XORB_BYTES))
                .unwrap_or(*MAX_XORB_BYTES)
        }
        #[cfg(not(feature = "simulation"))]
        { *MAX_XORB_BYTES }
    };

    /// The chunk-count threshold at which to cut a new xorb during building.
    /// Defaults to MAX_XORB_CHUNKS, but in simulation builds can be lowered
    /// via the `simulation_max_xorb_chunks` xorb config value to produce
    /// smaller (but still valid) xorbs.
    pub static ref XORB_CUT_THRESHOLD_CHUNKS: usize = {
        #[cfg(feature = "simulation")]
        {
            xet_runtime::core::xet_config()
                .xorb
                .simulation_max_chunks
                .unwrap_or(*MAX_XORB_CHUNKS)
                .min(*MAX_XORB_CHUNKS)
        }
        #[cfg(not(feature = "simulation"))]
        { *MAX_XORB_CHUNKS }
    };
}
