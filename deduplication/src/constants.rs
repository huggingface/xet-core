utils::test_configurable_constants! {
    /// This will target 1024 chunks per Xorb / CAS block
    ref DEDUP_TARGET_CHUNK_SIZE: usize = 64 * 1024;

    /// TARGET_CDC_CHUNK_SIZE / MINIMUM_CHUNK_DIVISOR is the smallest chunk size
    /// Note that this is not a threshold but a recommendation.
    /// Smaller chunks can be produced if size of a file is smaller than this number.
    ref DEDUP_MINIMUM_CHUNK_DIVISOR: usize = 8;

    /// TARGET_CDC_CHUNK_SIZE * MAXIMUM_CHUNK_MULTIPLIER is the largest chunk size
    /// Note that this is a limit.
    ref DEDUP_MAXIMUM_CHUNK_MULTIPLIER: usize = 2;

    /// The maximum number of bytes to go in a single xorb.
    ref DEDUP_MAX_XORB_BYTES: usize = 64 * 1024 * 1024;
}

lazy_static::lazy_static! {
    /// The maximum chunk size, calculated from the configurable constants above
    pub static ref MAX_CHUNK_SIZE: usize = (*DEDUP_TARGET_CHUNK_SIZE) * (*DEDUP_MAXIMUM_CHUNK_MULTIPLIER);
}
