utils::configurable_constants! {

    /// This will target 1024 chunks per Xorb / CAS block
    ref TARGET_CHUNK_SIZE: usize = release_fixed(64 * 1024);

    /// TARGET_CDC_CHUNK_SIZE / MINIMUM_CHUNK_DIVISOR is the smallest chunk size
    /// Note that this is not a threshold but a recommendation.
    /// Smaller chunks can be produced if size of a file is smaller than this number.
    ref MINIMUM_CHUNK_DIVISOR: usize = release_fixed(8);

    /// TARGET_CDC_CHUNK_SIZE * MAXIMUM_CHUNK_MULTIPLIER is the largest chunk size
    /// Note that this is a limit.
    ref MAXIMUM_CHUNK_MULTIPLIER: usize = release_fixed(2);

    /// The maximum number of bytes to go in a single xorb.
    ref MAX_XORB_BYTES: usize = release_fixed(64 * 1024 * 1024);

    /// The maximum number of chunks to go in a single xorb.
    /// Chunks are targeted at 64K, for ~1024 chunks per xorb, but
    /// can be much higher when there are a lot of small files.
    ref MAX_XORB_CHUNKS: usize = 8 * 1024;
}

lazy_static! {
    /// The maximum chunk size, calculated from the configurable constants above
    pub static ref MAX_CHUNK_SIZE: usize = (*TARGET_CHUNK_SIZE) * *(MAXIMUM_CHUNK_MULTIPLIER);
}
