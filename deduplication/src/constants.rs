utils::configurable_constants! {


    /// The maximum number of bytes to go in a single xorb.
    ref MAX_XORB_BYTES: usize = 64 * 1024 * 1024;

    /// The maximum number of chunks to go in a single xorb.
    /// Chunks are targeted at 64K, for ~1024 chunks per xorb, but
    /// can be much higher when there are a lot of small files.
    ref MAX_XORB_CHUNKS: usize = 8 * 1024;
}
