crate::config_group!({
    /// Default chunk cache capacity: 10 GB
    /// This matches chunk_cache::disk::DEFAULT_CHUNK_CACHE_CAPACITY
    ///
    /// The default value is 10000000000.
    ///
    /// Use the environment variable `HF_XET_CHUNK_CACHE_SIZE_BYTES` to set this value.
    ref size_bytes: u64 = 10_000_000_000;
});
