crate::config_group!({
    // Default chunk cache capacity: 10 GB
    // This matches chunk_cache::disk::DEFAULT_CHUNK_CACHE_CAPACITY
    ref size_bytes: u64 = 10_000_000_000;
});
