#[cfg(not(feature = "no-default-cache"))]
pub const DEFAULT_CHUNK_CACHE_CAPACITY: u64 = 10_000_000_000; // 10 GB
#[cfg(feature = "no-default-cache")]
pub const DEFAULT_CHUNK_CACHE_CAPACITY: u64 = 0;

crate::config_group!({
    /// Default chunk cache capacity: 10 GB or 0 depends on the build feature
    ///
    /// Use the environment variable `HF_XET_CHUNK_CACHE_SIZE_BYTES` to set this value.
    ref size_bytes: u64 = DEFAULT_CHUNK_CACHE_CAPACITY;

    /// Subdirectory name for chunk cache within the endpoint cache directory.
    ///
    /// The default value is "chunk-cache".
    ///
    /// Use the environment variable `HF_XET_CHUNK_CACHE_SUBDIR` to set this value.
    ref subdir: String = "chunk-cache".to_string();
});
