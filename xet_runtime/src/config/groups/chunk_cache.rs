use crate::utils::ConfigEnum;

#[cfg(not(feature = "no-default-cache"))]
pub const DEFAULT_CHUNK_CACHE_CAPACITY: u64 = 10_000_000_000; // 10 GB
#[cfg(feature = "no-default-cache")]
pub const DEFAULT_CHUNK_CACHE_CAPACITY: u64 = 0;
pub const DEFAULT_CHUNK_CACHE_ACCESS_UPDATE_INTERVAL_NS: u64 = 1_000_000_000; // 1 second

crate::config_group!({
    /// Default chunk cache capacity: 10 GB or 0 depends on the build feature
    ///
    /// Use the environment variable `HF_XET_CHUNK_CACHE_SIZE_BYTES` to set this value.
    ref size_bytes: u64 = DEFAULT_CHUNK_CACHE_CAPACITY;

    /// Eviction policy used when the chunk cache exceeds its configured capacity.
    /// Valid values: "random", "lru".
    ///
    /// Use the environment variable `HF_XET_CHUNK_CACHE_EVICTION_POLICY` to set this value.
    ref eviction_policy: ConfigEnum = ConfigEnum::new("random", &["random", "lru"]);

    /// Minimum time in nanoseconds between persisted access timestamp updates for the same LRU cache item.
    /// Set this to 0 to record every access.
    ///
    /// Use the environment variable `HF_XET_CHUNK_CACHE_ACCESS_UPDATE_INTERVAL_NS` to set this value.
    ref access_update_interval_ns: u64 = DEFAULT_CHUNK_CACHE_ACCESS_UPDATE_INTERVAL_NS;
});
