use std::time::Duration;

use utils::ByteSize;

crate::config_group!({

    /// The target shard size; shards.
    ///
    /// The default value is 67108864.
    ///
    /// Use the environment variable `HF_XET_MDB_SHARD_TARGET_SIZE` to set this value.
    ref target_size: u64 = 64 * 1024 * 1024;

    /// Maximum shard size; small shards are aggregated until they are at most this.
    ///
    /// The default value is 67108864.
    ///
    /// Use the environment variable `HF_XET_MDB_SHARD_MAX_TARGET_SIZE` to set this value.
    ref max_target_size: u64 = 64 * 1024 * 1024;

    /// The (soft) maximum size in bytes of the shard cache.  Default is 16 GB.
    ///
    /// As a rough calculation, a cache of size X will allow for dedup against data
    /// of size 1000 * X.  The default would allow a 16 TB repo to be deduped effectively.
    ///
    /// Note the cache is pruned to below this value at the beginning of a session,
    /// but during a single session new shards may be added such that this limit is exceeded.
    ///
    /// The default value is 16gb.
    ///
    /// Use the environment variable `HF_XET_MDB_SHARD_CACHE_SIZE_LIMIT` to set this value.
    ref cache_size_limit : ByteSize = ByteSize::from("16gb");

    /// The amount of time a shard should be expired by before it's deleted, in seconds.
    /// By default set to 7 days.
    ///
    /// The default value is 7day.
    ///
    /// Use the environment variable `HF_XET_MDB_SHARD_EXPIRATION_BUFFER` to set this value.
    ref expiration_buffer: Duration = Duration::from_secs(7 * 24 * 3600);

    /// The maximum size of the chunk index table that's stored in memory.  After this,
    /// no new chunks are loaded for deduplication.
    ///
    /// The default value is 67108864.
    ///
    /// Use the environment variable `HF_XET_MDB_SHARD_CHUNK_INDEX_TABLE_MAX_SIZE` to set this value.
    ref chunk_index_table_max_size: usize = 64 * 1024 * 1024;

    /// The expiration time of a local shard when first placed in the local shard cache.  Currently
    /// set to 3 weeks.
    ///
    /// The default value is 21day.
    ///
    /// Use the environment variable `HF_XET_MDB_SHARD_LOCAL_CACHE_EXPIRATION` to set this value.
    ref local_cache_expiration: Duration = Duration::from_secs(3 * 7 * 24 * 3600);
});
