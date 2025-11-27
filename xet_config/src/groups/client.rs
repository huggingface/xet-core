use std::time::Duration;

crate::config_group!({

    /// Retry at most this many times before permanently failing.
    ///
    /// The default value is 5.
    ///
    /// Use the environment variable `HF_XET_CLIENT_RETRY_MAX_ATTEMPTS` to set this value.
    ref retry_max_attempts : usize = 5;

    /// On errors that can be retried, delay for this amount of time
    /// before retrying.
    ///
    /// The default value is 3sec.
    ///
    /// Use the environment variable `HF_XET_CLIENT_RETRY_BASE_DELAY` to set this value.
    ref retry_base_delay : Duration = Duration::from_millis(3000);

    /// After this much time has passed since the first attempt,
    /// no more retries are attempted.
    ///
    /// The default value is 6min.
    ///
    /// Use the environment variable `HF_XET_CLIENT_RETRY_MAX_DURATION` to set this value.
    ref retry_max_duration: Duration = Duration::from_secs(6 * 60);

    /// Cleanup idle connections that are unused for this amount of time.
    ///
    /// The default value is 60sec.
    ///
    /// Use the environment variable `HF_XET_CLIENT_IDLE_CONNECTION_TIMEOUT` to set this value.
    ref idle_connection_timeout: Duration = Duration::from_secs(60);

    /// Only no more than this number of idle connections in the connection pool.
    ///
    /// The default value is 16.
    ///
    /// Use the environment variable `HF_XET_CLIENT_MAX_IDLE_CONNECTIONS` to set this value.
    ref max_idle_connections: usize = 16;

    /// Number of concurrent range gets.
    /// Setting this value to 0 disables the limit, sets it to the max, this is not recommended as it may lead to errors.
    /// High performance mode (enabled via HF_XET_HIGH_PERFORMANCE or HF_XET_HP)
    /// automatically sets this to 256 via XetConfig::with_high_performance().
    ///
    /// The default value is 48.
    ///
    /// Use the environment variable `HF_XET_CLIENT_NUM_CONCURRENT_RANGE_GETS` to set this value.
    ref num_concurrent_range_gets: usize = 48;

    /// Send a report of a successful partial upload every 512kb.
    ///
    /// The default value is 524288.
    ///
    /// Use the environment variable `HF_XET_CLIENT_UPLOAD_REPORTING_BLOCK_SIZE` to set this value.
    ref upload_reporting_block_size : usize = 512 * 1024;

    /// Switch to writing terms sequentially to disk.
    /// Benchmarks have shown that on SSD machines, writing in parallel seems to far outperform
    /// sequential term writes.
    /// However, this is not likely the case for writing to HDD and may in fact be worse,
    /// so for those machines, setting this env may help download perf.
    ///
    /// The default value is false.
    ///
    /// Use the environment variable `HF_XET_CLIENT_RECONSTRUCT_WRITE_SEQUENTIALLY` to set this value.
    ref reconstruct_write_sequentially : bool = false;

    /// Base value for the approximate number of ranges in the initial segment size used to download,
    /// where a segment is a range of a file that is downloaded.
    /// Setting this value to 0 causes no segments to be downloaded, this will cause downloads to fail/hang.
    /// High performance mode (enabled via HF_XET_HIGH_PERFORMANCE or HF_XET_HP)
    /// automatically sets this to 128 via XetConfig::with_high_performance().
    ///
    /// The default value is 16.
    ///
    /// Use the environment variable `HF_XET_CLIENT_NUM_RANGE_IN_SEGMENT_BASE` to set this value.
    ref num_range_in_segment_base: usize = 16;

    /// Delta value for the approx number of ranges in a segment size
    /// used to increase/decrease the segment size by this many approximate ranges
    /// setting this value to 0 causes no segment size change, i.e. will remain constant
    ///
    /// The default value is 1.
    ///
    /// Use the environment variable `HF_XET_CLIENT_NUM_RANGE_IN_SEGMENT_DELTA` to set this value.
    ref num_range_in_segment_delta: usize = 1; // increase/decrease segment size by 1 approx range ~64MB

    /// Max value for the approx number of ranges in a segment size
    /// setting this value to 0 will be ignored and the max size will be set to usize::MAX
    ///
    /// The default value is 400.
    ///
    /// Use the environment variable `HF_XET_CLIENT_NUM_RANGE_IN_SEGMENT_MAX` to set this value.
    ref num_range_in_segment_max: usize = 400; // * ~64MB -> max at 25GB segment
});
