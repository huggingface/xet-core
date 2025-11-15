use std::time::Duration;

crate::config_group!({

    /// Retry at most this many times before permanently failing.
    ref retry_max_attempts : usize = 5;

    /// On errors that can be retried, delay for this amount of time
    /// before retrying.
    ref retry_base_delay : Duration = Duration::from_millis(3000);

    /// After this much time has passed since the first attempt,
    /// no more retries are attempted.
    ref retry_max_duration: Duration = Duration::from_secs(6 * 60);

    /// Cleanup idle connections that are unused for this amount of time.
    ref idle_connection_timeout: Duration = Duration::from_secs(60);

    /// Only no more than this number of idle connections in the connection pool.
    ref max_idle_connections: usize = 16;

    /// Number of concurrent range gets.
    /// Can be overwritten by environment variable "HF_XET_CAS_CLIENT_NUM_CONCURRENT_RANGE_GETS".
    /// Setting this value to 0 disables the limit, sets it to the max, this is not recommended as it may lead to errors.
    /// Default value: 48. High performance mode (enabled via HF_XET_HIGH_PERFORMANCE or HF_XET_HP)
    /// automatically sets this to 256 via XetConfig::with_high_performance().
    ref num_concurrent_range_gets: usize = 48;

    /// Send a report of a successful partial upload every 512kb.
    ref upload_reporting_block_size : usize = 512 * 1024;

    /// Env (HF_XET_RECONSTRUCT_WRITE_SEQUENTIALLY) to switch to writing terms sequentially to disk.
    /// Benchmarks have shown that on SSD machines, writing in parallel seems to far outperform
    /// sequential term writes.
    /// However, this is not likely the case for writing to HDD and may in fact be worse,
    /// so for those machines, setting this env may help download perf.
    ref reconstruct_write_sequentially : bool = false;

    /// Base value for the approximate number of ranges in the initial segment size used to download,
    /// where a segment is a range of a file that is downloaded.
    /// Can be overwritten by environment variable "HF_XET_CAS_CLIENT_NUM_RANGE_IN_SEGMENT_BASE".
    /// Setting this value to 0 causes no segments to be downloaded, this will cause downloads to fail/hang.
    /// Default value: 16 (16 * ~64MB -> ~1GB initial segment size).
    /// High performance mode (enabled via HF_XET_HIGH_PERFORMANCE or HF_XET_HP)
    /// automatically sets this to 128 (128 * ~64MB -> ~8GB initial segment size) via XetConfig::with_high_performance().
    ref num_range_in_segment_base: usize = 16;

    // Env (HF_XET_NUM_RANGE_IN_SEGMENT_DELTA) delta value for the approx number of ranges in a segment size
    // used to increase/decrease the segment size by this many approximate ranges
    // setting this value to 0 causes no segment size change, i.e. will remain constant
    ref num_range_in_segment_delta: usize = 1; // increase/decrease segment size by 1 approx range ~64MB

    // Env (HF_XET_NUM_RANGE_IN_SEGMENT_MAX) max value for the approx number of ranges in a segment size
    // setting this value to 0 will be ignored and the max size will be set to usize::MAX
    ref num_range_in_segment_max: usize = 400; // * ~64MB -> max at 25GB segment
});
