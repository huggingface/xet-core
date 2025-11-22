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

    /// Maximum time allowed to establish a TCP connection to the server.
    /// This timeout applies only to connection establishment, not to data transfer.
    ///
    /// The default value is 60 seconds.
    ///
    /// Use the environment variable `HF_XET_CLIENT_CONNECT_TIMEOUT` to set this value.
    ref connect_timeout: Duration = Duration::from_secs(60);

    /// Maximum time allowed between receiving data packets during a transfer.
    /// This timeout resets whenever data is received, allowing slow but progressing
    /// transfers to complete. If no data is received for this duration, the connection
    /// is considered stalled and will timeout.
    ///
    /// The default value is 120 seconds.
    ///
    /// Use the environment variable `HF_XET_CLIENT_READ_TIMEOUT` to set this value.
    ref read_timeout: Duration = Duration::from_secs(120);

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

    /// The minimum time in milliseconds between adjustments when increasing the concurrency.
    ///
    /// The default value is 500ms.
    ///
    /// Use the environment variable `HF_XET_CLIENT_CONCURRENCY_MIN_INCREASE_WINDOW_MS` to set this value.
    ref concurrency_min_increase_window_ms: u64 = 500;

    /// The minimum time in milliseconds between adjustments when decreasing the concurrency.
    ///
    /// The default value is 250ms.
    ///
    /// Use the environment variable `HF_XET_CLIENT_CONCURRENCY_MIN_DECREASE_WINDOW_MS` to set this value.
    ref concurrency_min_decrease_window_ms: u64 = 250;

    /// Observations of observed transfer time and deviances are tracked using exponentially
    /// weighted decay. This is parameterized by the half life in number of samples.
    /// Thus if this value is 100, it means that observations count for 50% weight after 100 samples, 25% weight
    /// after 200 samples, etc. This allows us adapt to changing network conditions and give more
    /// weight to newer observations, but still maintain history.
    ///
    /// There are two things being tracked in this model; a prediction of the latency and a record of
    /// how accurate the model is.
    ///
    /// The default value is 64.0.
    ///
    /// Use the environment variable `HF_XET_CLIENT_CONCURRENCY_LATENCY_RTT_HALF_LIFE_COUNT` to set this value.
    ref concurrency_latency_rtt_half_life_count: f64 = 64.0;

    /// Observations of deviance (the ratio of actual vs predicted latency) are tracked using
    /// exponentially weighted decay. This is parameterized by the half life in number of samples.
    /// Thus if this value is 8, it means that observations count for 50% weight after 8 samples, 25% weight
    /// after 16 samples, etc. This allows us adapt to changing network conditions more quickly and give more
    /// weight to newer observations, but still maintain history.
    ///
    /// The default value is 8.0.
    ///
    /// Use the environment variable `HF_XET_CLIENT_CONCURRENCY_SUCCESS_TRACKING_HALF_LIFE_COUNT` to set this value.
    ref concurrency_success_tracking_half_life_count: f64 = 8.0;

    /// The target RTT (in seconds) for increasing concurrency.
    /// Concurrency is only increased if the predicted RTT is below this target.
    ///
    /// The default value is 60 seconds.
    ///
    /// Use the environment variable `HF_XET_CLIENT_CONCURRENCY_TARGET_RTT` to set this value.
    ref concurrency_target_rtt: Duration = Duration::from_secs(60);

    /// The maximum acceptable RTT (in seconds) for a transfer to be considered successful.
    /// Transfers taking longer than this are counted as failures.
    ///
    /// The default value is 90 seconds.
    ///
    /// Use the environment variable `HF_XET_CLIENT_CONCURRENCY_MAX_HEALTHY_RTT` to set this value.
    ref concurrency_max_healthy_rtt: Duration = Duration::from_secs(90);

    /// The RTT cutoff quantile used to determine success vs failure.
    /// A transmission is considered a success if it completes within a healthy time and
    /// it's within a reasonable margin of error from the predicted rtt for a packet of
    /// its size. If the rtt actual value is much higher than the predicted rtt, than it can
    /// indicate unacceptable congestion. Thus in this case, we count values where the actual
    /// is much higher than the predicted rtt to be a failure, even if they do succeed within a
    /// healthy total RTT value.
    ///
    /// The default value is 0.95.
    ///
    /// Use the environment variable `HF_XET_CLIENT_CONCURRENCY_RTT_SUCCESS_MAX_QUANTILE` to set this value.
    ref concurrency_rtt_success_max_quantile: f64 = 0.95;

    /// The success ratio threshold above which we increase concurrency.
    /// When the tracked success ratio exceeds this value, it indicates the connection
    /// is performing well and can handle more concurrency, provided the RTT is predicted to
    /// be below the target RTT.
    ///
    /// The default value is 0.8.
    ///
    /// Use the environment variable `HF_XET_CLIENT_CONCURRENCY_HEALTHY_SUCCESS_RATIO_THRESHOLD` to set this value.
    ref concurrency_healthy_success_ratio_threshold: f64 = 0.8;

    /// The success ratio threshold below which we decrease concurrency.
    /// When the tracked success ratio falls below this value, it indicates the connection
    /// is struggling and concurrency should be reduced.
    ///
    /// The default value is 0.5.
    ///
    /// Use the environment variable `HF_XET_CLIENT_CONCURRENCY_UNHEALTHY_SUCCESS_RATIO_THRESHOLD` to set this value.
    ref concurrency_unhealthy_success_ratio_threshold: f64 = 0.5;

    /// The reference size (64MB) used for bandwidth target checks.
    ///
    /// The default value is 64MB.
    ///
    /// Use the environment variable `HF_XET_CLIENT_CONCURRENCY_MAX_TRANSMISSION_SIZE` to set this value.
    ref concurrency_max_transmission_size: u64 = 64 * 1024 * 1024;

    /// Log the concurrency on this interval.
    ///
    /// The default value is 10 seconds.
    ///
    /// Use the environment variable `HF_XET_CLIENT_CONCURRENCY_LOGGING_INTERVAL_MS` to set this value.
    ref concurrency_logging_interval_ms: u64 = 10 * 1000;

    /// The maximum number of simultaneous xorb and/or shard upload streams permitted by
    /// the adaptive concurrency control.
    ///
    /// The default value is 64.
    ///
    /// Use the environment variable `HF_XET_CLIENT_MAX_CONCURRENT_UPLOADS` to set this value.
    ref max_concurrent_uploads: usize = 64;

    /// The minimum number of simultaneous xorb and/or shard upload streams that the
    /// adaptive concurrency control may reduce the concurrency down to on slower connections.
    ///
    /// The default value is 2.
    ///
    /// Use the environment variable `HF_XET_CLIENT_MIN_CONCURRENT_UPLOADS` to set this value.
    ref min_concurrent_uploads: usize = 1;

    /// The starting number of concurrent upload streams, which will increase up to max_concurrent_uploads
    /// on successful completions.
    ///
    /// The default value is 2.
    ///
    /// Use the environment variable `HF_XET_CLIENT_NUM_INITIAL_CONCURRENT_UPLOADS` to set this value.
    ref num_initial_concurrent_uploads: usize = 1;
});
