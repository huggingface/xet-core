use std::time::Duration;

use utils::ByteSize;

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

    /// Send a report of a successful partial upload every 512kb.
    ///
    /// The default value is 524288.
    ///
    /// Use the environment variable `HF_XET_CLIENT_UPLOAD_REPORTING_BLOCK_SIZE` to set this value.
    ref upload_reporting_block_size : usize = 512 * 1024;

    /// Whether or not to enable the adaptive concurrency control.
    ///
    /// The default value is true.
    ///
    /// Use the environment variable `HF_XET_CLIENT_ENABLE_ADAPTIVE_CONCURRENCY` to set this value.
    ref enable_adaptive_concurrency: bool = true;

    /// The minimum time in milliseconds between adjustments when increasing the concurrency.
    ///
    /// The default value is 500ms.
    ///
    /// Use the environment variable `HF_XET_CLIENT_AC_MIN_ADJUSTMENT_WINDOW_MS` to set this value.
    ref ac_min_adjustment_window_ms: u64 = 500;

    /// The minimum number of bytes that must be observed before we start adjusting the concurrency.  This,
    /// along with ac_num_transmissions_required_for_adjustment, ensure that we have enough data points to
    /// accurately predict the RTT.  When these are too low, we observe that we can scale up the concurrency too quickly,
    /// causing a burst of failed connections at the start of a transfer.
    ///
    /// The default value is 20mb.
    ///
    /// Use the environment variable `HF_XET_CLIENT_AC_MIN_BYTES_REQUIRED_FOR_ADJUSTMENT` to set this value.
    ref ac_min_bytes_required_for_adjustment: ByteSize = ByteSize::from("20mb");

    /// The minimum number of completed transmissions that must be observed before we start adjusting the concurrency.
    /// This ensures we have enough data points to make reliable adjustments.
    ///
    /// The default value is 1, meaning we start adjusting the concurrency as soon as we have a completed transmission.
    ///
    /// Use the environment variable `HF_XET_CLIENT_AC_NUM_TRANSMISSIONS_REQUIRED_FOR_ADJUSTMENT` to set this value.
    ref ac_num_transmissions_required_for_adjustment: u64 = 1;

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
    /// Use the environment variable `HF_XET_CLIENT_AC_LATENCY_RTT_HALF_LIFE` to set this value.
    ref ac_latency_rtt_half_life: f64 = 64.0;

    /// Observations of deviance (the ratio of actual vs predicted latency) are tracked using
    /// exponentially weighted decay. This is parameterized by the half life in number of samples.
    /// Thus if this value is 8, it means that observations count for 50% weight after 8 samples, 25% weight
    /// after 16 samples, etc. This allows us adapt to changing network conditions more quickly and give more
    /// weight to newer observations, but still maintain history.
    ///
    /// The default value is 8.0.
    ///
    /// Use the environment variable `HF_XET_CLIENT_AC_SUCCESS_TRACKING_HALF_LIFE` to set this value.
    ref ac_success_tracking_half_life: f64 = 8.0;

    /// The target RTT (in seconds) for increasing concurrency.
    /// Concurrency is only increased if the predicted RTT is below this target.
    ///
    /// The default value is 60 seconds.
    ///
    /// Use the environment variable `HF_XET_CLIENT_AC_TARGET_RTT` to set this value.
    ref ac_target_rtt: Duration = Duration::from_secs(60);

    /// The maximum acceptable RTT (in seconds) for a transfer to be considered successful.
    /// Transfers taking longer than this are counted as failures.
    ///
    /// The default value is 90 seconds.
    ///
    /// Use the environment variable `HF_XET_CLIENT_AC_MAX_HEALTHY_RTT` to set this value.
    ref ac_max_healthy_rtt: Duration = Duration::from_secs(90);

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
    /// Use the environment variable `HF_XET_CLIENT_AC_RTT_SUCCESS_MAX_QUANTILE` to set this value.
    ref ac_rtt_success_max_quantile: f64 = 0.95;

    /// The success ratio threshold above which we increase concurrency.
    /// When the tracked success ratio exceeds this value, it indicates the connection
    /// is performing well and can handle more concurrency, provided the RTT is predicted to
    /// be below the target RTT.
    ///
    /// The default value is 0.8.
    ///
    /// Use the environment variable `HF_XET_CLIENT_AC_HEALTHY_SUCCESS_RATIO_THRESHOLD` to set this value.
    ref ac_healthy_success_ratio_threshold: f64 = 0.8;

    /// The success ratio threshold below which we decrease concurrency.
    /// When the tracked success ratio falls below this value, it indicates the connection
    /// is struggling and concurrency should be reduced.
    ///
    /// The default value is 0.5.
    ///
    /// Use the environment variable `HF_XET_CLIENT_AC_UNHEALTHY_SUCCESS_RATIO_THRESHOLD` to set this value.
    ref ac_unhealthy_success_ratio_threshold: f64 = 0.5;

    /// The reference size (64MB) used for bandwidth target checks.
    ///
    /// The default value is 64MB.
    ///
    /// Use the environment variable `HF_XET_CLIENT_AC_TARGET_RTT_TRANSMISSION_SIZE` to set this value.
    ref ac_target_rtt_transmission_size: u64 = 64 * 1024 * 1024;

    /// Log the concurrency on this interval.
    ///
    /// The default value is 10 seconds.
    ///
    /// Use the environment variable `HF_XET_CLIENT_AC_LOGGING_INTERVAL_MS` to set this value.
    ref ac_logging_interval_ms: u64 = 10 * 1000;

    /// The maximum number of simultaneous xorb and/or shard upload streams permitted by
    /// the adaptive concurrency control.
    ///
    /// The default value is 64.
    ///
    /// Use the environment variable `HF_XET_CLIENT_AC_MAX_UPLOAD_CONCURRENCY` to set this value.
    ref ac_max_upload_concurrency: usize = 64;

    /// The minimum number of simultaneous xorb and/or shard upload streams that the
    /// adaptive concurrency control may reduce the concurrency down to on slower connections.
    ///
    /// The default value is 2.
    ///
    /// Use the environment variable `HF_XET_CLIENT_AC_MIN_UPLOAD_CONCURRENCY` to set this value.
    ref ac_min_upload_concurrency: usize = 1;

    /// The starting number of concurrent upload streams, which will increase up to max_concurrent_uploads
    /// on successful completions.
    ///
    /// The default value is 2.
    ///
    /// Use the environment variable `HF_XET_CLIENT_AC_INITIAL_UPLOAD_CONCURRENCY` to set this value.
    ref ac_initial_upload_concurrency: usize = 1;

    /// The maximum number of simultaneous download streams permitted by
    /// the adaptive concurrency control.
    ///
    /// The default value is 64.
    ///
    /// Use the environment variable `HF_XET_CLIENT_AC_MAX_DOWNLOAD_CONCURRENCY` to set this value.
    ref ac_max_download_concurrency: usize = 64;

    /// The minimum number of simultaneous download streams that the
    /// adaptive concurrency control may reduce the concurrency down to on slower connections.
    ///
    /// The default value is 1.
    ///
    /// Use the environment variable `HF_XET_CLIENT_AC_MIN_DOWNLOAD_CONCURRENCY` to set this value.
    ref ac_min_download_concurrency: usize = 1;

    /// The starting number of concurrent download streams, which will increase up to max_concurrent_downloads
    /// on successful completions.
    ///
    /// The default value is 1.
    ///
    /// Use the environment variable `HF_XET_CLIENT_AC_INITIAL_DOWNLOAD_CONCURRENCY` to set this value.
    ref ac_initial_download_concurrency: usize = 1;

    /// Path to Unix domain socket for CAS HTTP connections.
    /// When set, all CAS HTTP traffic uses this socket instead of TCP.
    /// Only supported on Linux/macOS (not WASM).
    ///
    /// The default value is None (use TCP).
    ///
    /// Use the environment variable `HF_XET_CLIENT_UNIX_SOCKET_PATH` to set this value.
    ref unix_socket_path: Option<String> = None;

    /// The reconstruction API version to request from the CAS server.
    /// Version 1 returns per-range presigned URLs; version 2 returns
    /// per-xorb multi-range fetch descriptors.
    ///
    /// The default value is 2.
    ///
    /// Use the environment variable `HF_XET_CLIENT_RECONSTRUCTION_API_VERSION` to set this value.
    ref reconstruction_api_version: u32 = 2;

});
