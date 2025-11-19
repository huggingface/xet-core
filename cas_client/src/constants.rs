use std::time::Duration;

utils::configurable_constants! {

    /// Retry at most this many times before permanently failing.
    ref CLIENT_RETRY_MAX_ATTEMPTS : usize = 5;

    /// On errors that can be retried, delay for this amount of time
    /// before retrying.
    ref CLIENT_RETRY_BASE_DELAY : Duration = Duration::from_millis(3000);

    /// After this much time has passed since the first attempt,
    /// no more retries are attempted.
    ref CLIENT_RETRY_MAX_DURATION: Duration = Duration::from_secs(6 * 60);

    /// The minimum time in milliseconds between adjustments when increasing the concurrency.
    ref CONCURRENCY_CONTROL_MIN_INCREASE_WINDOW_MS : u64 = 500;

    /// The minimum time in milliseconds between adjustments when decreasing the concurrency.
    ref CONCURRENCY_CONTROL_MIN_DECREASE_WINDOW_MS : u64 = 250;

    /// Observations of observed transfer time and deviances are tracked using exponentially
    /// weighted decay.  This is parameterized by the half life in number of samples.
    /// Thus if this value is 100, it means that observations count for 50% weight after 100 samples, 25% weight
    /// after 200 samples, etc.  This allows us adapt to changing network conditions and give more
    /// weight to newer observations, but still maintain history.
    ///
    /// There are two things being tracked in this model; a prediction of the latency and a record of
    /// how accurate the model is.  The primary assumption here is the following:
    ref CONCURRENCY_CONTROL_LATENCY_RTT_HALF_LIFE_COUNT : f64 = 64.0;

    /// Observations of deviance (the ratio of actual vs predicted latency) are tracked using
    /// exponentially weighted decay.  This is parameterized by the half life in number of samples.
    /// Thus if this value is 8, it means that observations count for 50% weight after 8 samples, 25% weight
    /// after 16 samples, etc.  This allows us adapt to changing network conditions more quicklyand give more
    /// weight to newer observations, but still maintain history.
    ref CONCURRENCY_CONTROL_SUCCESS_TRACKING_HALF_LIFE_COUNT : f64 = 8.0;

    /// The target RTT (in seconds) for increasing concurrency.
    /// Concurrency is only increased if the predicted RTT is below this target.
    ref CONCURRENCY_CONTROL_TARGET_RTT: Duration  = Duration::from_secs(60);

    /// The maximum acceptable RTT (in seconds) for a transfer to be considered successful.
    /// Transfers taking longer than this are counted as failures.
    ref CONCURRENCY_CONTROL_MAX_HEALTHY_RTT: Duration = Duration::from_secs(90);

    /// The RTT cutoff quantile used to determine success vs failure.
    /// A transmission is considered a success if it completes within a healthy time and
    /// it's within a reasonable margin of error from the predicted rtt for a packet of
    /// its size.  If the rtt actual value is much higher than the predicted rtt, than it can
    /// indicate unaccepable congestion.  Thus in this case, we count values where the actual
    /// is much higher than the predicted rtt to be a failure, even if they do succeed within a
    /// healthy total RTT value.
    ref CONCURRENCY_CONTROL_RTT_SUCCESS_MAX_QUANTILE : f64 = 0.95;

    /// The success ratio threshold above which we increase concurrency.
    /// When the tracked success ratio exceeds this value, it indicates the connection
    /// is performing well and can handle more concurrency, provided the RTT is predicted to
    /// be below the target RTT.
    ref CONCURRENCY_CONTROL_HEALTHY_SUCCESS_RATIO_THRESHOLD : f64 = 0.8;

    /// The success ratio threshold below which we decrease concurrency.
    /// When the tracked success ratio falls below this value, it indicates the connection
    /// is struggling and concurrency should be reduced.
    ref CONCURRENCY_CONTROL_UNHEALTHY_SUCCESS_RATIO_THRESHOLD : f64 = 0.5;

    /// The reference size (64MB) used for bandwidth target checks.
    ref CONCURRENCY_CONTROL_MAX_TRANSMISSION_SIZE: u64 = 64 * 1024 * 1024;

    /// Log the concurrency on this interval.
    ref CONCURRENCY_CONTROL_LOGGING_INTERVAL_MS: u64 = 10 * 1000;

    /// The maximum number of simultaneous xorb and/or shard upload streams permitted by
    /// the adaptive concurrency control. Can be overwritten by environment variable "HF_XET_MAX_CONCURRENT_UPLOADS".
    ref MAX_CONCURRENT_UPLOADS: usize = 64;

    /// The minimum number of simultaneous xorb and/or shard upload streams that the
    /// the adaptive concurrency control may reduce the concurrency down to on slower connections.
    ref MIN_CONCURRENT_UPLOADS: usize = 2;

    /// The starting number of concurrent upload streams, which will increase up to MAX_CONCURRENT_UPLOADS
    /// on successful completions.
    ref NUM_INITIAL_CONCURRENT_UPLOADS: usize = 2;

    /// Cleanup idle connections that are unused for this amount of time.
    ref CLIENT_IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);

    /// Only no more than this number of idle connections in the connection pool.
    ref CLIENT_MAX_IDLE_CONNECTIONS: usize = 16;

    /// Maximum time allowed to establish a TCP connection to the server.
    /// This timeout applies only to connection establishment, not to data transfer.
    ref CLIENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(60);

    /// Maximum time allowed between receiving data packets during a transfer.
    /// This timeout resets whenever data is received, allowing slow but progressing
    /// transfers to complete. If no data is received for this duration, the connection
    /// is considered stalled and will timeout.
    ref CLIENT_READ_TIMEOUT: Duration = Duration::from_secs(120);
}
