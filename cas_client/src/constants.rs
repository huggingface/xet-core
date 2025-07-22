utils::configurable_constants! {

    /// Retry at most this many times before permanently failing.
    ref CLIENT_RETRY_MAX_ATTEMPTS : usize = 5;

    /// On errors that can be retried, delay for this amount of time
    /// before retrying.
    ref CLIENT_RETRY_BASE_DELAY_MS : u64 = 3000;

    /// After this much time has passed since the first attempt,
    /// no more retries are attempted.
    ref CLIENT_RETRY_MAX_DURATION_MS: u64 = 6 * 60 * 1000; // 6m

    /// The maximum amount of time for a transfer to be deamed within target.  Set to 45 sec.
    ref CONCURRENCY_CONTROL_MAX_WITHIN_TARGET_TRANSFER_TIME_MS: u64 = 45 * 1000;

    /// The minimum time in milliseconds between adjustments when increasing the concurrency.
    ref CONCURRENCY_CONTROL_MIN_INCREASE_WINDOW_MS : u64 = 500;

    /// The minimum time in milliseconds between adjustments when decreasing the concurrency.
    ref CONCURRENCY_CONTROL_MIN_DECREASE_WINDOW_MS : u64 = 250;

    /// Observations of observed transfer time and deviances are tracked using exponentially
    /// weighted decay.  This is parameterized by the half life of a weighting for an observation.
    /// Thus if this value is 30 sec, it means that observations count for 50% weight after 30 seconds, 25% weight
    /// after 1 min, etc.  This allows us adapt to changing network conditions and give more
    /// weight to newer observations, but still maintain history.
    ///
    /// There are two things being tracked in this model; a prediction of the latency and a record of
    /// how accurate the model is.  The primary assumption here is the following:
    ///
    ///
    ref CONCURRENCY_CONTROL_LATENCY_TRACKING_HALF_LIFE_MS : u64 = 30 * 1000;
    ref CONCURRENCY_CONTROL_SUCCESS_TRACKING_HALF_LIFE_MS : u64 = 10 * 1000;


    ref CONCURRENCY_CONTROL_DEVIANCE_TARGET_SPREAD : f64 = 0.05;
    ref CONCURRENCY_CONTROL_DEVIANCE_MAX_SPREAD : f64 = 0.3;

    /// Log the concurrency on this interval.
    ref CONCURRENCY_CONTROL_LOGGING_INTERVAL_MS: u64 = 10 * 1000;

    /// The maximum number of simultaneous xorb and/or shard upload streams permitted by
    /// the adaptive concurrency control. Can be overwritten by environment variable "HF_XET_MAX_CONCURRENT_UPLOADS".
    ref MAX_CONCURRENT_UPLOADS: usize = 100;

    /// The minimum number of simultaneous xorb and/or shard upload streams that the
    /// the adaptive concurrency control may reduce the concurrency down to on slower connections.
    ref MIN_CONCURRENT_UPLOADS: usize = 2;

    /// The starting number of concurrent upload streams, which will increase up to MAX_CONCURRENT_UPLOADS
    /// on successful completions.
    ref NUM_INITIAL_CONCURRENT_UPLOADS: usize = 4;
}
