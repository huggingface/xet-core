utils::configurable_constants! {

    /// Retry at most this many times before permanently failing.
    ref CLIENT_RETRY_MAX_ATTEMPTS : usize = 5;

    /// On errors that can be retried, delay for this amount of time
    /// before retrying.
    ref CLIENT_RETRY_BASE_DELAY_MS : u64 = 3000;

    /// After this much time has passed since the first attempt,
    /// no more retries are attempted.
    ref CLIENT_RETRY_MAX_DURATION_MS: u64 = 6 * 60 * 1000; // 6m

    /// The target time for a small transfer to complete.
    ref CONCURRENCY_CONTROL_TARGET_TIME_SMALL_TRANSFER_MS : u64 = 5 * 1000;

    /// The target time for a large transfer to complete.  Default is 20 seconds.
    ref CONCURRENCY_CONTROL_TARGET_TIME_LARGE_TRANSFER_MS : u64 = 20 * 1000;

    /// The size of a large transfer.
    ref CONCURRENCY_CONTROL_LARGE_TRANSFER_NUM_BYTES : u64 = 64_000_000;

    /// The minimum time in milliseconds between adjustments when increasing the concurrency.
    ref CONCURRENCY_CONTROL_MIN_INCREASE_WINDOW_MS : u64 = 500;

    /// The minimum time in milliseconds between adjustments when decreasing the concurrency.
    ref CONCURRENCY_CONTROL_MIN_DECREASE_WINDOW_MS : u64 = 250;

    /// The maximum number of connection successes and failures to examine when adjusting the concurrancy.
    ref CONCURRENCY_CONTROL_TRACKING_SIZE : usize = 32;

    /// The maximum time window within which to examine successes and failures when adjusting the concurrancy.
    ref CONCURRENCY_CONTROL_TRACKING_WINDOW_MS : u64 = 30 * 1000;

    /// Log the concurrency on this interval.
    ref CONCURRENCY_CONTROL_LOGGING_INTERVAL_MS: u64 = 10 * 1000;

    /// The maximum number of simultaneous xorb and/or shard upload streams permitted by
    /// the adaptive concurrency control. Can be overwritten by environment variable "HF_XET_MAX_CONCURRENT_UPLOADS".
    ref MAX_CONCURRENT_UPLOADS: usize = 100;

    /// The minimum number of simultaneous xorb and/or shard upload streams that the
    /// the adaptive concurrency control may reduce the concurrancy down to on slower connections.
    ref MIN_CONCURRENT_UPLOADS: usize = 2;

    /// The starting number of concurrent upload streams, which will increase up to MAX_CONCURRENT_UPLOADS
    /// on successful completions.
    ref NUM_INITIAL_CONCURRENT_UPLOADS: usize = 16;
}
