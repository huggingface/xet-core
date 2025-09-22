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

    /// Cleanup idle connections that are unused for this amount of time.
    ref CLIENT_IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);

    /// Only no more than this number of idle connections in the connection pool.
    ref CLIENT_MAX_IDLE_CONNECTIONS: usize = 16;
}
