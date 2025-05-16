use std::future::Future;
use std::time::SystemTime;

use reqwest_middleware::Error;
use reqwest_retry::{RetryDecision, RetryPolicy, Retryable, RetryableStrategy};

use crate::http_client::get_retry_policy_and_strategy;
use crate::RetryConfig;

/// Executes a request-generating function with retry logic using a provided strategy and backoff policy.
///
/// This wrapper is intended for use around requests that cannot use the retry middleware for
/// whatever reason (E.g. reading data from streams).  It replicates the exact same logic in the
/// retry middleware by using the same policy and strategy structs used there.
///
/// # Parameters
///
/// - `create_request`: A closure that creates and executes the request, returning a future that resolves to a
///   `Result<reqwest::Response, reqwest_middleware::Error>`.
/// - `retry_config`: Configuration that defines retry behavior, including maximum retries, timing, and the retry
///   strategy.
///
/// # Returns
///
/// Returns `Ok(reqwest::Response)` on success, or the final `Err(reqwest_middleware::Error)` if
/// no further retries are allowed or the error is non-retryable.
///
/// # Example
/// ```rust
/// let result = reqwest_retry_wrapper(
///     || client.get("https://example.com").send(),
///     RetryConfig<DefaultRetryableStrategy>::default()
/// )
/// .await;
/// ```
pub async fn retry_wrapper<R, RequestFuture>(
    create_request: impl Fn() -> RequestFuture,
    retry_config: RetryConfig<R>,
) -> Result<reqwest::Response, Error>
where
    R: RetryableStrategy + Send + Sync,
    RequestFuture: Future<Output = Result<reqwest::Response, Error>>,
{
    let (retry_policy, strategy) = get_retry_policy_and_strategy(retry_config);
    let start_time = SystemTime::now();

    for attempt in 0.. {
        let result = create_request().await;

        // If all is ok, then return.
        if result.is_ok() {
            return result;
        }

        // Do we retry?
        if matches!(strategy.handle(&result), Some(Retryable::Transient)) {
            // Does our retry count / timing policy allow us to retry, and when?
            if let RetryDecision::Retry { execute_after } = retry_policy.should_retry(start_time, attempt) {
                // Retry after system time is a specific value.
                if let Ok(wait_dur) = execute_after.duration_since(SystemTime::now()) {
                    tokio::time::sleep(wait_dur).await;
                }
                continue;
            }
        }

        return result;
    }

    unreachable!("Retry loop should exit via return on success or final failure");
}
