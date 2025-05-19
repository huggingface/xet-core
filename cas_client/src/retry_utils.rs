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
/// let result = reqwest_retry_wrapper(
///     || client.get("https://example.com").send(),
///     RetryConfig<DefaultRetryableStrategy>::default()
/// )
/// .await;
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

        // Do we retry?
        if matches!(strategy.handle(&result), Some(Retryable::Transient)) {
            // Does our retry count / timing policy allow us to retry, and when?
            if let RetryDecision::Retry { execute_after } = retry_policy.should_retry(start_time, attempt) {
                // Retry after system time is a specific value.
                if let Ok(wait_dur) = execute_after.duration_since(SystemTime::now()) {
                    tokio::time::sleep(wait_dur).await;
                }
                continue;
            } else {
                return match result {
                    Err(e) => Err(e),
                    Ok(v) => Err(Error::Middleware(anyhow::anyhow!(
                        "retry limit exceeded after {attempt} attempts (status = {}, value = {v:?}) ",
                        v.status()
                    ))),
                };
            }
        }

        return result;
    }

    unreachable!("Retry loop should exit via return on success or final failure");
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    use reqwest::{Client, Response};
    use reqwest_middleware::Error;
    use reqwest_retry::{Retryable, RetryableStrategy};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    /// Simple strategy that retries on any 5xx or reqwest error.
    #[derive(Clone)]
    struct RetryOn5xx {
        call_counter: Arc<AtomicU32>,
    }

    impl RetryableStrategy for RetryOn5xx {
        fn handle(&self, result: &Result<Response, Error>) -> Option<Retryable> {
            self.call_counter.fetch_add(1, Ordering::SeqCst);
            match result {
                Ok(resp) if resp.status().is_server_error() => Some(Retryable::Transient),
                Err(_) => Some(Retryable::Transient),
                _ => None,
            }
        }
    }

    fn make_retry_config(strategy: RetryOn5xx, num_retries: u32) -> RetryConfig<RetryOn5xx> {
        RetryConfig {
            num_retries,
            min_retry_interval_ms: 1,
            max_retry_interval_ms: 50,
            strategy,
        }
    }

    #[tokio::test]
    async fn test_success_first_try() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/success"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let client = Client::new();
        let counter = Arc::new(AtomicU32::new(0));
        let strategy = RetryOn5xx {
            call_counter: counter.clone(),
        };

        let result = retry_wrapper(
            || {
                let url = format!("{}/success", server.uri());
                let client = client.clone();
                async move { client.get(&url).send().await.map_err(Error::Reqwest) }
            },
            make_retry_config(strategy, 3),
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_then_success() {
        let server = MockServer::start().await;

        // First two return 500
        Mock::given(method("GET"))
            .and(path("/flaky"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(2)
            .mount(&server)
            .await;

        // Third returns 200
        Mock::given(method("GET"))
            .and(path("/flaky"))
            .respond_with(ResponseTemplate::new(200).set_body_string("Recovered"))
            .mount(&server)
            .await;

        let client = Client::new();
        let counter = Arc::new(AtomicU32::new(0));
        let strategy = RetryOn5xx {
            call_counter: counter.clone(),
        };

        let result = retry_wrapper(
            || {
                let url = format!("{}/flaky", server.uri());
                let client = client.clone();
                async move { client.get(&url).send().await.map_err(Error::Reqwest) }
            },
            make_retry_config(strategy, 5),
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(&result.unwrap().bytes().await.unwrap()[..], b"Recovered");
        assert_eq!(counter.load(Ordering::SeqCst), 3); // handle() only called on retry attempts
    }

    #[tokio::test]
    async fn test_retry_limit_exceeded() {
        let server = MockServer::start().await;

        // Always return 500
        Mock::given(method("GET"))
            .and(path("/fail"))
            .respond_with(ResponseTemplate::new(500))
            .expect(4) // 1 initial + 3 retries
            .mount(&server)
            .await;

        let client = Client::new();
        let counter = Arc::new(AtomicU32::new(0));
        let strategy = RetryOn5xx {
            call_counter: counter.clone(),
        };

        let result = retry_wrapper(
            || {
                let url = format!("{}/fail", server.uri());
                let client = client.clone();
                async move { client.get(&url).send().await.map_err(Error::Reqwest) }
            },
            make_retry_config(strategy, 3),
        )
        .await;

        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 4); // 3 retries attempted
    }

    #[tokio::test]
    async fn test_non_retryable_status() {
        let server = MockServer::start().await;

        // Respond with a 400 Bad Request
        Mock::given(method("GET"))
            .and(path("/bad"))
            .respond_with(ResponseTemplate::new(400))
            .expect(1)
            .mount(&server)
            .await;

        let client = Client::new();
        let counter = Arc::new(AtomicU32::new(0));
        let strategy = RetryOn5xx {
            call_counter: counter.clone(),
        };

        let result = retry_wrapper(
            || {
                let url = format!("{}/bad", server.uri());
                let client = client.clone();
                async move { client.get(&url).send().await.map_err(Error::Reqwest) }
            },
            make_retry_config(strategy, 5),
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 1); // strategy called once
    }
}
