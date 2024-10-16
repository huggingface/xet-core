use anyhow::anyhow;
use error_printer::OptionPrinter;
use reqwest::header::HeaderValue;
use reqwest::header::AUTHORIZATION;
use reqwest::{Request, Response};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, Middleware, Next};
use reqwest_retry::default_on_request_failure;
use reqwest_retry::default_on_request_success;
use reqwest_retry::Retryable;
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::warn;
use utils::auth::{AuthConfig, TokenProvider};

use crate::CasClientError;

/// Number of retries for transient errors.
const NUM_RETRIES: u32 = 5;
/// Base delay before retrying, default to 3s.
const BASE_RETRY_DELAY_MS: u64 = 3000; // 3s
/// Base max duration for retry attempts, default to 6m.
const BASE_RETRY_MAX_DURATION_MS: u64 = 6 * 60 * 1000; // 6m

/// builds the client to talk to CAS.
pub fn build_auth_http_client(
    auth_config: &Option<AuthConfig>,
) -> std::result::Result<ClientWithMiddleware, CasClientError> {
    let auth_middleware = auth_config
        .as_ref()
        .map(AuthMiddleware::from)
        .info_none("CAS auth disabled");
    let logging_middleware = Some(LoggingMiddleware);
    let retry_policy = get_retry_middleware(NUM_RETRIES);
    let reqwest_client = reqwest::Client::builder().build()?;
    Ok(ClientBuilder::new(reqwest_client)
        .maybe_with(auth_middleware)
        .maybe_with(Some(retry_policy))
        .maybe_with(logging_middleware)
        .build())
}

pub fn build_http_client() -> std::result::Result<ClientWithMiddleware, CasClientError> {
    let retry_middleware = get_retry_middleware(NUM_RETRIES);
    let logging_middleware = Some(LoggingMiddleware);
    let reqwest_client = reqwest::Client::builder().build()?;
    Ok(ClientBuilder::new(reqwest_client)
        .maybe_with(Some(retry_middleware))
        .maybe_with(logging_middleware)
        .build())
}

// retry policy with exponential backoff and configurable number of retries using reqwest-retry
fn get_retry_middleware(num_retries: u32) -> RetryTransientMiddleware<ExponentialBackoff> {
    let retry_policy = ExponentialBackoff::builder()
        .retry_bounds(
            Duration::from_millis(BASE_RETRY_DELAY_MS),
            Duration::from_millis(BASE_RETRY_MAX_DURATION_MS),
        )
        .build_with_max_retries(num_retries);

    // Uses DefaultRetryableStrategy which retries on 5xx/400/429 status codes, and retries on transient errors.
    // See https://github.com/TrueLayer/reqwest-middleware/blob/cf06f0962aae543526756ff7e1aa5e5cd0c42e42/reqwest-retry/src/retryable_strategy.rs#L97
    RetryTransientMiddleware::new_with_policy(retry_policy)
}

/// Helper trait to allow the reqwest_middleware client to optionally add a middleware.
trait OptionalMiddleware {
    fn maybe_with<M: Middleware>(self, middleware: Option<M>) -> Self;
}

impl OptionalMiddleware for ClientBuilder {
    fn maybe_with<M: Middleware>(self, middleware: Option<M>) -> Self {
        match middleware {
            Some(m) => self.with(m),
            None => self,
        }
    }
}

/// Adds logging middleware that will trace::warn! on retryable errors.
pub struct LoggingMiddleware;

#[async_trait::async_trait]
impl Middleware for LoggingMiddleware {
    async fn handle(
        &self,
        req: Request,
        extensions: &mut hyper::http::Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<Response> {
        let res = next.run(req, extensions).await;
        if res.is_ok() {
            let res = res.as_ref().unwrap();
            if Some(Retryable::Transient) == default_on_request_success(&res) {
                let status_code = res.status();
                warn!("Status Code: {status_code:?}. Retrying...");
            }
        } else {
            let err = res.as_ref().unwrap_err();
            if Some(Retryable::Transient) == default_on_request_failure(&err) {
                warn!("{err:?}. Retrying...");
            }
        }
        res
    }
}

/// AuthMiddleware is a thread-safe middleware that adds a CAS auth token to outbound requests.
/// If the token it holds is expired, it will automatically be refreshed.
pub struct AuthMiddleware {
    token_provider: Arc<Mutex<TokenProvider>>,
}

impl AuthMiddleware {
    /// Fetches a token from our TokenProvider. This locks the TokenProvider as we might need
    /// to refresh the token if it has expired.
    ///
    /// In the common case, this lock is held only to read the underlying token stored
    /// in memory. However, in the event of an expired token (e.g. once every 15 min),
    /// we will need to hold the lock while making a call to refresh the token
    /// (e.g. to a remote service). During this time, no other CAS requests can proceed
    /// from this client until the token has been fetched. This is expected/ok since we
    /// don't have a valid token and thus any calls would fail.
    fn get_token(&self) -> Result<String, anyhow::Error> {
        let mut provider = self
            .token_provider
            .lock()
            .map_err(|e| anyhow!("lock error: {e:?}"))?;
        provider
            .get_valid_token()
            .map_err(|e| anyhow!("couldn't get token: {e:?}"))
    }
}

impl From<&AuthConfig> for AuthMiddleware {
    fn from(cfg: &AuthConfig) -> Self {
        Self {
            token_provider: Arc::new(Mutex::new(TokenProvider::new(cfg))),
        }
    }
}

#[async_trait::async_trait]
impl Middleware for AuthMiddleware {
    async fn handle(
        &self,
        mut req: Request,
        extensions: &mut hyper::http::Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<Response> {
        let token = self
            .get_token()
            .map_err(reqwest_middleware::Error::Middleware)?;

        let headers = req.headers_mut();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
        );
        next.run(req, extensions).await
    }
}
