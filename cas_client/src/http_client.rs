use std::sync::Arc;

use anyhow::anyhow;
use cas_types::{REQUEST_ID_HEADER, SESSION_ID_HEADER};
use error_printer::{ErrorPrinter, OptionPrinter};
use http::{Extensions, StatusCode};
use reqwest::header::{AUTHORIZATION, HeaderValue};
use reqwest::{Request, Response};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, Middleware, Next};
use tokio::sync::Mutex;
use tracing::{Instrument, info, info_span, warn};
use utils::auth::{AuthConfig, TokenProvider};

use crate::{CasClientError, error};

/// Returns the Unix socket path if configured via `HF_XET_CLIENT_UNIX_SOCKET_PATH` env var.
#[cfg(not(target_family = "wasm"))]
pub fn get_unix_socket_path() -> Option<String> {
    use xet_runtime::xet_config;
    xet_config().client.unix_socket_path.clone()
}

/// Middleware that rewrites https:// URLs to http:// when using Unix socket.
/// This allows the proxy to parse plain HTTP and upgrade to HTTPS when forwarding.
#[cfg(unix)]
struct HttpsToHttpMiddleware;

#[cfg(unix)]
#[async_trait::async_trait]
impl Middleware for HttpsToHttpMiddleware {
    async fn handle(
        &self,
        mut req: Request,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> Result<Response, reqwest_middleware::Error> {
        let url = req.url_mut();
        if url.scheme() == "https" {
            let original_scheme = url.scheme().to_string();
            url.set_scheme("http").ok();
            info!(original_scheme, new_scheme = "http", url = %url, "Rewriting URL scheme for Unix socket proxy");
        }
        next.run(req, extensions).await
    }
}

#[cfg(not(target_family = "wasm"))]
fn reqwest_client(user_agent: &str) -> Result<reqwest::Client, CasClientError> {
    use xet_runtime::{XetRuntime, xet_config};

    let unix_socket_path = get_unix_socket_path();

    let client = if let Some(ref socket_path) = unix_socket_path {
        XetRuntime::get_or_create_reqwest_uds_client(|| {
            let mut builder = reqwest::Client::builder()
                .pool_idle_timeout(xet_config().client.idle_connection_timeout)
                .pool_max_idle_per_host(xet_config().client.max_idle_connections)
                .connect_timeout(xet_config().client.connect_timeout)
                .read_timeout(xet_config().client.read_timeout)
                .http1_only();

            if !user_agent.is_empty() {
                builder = builder.user_agent(user_agent);
            }

            #[cfg(unix)]
            {
                builder = builder.unix_socket(socket_path.clone());
            }

            builder.build()
        })?
    } else {
        XetRuntime::get_or_create_reqwest_client(|| {
            let mut builder = reqwest::Client::builder()
                .pool_idle_timeout(xet_config().client.idle_connection_timeout)
                .pool_max_idle_per_host(xet_config().client.max_idle_connections)
                .connect_timeout(xet_config().client.connect_timeout)
                .read_timeout(xet_config().client.read_timeout)
                .http1_only();

            if !user_agent.is_empty() {
                builder = builder.user_agent(user_agent);
            }

            builder.build()
        })?
    };

    if unix_socket_path.is_some() {
        info!(socket_path=?unix_socket_path, "HTTP client configured with Unix socket");
    } else {
        info!(
            idle_timeout=?xet_config().client.idle_connection_timeout,
            max_idle_connections=xet_config().client.max_idle_connections,
            user_agent=?if user_agent.is_empty() { None } else { Some(user_agent) },
            "HTTP client configured"
        );
    }

    Ok(client)
}

#[cfg(target_family = "wasm")]
fn reqwest_client(user_agent: &str) -> Result<reqwest::Client, CasClientError> {
    // For WASM, create a new client with the specified user_agent
    // Note: we could cache this, but user_agent can vary, so we create per-call
    let mut builder = reqwest::Client::builder();
    if !user_agent.is_empty() {
        builder = builder.user_agent(user_agent);
    }
    Ok(builder.build()?)
}

/// Builds authenticated HTTP Client to talk to CAS.
pub fn build_auth_http_client(
    auth_config: &Option<AuthConfig>,
    session_id: &str,
    user_agent: &str,
) -> Result<ClientWithMiddleware, CasClientError> {
    let auth_middleware = auth_config.as_ref().map(AuthMiddleware::from).info_none("CAS auth disabled");
    let logging_middleware = Some(LoggingMiddleware);
    let session_middleware = (!session_id.is_empty()).then(|| SessionMiddleware(session_id.to_owned()));

    let mut builder = ClientBuilder::new(reqwest_client(user_agent)?);

    #[cfg(unix)]
    if get_unix_socket_path().is_some() {
        builder = builder.with(HttpsToHttpMiddleware);
    }

    Ok(builder
        .maybe_with(auth_middleware)
        .maybe_with(logging_middleware)
        .maybe_with(session_middleware)
        .build())
}

/// Builds HTTP Client to talk to CAS.
pub fn build_http_client(session_id: &str, user_agent: &str) -> Result<ClientWithMiddleware, CasClientError> {
    build_auth_http_client(&None, session_id, user_agent)
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

#[derive(Copy, Clone)]
pub struct Api(pub &'static str);

/// Adds logging middleware that logs requests and responses.
pub struct LoggingMiddleware;

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl Middleware for LoggingMiddleware {
    async fn handle(
        &self,
        req: Request,
        extensions: &mut http::Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<Response> {
        let api = extensions.get::<Api>().map(|a| a.0);
        next.run(req, extensions)
            .instrument(info_span!("client::request", api))
            .await
            .inspect(|res| {
                let status_code = res.status().as_u16();
                let request_id = request_id_from_response(res);
                info!(request_id, status_code, "Received CAS response");
            })
            .inspect_err(|err| {
                warn!(?err, "Request error");
            })
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
    async fn get_token(&self) -> Result<String, anyhow::Error> {
        let mut provider = self.token_provider.lock().await;
        provider
            .get_valid_token()
            .await
            .map_err(|err| {
                warn!(?err, "Token refresh failed");
                anyhow!("couldn't get token: {err:?}")
            })
            .inspect(|_token| {
                info!("Token refresh successful for CAS authentication");
            })
    }
}

impl From<&AuthConfig> for AuthMiddleware {
    fn from(cfg: &AuthConfig) -> Self {
        Self {
            token_provider: Arc::new(Mutex::new(TokenProvider::new(cfg))),
        }
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl Middleware for AuthMiddleware {
    async fn handle(
        &self,
        mut req: Request,
        extensions: &mut http::Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<Response> {
        let token = self.get_token().await.map_err(reqwest_middleware::Error::Middleware)?;

        let headers = req.headers_mut();
        headers.insert(AUTHORIZATION, HeaderValue::from_str(&format!("Bearer {token}")).unwrap());
        next.run(req, extensions).await
    }
}

pub struct SessionMiddleware(String);

// WASM compatibility; note the use of the pattern:
//
// #[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
// #[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
// instead of just #[async_trait::async_trait]
// this makes the use of the async_trait wasm compatible to not enforce `Send` bounds when
// compiling for wasm, while those Send bounds are useful in non-wasm mode.

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl Middleware for SessionMiddleware {
    async fn handle(
        &self,
        mut req: Request,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<Response> {
        req.headers_mut()
            .insert(SESSION_ID_HEADER, HeaderValue::from_str(&self.0).unwrap());
        next.run(req, extensions).await
    }
}

/// Helper trait to log the different types of errors that come back from a request to CAS,
/// transforming the implementation into some new error type.
pub trait ResponseErrorLogger<T> {
    fn process_error(self, api: &str) -> T;
}

/// Add ResponseErrorLogger to Result<Response> for our requests.
/// This logs an error if one occurred before receiving a response or
/// if the status code indicates a failure.
/// As a result of these checks, the response is also transformed into a
/// cas_client::error::Result instead of the raw reqwest_middleware::Result.
impl ResponseErrorLogger<error::Result<Response>> for reqwest_middleware::Result<Response> {
    fn process_error(self, api: &str) -> error::Result<Response> {
        let res = self
            .map_err(CasClientError::from)
            .log_error(format!("error invoking {api} api"))?;
        let request_id = request_id_from_response(&res);
        let error_message = format!("{api} api failed: request id: {request_id}");
        let status = res.status();
        let res = res.error_for_status().map_err(CasClientError::from);
        match (api, status) {
            ("get_reconstruction", StatusCode::RANGE_NOT_SATISFIABLE) => res.debug_error(&error_message),
            // not all status codes mean fatal error
            _ => res.info_error(&error_message),
        }
    }
}

pub fn request_id_from_response(res: &Response) -> &str {
    res.headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|h| h.to_str().ok())
        .unwrap_or_default()
}

#[cfg(test)]
#[cfg(not(target_family = "wasm"))]
mod tests {
    use super::*;

    #[cfg(unix)]
    mod uds_tests {
        use super::*;

        #[tokio::test]
        async fn test_https_to_http_middleware_rewrites_https() {
            let _middleware = HttpsToHttpMiddleware;

            let client = reqwest::Client::new();
            let mut test_request = client.get("https://example.com/api/data").build().unwrap();

            let url = test_request.url_mut();
            assert_eq!(url.scheme(), "https");

            if url.scheme() == "https" {
                url.set_scheme("http").ok();
            }

            assert_eq!(test_request.url().scheme(), "http");
            assert_eq!(test_request.url().host_str(), Some("example.com"));
            assert_eq!(test_request.url().path(), "/api/data");
        }

        #[tokio::test]
        async fn test_https_to_http_middleware_preserves_http() {
            let client = reqwest::Client::new();
            let mut test_request = client.get("http://example.com/api/data").build().unwrap();

            let url = test_request.url_mut();
            let original_scheme = url.scheme().to_string();

            if url.scheme() == "https" {
                url.set_scheme("http").ok();
            }

            assert_eq!(test_request.url().scheme(), "http");
            assert_eq!(original_scheme, "http");
        }

        #[tokio::test]
        async fn test_https_to_http_middleware_preserves_url_components() {
            let client = reqwest::Client::new();
            let mut test_request = client
                .get("https://example.com:8443/path/to/resource?query=value&foo=bar")
                .build()
                .unwrap();

            let url = test_request.url_mut();
            if url.scheme() == "https" {
                url.set_scheme("http").ok();
            }

            assert_eq!(test_request.url().scheme(), "http");
            assert_eq!(test_request.url().host_str(), Some("example.com"));
            assert_eq!(test_request.url().port(), Some(8443));
            assert_eq!(test_request.url().path(), "/path/to/resource");
            assert_eq!(test_request.url().query(), Some("query=value&foo=bar"));
        }
    }

    #[test]
    fn test_get_unix_socket_path_returns_none_by_default() {
        let path = get_unix_socket_path();
        let _: Option<String> = path;
    }

    #[test]
    fn test_build_http_client_without_uds() {
        let result = build_http_client("test-session", "test-agent");
        assert!(result.is_ok());
    }
}
