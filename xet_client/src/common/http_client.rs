use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use http::{Extensions, HeaderMap, StatusCode};
use reqwest::header::{AUTHORIZATION, COOKIE, HeaderValue, SET_COOKIE};
use reqwest::{Request, Response};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, Middleware, Next};
use tokio::sync::Mutex;
use tracing::{Instrument, info, info_span, warn};
use xet_runtime::config::XetConfig;
use xet_runtime::core::XetContext;
use xet_runtime::error_printer::{ErrorPrinter, OptionPrinter};

use crate::cas_client::auth::{AuthConfig, TokenProvider};
use crate::cas_types::{REQUEST_ID_HEADER, SESSION_ID_HEADER};
use crate::error::{ClientError, Result};

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
    ) -> std::result::Result<Response, reqwest_middleware::Error> {
        let url = req.url_mut();
        if url.scheme() == "https" {
            let original_scheme = url.scheme().to_string();
            url.set_scheme("http").ok();
            info!(original_scheme, new_scheme = "http", url = %url, "Rewriting URL scheme for Unix socket proxy");
        }
        next.run(req, extensions).await
    }
}

/// Utility to redact sensitive headers from a HeaderMap before logging
fn redact_headers(headers: &HeaderMap) -> HeaderMap {
    let mut sanitized_headers = headers.clone();
    let sensitive_keys = vec![AUTHORIZATION, COOKIE, SET_COOKIE];

    for key in sensitive_keys {
        if sanitized_headers.contains_key(&key) {
            sanitized_headers.insert(key, "[REDACTED]".parse().unwrap());
        }
    }
    sanitized_headers
}

/// Produces a stable hash tag for `custom_headers` that can be included in the
/// client cache key.  Header names are sorted for order-independence.  Values are
/// hashed rather than included verbatim to avoid persisting auth tokens (e.g.
/// `Authorization: Bearer …`) as plaintext in a long-lived map key.
fn headers_tag(headers: Option<&HeaderMap>) -> String {
    let Some(headers) = headers else {
        return String::new();
    };
    let mut pairs: Vec<(&str, &[u8])> = headers.iter().map(|(k, v)| (k.as_str(), v.as_bytes())).collect();
    pairs.sort_by_key(|(k, _)| *k);
    let mut hasher = DefaultHasher::new();
    pairs.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[allow(unused_variables)]
#[cfg(not(target_family = "wasm"))]
fn reqwest_client_raw(
    config: &XetConfig,
    unix_socket_path: Option<&str>,
    custom_headers: Option<Arc<HeaderMap>>,
) -> std::result::Result<reqwest::Client, reqwest::Error> {
    let socket_path = unix_socket_path
        .map(|s| s.to_string())
        .or_else(|| config.client.unix_socket_path.clone());

    let socket_path_for_builder = socket_path.clone();
    let custom_headers_for_client = custom_headers.clone();
    let client_cfg = &config.client;
    let mut builder = reqwest::Client::builder()
        .pool_idle_timeout(client_cfg.idle_connection_timeout)
        .pool_max_idle_per_host(client_cfg.max_idle_connections)
        .connect_timeout(client_cfg.connect_timeout)
        .read_timeout(client_cfg.read_timeout)
        .http1_only();

    #[cfg(unix)]
    if let Some(ref path) = socket_path_for_builder {
        builder = builder.unix_socket(path.clone());
    }

    if let Some(headers) = custom_headers_for_client {
        builder = builder.default_headers((*headers).clone());
    }

    builder.build()
}

/// Creates a reqwest client with no read_timeout. Used for shard uploads where server-side
/// processing time is unbounded and would otherwise be killed by the global read_timeout.
/// Not cached — uses a separate connection pool from the main client.
#[cfg(not(target_family = "wasm"))]
#[allow(unused_variables)]
fn reqwest_client_no_read_timeout(
    config: &XetConfig,
    unix_socket_path: Option<&str>,
    custom_headers: Option<Arc<HeaderMap>>,
) -> Result<reqwest::Client> {
    let socket_path = unix_socket_path
        .map(|s| s.to_string())
        .or_else(|| config.client.unix_socket_path.clone());

    let client_cfg = &config.client;
    let mut builder = reqwest::Client::builder()
        .pool_idle_timeout(client_cfg.idle_connection_timeout)
        .pool_max_idle_per_host(client_cfg.max_idle_connections)
        .connect_timeout(client_cfg.connect_timeout)
        .http1_only();

    #[cfg(unix)]
    if let Some(ref path) = socket_path {
        builder = builder.unix_socket(path.clone());
    }

    if let Some(headers) = custom_headers {
        builder = builder.default_headers((*headers).clone());
    }

    let client = builder.build()?;

    info!(
        connect_timeout=?client_cfg.connect_timeout,
        "No-read-timeout HTTP client configured (for shard uploads)"
    );

    Ok(client)
}

#[cfg(target_family = "wasm")]
fn reqwest_client_raw(
    _config: &XetConfig,
    _unix_socket_path: Option<&str>,
    custom_headers: Option<Arc<HeaderMap>>,
) -> std::result::Result<reqwest::Client, reqwest::Error> {
    let mut builder = reqwest::Client::builder();
    if let Some(custom_headers) = custom_headers {
        builder = builder.default_headers((*custom_headers).clone());
    }
    builder.build()
}

/// Builds authenticated HTTP Client to talk to CAS.
#[allow(unused_mut)]
pub fn build_auth_http_client(
    ctx: &XetContext,
    auth_config: &Option<AuthConfig>,
    session_id: &str,
    unix_socket_path: Option<&str>,
    custom_headers: Option<Arc<HeaderMap>>,
) -> Result<ClientWithMiddleware> {
    let auth_middleware = auth_config.as_ref().map(AuthMiddleware::from).info_none("CAS auth disabled");
    let logging_middleware = Some(LoggingMiddleware);
    let session_middleware = (!session_id.is_empty()).then(|| SessionMiddleware(session_id.to_owned()));

    let config_arc = ctx.config.clone();
    let unix_owned = unix_socket_path.map(|s| s.to_string());
    let custom_for_client = custom_headers.clone();
    let socket_path = unix_socket_path
        .map(|s| s.to_string())
        .or_else(|| config_arc.client.unix_socket_path.clone());
    let tag = format!("{}|{}", socket_path.as_deref().unwrap_or("tcp"), headers_tag(custom_headers.as_deref()));

    let raw_client = ctx.get_or_create_reqwest_client(tag, move || {
        reqwest_client_raw(config_arc.as_ref(), unix_owned.as_deref(), custom_for_client)
    })?;

    if socket_path.is_some() {
        info!(socket_path=?socket_path, "HTTP client configured with Unix socket");
    } else {
        let client_cfg = &ctx.config.client;
        let custom_headers_log = custom_headers.as_deref().map(redact_headers);
        info!(
            idle_timeout=?client_cfg.idle_connection_timeout,
            max_idle_connections=client_cfg.max_idle_connections,
            custom_headers=?custom_headers_log,
            "HTTP client configured"
        );
    }

    let mut builder = ClientBuilder::new(raw_client);

    #[cfg(unix)]
    if unix_socket_path.is_some() {
        builder = builder.with(HttpsToHttpMiddleware);
    }

    Ok(builder
        .maybe_with(auth_middleware)
        .maybe_with(logging_middleware)
        .maybe_with(session_middleware)
        .build())
}

/// Builds an authenticated HTTP Client with no read_timeout.
///
/// All other settings (connect timeout, pool config, middleware) are identical
/// to the standard client. Used for shard uploads where server-side processing time
/// scales with file entry count and can exceed the global read_timeout.
#[cfg(not(target_family = "wasm"))]
#[allow(unused_mut)]
pub fn build_auth_http_client_no_read_timeout(
    ctx: &XetContext,
    auth_config: &Option<AuthConfig>,
    session_id: &str,
    unix_socket_path: Option<&str>,
    custom_headers: Option<Arc<HeaderMap>>,
) -> Result<ClientWithMiddleware> {
    let auth_middleware = auth_config.as_ref().map(AuthMiddleware::from).info_none("CAS auth disabled");
    let logging_middleware = Some(LoggingMiddleware);
    let session_middleware = (!session_id.is_empty()).then(|| SessionMiddleware(session_id.to_owned()));

    let raw_client = reqwest_client_no_read_timeout(ctx.config.as_ref(), unix_socket_path, custom_headers)?;
    let mut builder = ClientBuilder::new(raw_client);

    #[cfg(unix)]
    if unix_socket_path.is_some() {
        builder = builder.with(HttpsToHttpMiddleware);
    }

    Ok(builder
        .maybe_with(auth_middleware)
        .maybe_with(logging_middleware)
        .maybe_with(session_middleware)
        .build())
}

/// Builds HTTP Client to talk to CAS.
pub fn build_http_client(
    ctx: &XetContext,
    session_id: &str,
    unix_socket_path: Option<&str>,
    custom_headers: Option<Arc<HeaderMap>>,
) -> Result<ClientWithMiddleware> {
    build_auth_http_client(ctx, &None, session_id, unix_socket_path, custom_headers)
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
    async fn get_token(&self) -> Result<String> {
        let mut provider = self.token_provider.lock().await;
        provider
            .get_valid_token()
            .await
            .map_err(|err| {
                warn!(?err, "Token refresh failed");
                ClientError::AuthError(err)
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
        let token = self.get_token().await.map_err(reqwest_middleware::Error::middleware)?;

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
/// crate::error::Result instead of the raw reqwest_middleware::Result.
impl ResponseErrorLogger<Result<Response>> for reqwest_middleware::Result<Response> {
    fn process_error(self, api: &str) -> Result<Response> {
        let res = self.map_err(ClientError::from).log_error(format!("error invoking {api} api"))?;
        let request_id = request_id_from_response(&res);
        let error_message = format!("{api} api failed: request id: {request_id}");
        let status = res.status();
        let res = res.error_for_status().map_err(ClientError::from);
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
    fn test_build_http_client_without_uds() {
        let ctx = XetContext::default().expect("ctx");
        let result = build_http_client(&ctx, "test-session", None, None);
        assert!(result.is_ok());
    }

    mod shard_upload_client_tests {
        use super::*;

        #[test]
        fn test_build_no_read_timeout_succeeds() {
            let ctx = XetContext::default().expect("ctx");
            let result = build_auth_http_client_no_read_timeout(&ctx, &None, "test-session", None, None);
            assert!(result.is_ok());
        }

        #[test]
        fn test_build_no_read_timeout_with_empty_session_id() {
            let ctx = XetContext::default().expect("ctx");
            let result = build_auth_http_client_no_read_timeout(&ctx, &None, "", None, None);
            assert!(result.is_ok());
        }

        #[test]
        fn test_build_no_read_timeout_with_custom_headers() {
            let ctx = XetContext::default().expect("ctx");
            let mut headers = HeaderMap::new();
            headers.insert("X-Custom-Header", HeaderValue::from_static("test-value"));
            headers.insert(reqwest::header::USER_AGENT, HeaderValue::from_static("test-agent/1.0"));

            let result =
                build_auth_http_client_no_read_timeout(&ctx, &None, "test-session", None, Some(Arc::new(headers)));
            assert!(result.is_ok());
        }

        #[test]
        fn test_no_read_timeout_client_is_distinct_from_standard_client() {
            let ctx = XetContext::default().expect("ctx");
            let standard = build_auth_http_client(&ctx, &None, "test-session", None, None).unwrap();
            let no_timeout = build_auth_http_client_no_read_timeout(&ctx, &None, "test-session", None, None).unwrap();

            assert_ne!(
                format!("{:p}", &standard),
                format!("{:p}", &no_timeout),
                "Standard and no-timeout clients should be distinct instances"
            );
        }
    }
}
