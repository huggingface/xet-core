use cas::constants::*;
use std::str::FromStr;
use std::time::Duration;

use crate::cas_connection_pool::CasConnectionConfig;
use anyhow::{anyhow, Result};
use cas::common::CompressionScheme;
use cas::compression::{
    multiple_accepted_encoding_header_value, CAS_ACCEPT_ENCODING_HEADER,
    CAS_CONTENT_ENCODING_HEADER, CAS_INFLATED_SIZE_HEADER,
};
use error_printer::ErrorPrinter;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{
    header::RANGE,
    header::{HeaderMap, HeaderName, HeaderValue},
    Method, Request, Response, Version,
};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use lazy_static::lazy_static;
use lz4::block::CompressionMode;
use opentelemetry::propagation::Injector;
use retry_strategy::RetryStrategy;
use rustls_pemfile::Item;
use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::CertificateDer;
use tracing::{debug, error, info_span, warn, Instrument};
use xet_error::Error;

use merklehash::MerkleHash;

const HTTP2_POOL_IDLE_TIMEOUT_SECS: u64 = 30;
const HTTP2_KEEPALIVE_MILLIS: u64 = 500;
const HTTP2_WINDOW_SIZE: u32 = 2147418112;
const NUM_RETRIES: usize = 5;
const BASE_RETRY_DELAY_MS: u64 = 3000;

lazy_static! {
    static ref ACCEPTED_ENCODINGS_HEADER_VALUE: HeaderValue = HeaderValue::from_str(
        multiple_accepted_encoding_header_value(vec![
            CompressionScheme::Lz4,
            CompressionScheme::None
        ])
        .as_str()
    )
    .unwrap_or_else(|_| HeaderValue::from_static(""));
}

pub struct DataTransport {
    http2_client: Client<HttpsConnector<HttpConnector>, Full<Bytes>>,
    retry_strategy: RetryStrategy,
    cas_connection_config: CasConnectionConfig,
}

impl std::fmt::Debug for DataTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataTransport")
            .field("authority", &self.authority())
            .finish()
    }
}

/// This struct is used to wrap the error types which we may
/// retry on. Request Errors (which are triggered if there was a
/// header error building the request) are not retryable.
/// Right now this retries every h2 error. Reading these:
///  - https://docs.rs/h2/latest/h2/struct.Error.html,
///  - https://docs.rs/h2/latest/h2/struct.Reason.html
/// unclear if there is any reason not to retry.
#[derive(Error, Debug)]
enum RetryError {
    #[error("{0}")]
    Hyper(#[from] hyper::Error),

    #[error("{0}")]
    HyperLegacy(#[from] hyper_util::client::legacy::Error),

    #[error("Request Error: {0}")]
    Request(#[from] anyhow::Error),

    /// Should only be used for non-success errors
    #[error("Status Error: {0}")]
    Status(hyper::StatusCode),
}
fn is_status_retriable(err: &RetryError) -> bool {
    match err {
        RetryError::Hyper(_) => true,
        RetryError::HyperLegacy(_) => true,
        RetryError::Request(_) => false,
        RetryError::Status(n) => retry_http_status_code(n),
    }
}
fn retry_http_status_code(stat: &hyper::StatusCode) -> bool {
    stat.is_server_error() || *stat == hyper::StatusCode::TOO_MANY_REQUESTS
}

fn is_status_retriable_and_print(err: &RetryError) -> bool {
    let ret = is_status_retriable(err);
    if ret {
        debug!("{}. Retrying...", err);
    }
    ret
}
fn print_final_retry_error(err: RetryError) -> RetryError {
    if is_status_retriable(&err) {
        warn!("Many failures {}", err);
    }
    err
}

impl DataTransport {
    pub fn new(
        http2_client: Client<HttpsConnector<HttpConnector>, Full<Bytes>>,
        retry_strategy: RetryStrategy,
        cas_connection_config: CasConnectionConfig,
    ) -> Self {
        Self {
            http2_client,
            retry_strategy,
            cas_connection_config,
        }
    }

    /// creates the DataTransport instance for the H2 connection using
    /// CasConnectionConfig info, and additional port
    pub async fn from_config(cas_connection_config: CasConnectionConfig) -> Result<Self> {
        debug!(
            "Attempting to make HTTP connection with {}",
            cas_connection_config.endpoint
        );
        let mut builder = Client::builder(TokioExecutor::new());
        builder
            .timer(TokioTimer::new())
            .pool_idle_timeout(Duration::from_secs(HTTP2_POOL_IDLE_TIMEOUT_SECS))
            .http2_keep_alive_interval(Duration::from_millis(HTTP2_KEEPALIVE_MILLIS))
            .http2_initial_connection_window_size(HTTP2_WINDOW_SIZE)
            .http2_initial_stream_window_size(HTTP2_WINDOW_SIZE)
            .http2_only(true);
        let root_ca = cas_connection_config
            .root_ca
            .clone()
            .ok_or_else(|| anyhow!("missing server certificate"))?;
        let cert = try_from_pem(root_ca.as_bytes())?;
        let mut root_store = rustls::RootCertStore::empty();
        root_store.add(cert)?;
        let config = rustls::ClientConfig::builder()
            // add the CAS certificate to the client's root store
            // client does not need to assume identity for authentication
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let connector = HttpsConnectorBuilder::new()
            .with_tls_config(config)
            .https_only()
            .enable_http2()
            .build();
        let h2_client = builder.build(connector);
        let retry_strategy = RetryStrategy::new(NUM_RETRIES, BASE_RETRY_DELAY_MS);
        Ok(Self::new(h2_client, retry_strategy, cas_connection_config))
    }

    fn authority(&self) -> &str {
        self.cas_connection_config.endpoint.as_str()
    }

    fn get_uri(&self, prefix: &str, hash: &MerkleHash) -> String {
        let cas_key_string = cas::key::Key {
            prefix: prefix.to_string(),
            hash: *hash,
        }
        .to_string();
        if cas_key_string.starts_with('/') {
            format!("{}{}", self.authority(), cas_key_string)
        } else {
            format!("{}/{}", self.authority(), cas_key_string)
        }
    }

    fn setup_request(
        &self,
        method: Method,
        prefix: &str,
        hash: &MerkleHash,
        body: Option<Vec<u8>>,
    ) -> Result<Request<Full<Bytes>>> {
        let dest = self.get_uri(prefix, hash);
        debug!("Calling {} with address: {}", method, dest);
        let user_id = self.cas_connection_config.user_id.clone();
        let auth = self.cas_connection_config.auth.clone();
        // let request_id = get_request_id();
        let repo_paths = self.cas_connection_config.repo_paths.clone();
        let git_xet_version = self.cas_connection_config.git_xet_version.clone();
        // let cas_protocol_version = CAS_PROTOCOL_VERSION.clone();

        let mut req = Request::builder()
            .method(method.clone())
            .header(USER_ID_HEADER, user_id)
            .header(AUTH_HEADER, auth)
            //.header(REQUEST_ID_HEADER, request_id)
            .header(REPO_PATHS_HEADER, repo_paths)
            .header(GIT_XET_VERSION_HEADER, git_xet_version)
            //.header(CAS_PROTOCOL_VERSION_HEADER, cas_protocol_version)
            .uri(&dest)
            .version(Version::HTTP_2);

        if method == Method::GET {
            req = req.header(
                CAS_ACCEPT_ENCODING_HEADER,
                ACCEPTED_ENCODINGS_HEADER_VALUE.clone(),
            );
        }

        /*
        if trace_forwarding() {
            if let Some(headers) = req.headers_mut() {
                let mut injector = HeaderInjector(headers);
                let propagator = opentelemetry_jaeger::Propagator::new();
                let cur_span = Span::current();
                let ctx = cur_span.context();
                propagator.inject_context(&ctx, &mut injector);
            }
        }
        */

        let bytes = match body {
            None => Bytes::new(),
            Some(data) => Bytes::from(data),
        };
        req.body(Full::new(bytes)).map_err(|e| anyhow!(e))
    }

    // Single get to the H2 server
    pub async fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>> {
        let resp = self
            .retry_strategy
            .retry(
                || async {
                    let req = self
                        .setup_request(Method::GET, prefix, hash, None)
                        .map_err(RetryError::from)?;

                    let resp = self
                        .http2_client
                        .request(req)
                        .instrument(info_span!("transport.h2_get"))
                        .await
                        .map_err(|e| {
                            error!("{e}");
                            RetryError::from(e)
                        })?;

                    if retry_http_status_code(&resp.status()) {
                        return Err(RetryError::Status(resp.status()));
                    }
                    Ok(resp)
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(print_final_retry_error)?;
        let status = resp.status();
        if status != hyper::StatusCode::OK {
            return Err(anyhow!(
                "data get status {} received for URL {}",
                status,
                self.get_uri(prefix, hash)
            ));
        }
        debug!("Received Response from HTTP2 GET: {}", status);
        let (encoding, uncompressed_size) =
            get_encoding_info(&resp).unwrap_or((CompressionScheme::None, None));
        // Get the body
        let bytes = resp
            .collect()
            .instrument(info_span!("transport.read_body"))
            .await?
            .to_bytes()
            .to_vec();
        let payload_size = bytes.len();
        let bytes = maybe_decode(bytes.as_slice(), encoding, uncompressed_size)?;
        debug!(
            "GET; encoding: ({}), uncompressed size: ({}), payload ({})  prefix: ({}), hash: ({})",
            encoding.as_str_name(),
            uncompressed_size.unwrap_or_default(),
            payload_size,
            prefix,
            hash
        );
        Ok(bytes)
    }

    // Single get range to the H2 server
    pub async fn get_range(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        range: (u64, u64),
    ) -> Result<Vec<u8>> {
        let res = self
            .retry_strategy
            .retry(
                || async {
                    let mut req = self
                        .setup_request(Method::GET, prefix, hash, None)
                        .map_err(RetryError::from)?;
                    let header_value =
                        HeaderValue::from_str(&format!("bytes={}-{}", range.0, range.1 - 1))
                            .map_err(anyhow::Error::from)
                            .map_err(RetryError::from)?;
                    req.headers_mut().insert(RANGE, header_value);

                    let resp = self
                        .http2_client
                        .request(req)
                        .instrument(info_span!("transport.h2_get_range"))
                        .await
                        .map_err(RetryError::from)?;

                    if retry_http_status_code(&resp.status()) {
                        return Err(RetryError::Status(resp.status()));
                    }

                    let status = resp.status();
                    if status != hyper::StatusCode::OK {
                        return Err(RetryError::Request(anyhow!(
                            "data get_range status {} received for URL {} with range {:?}",
                            status,
                            self.get_uri(prefix, hash),
                            range
                        )));
                    }
                    debug!("Received Response from HTTP2 GET range: {}", status);
                    let (encoding, uncompressed_size) = get_encoding_info(&resp).unwrap_or((CompressionScheme::None, None));
                    // Get the body
                    let bytes: Vec<u8> = resp
                        .collect()
                        .instrument(info_span!("transport.read_body"))
                        .await?
                        .to_bytes()
                        .to_vec();
                    let payload_size = bytes.len();
                    let bytes = maybe_decode(bytes.as_slice(), encoding, uncompressed_size)?;
                    debug!("GET RANGE; encoding: ({}), uncompressed size: ({}), payload ({}) prefix: ({}), hash: ({})", encoding.as_str_name(), uncompressed_size.unwrap_or_default(), payload_size, prefix, hash);
                    Ok(bytes.to_vec())
                },
                is_status_retriable_and_print,
            )
            .await;

        res.map_err(print_final_retry_error)
            .map_err(anyhow::Error::from)
    }

    // Single put to the H2 server
    pub async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: &[u8],
        encoding: CompressionScheme,
    ) -> Result<()> {
        let full_size = data.len();
        let data = maybe_encode(data, encoding)?;
        debug!(
            "PUT; encoding: ({}), uncompressed size: ({}), payload: ({}), prefix: ({}), hash: ({})",
            encoding.as_str_name(),
            full_size,
            data.len(),
            prefix,
            hash
        );
        let resp = self
            .retry_strategy
            .retry(
                || async {
                    // compression of data to be done here, for now none.
                    let mut req = self
                        .setup_request(Method::POST, prefix, hash, Some(data.clone()))
                        .map_err(RetryError::from)?;
                    let headers = req.headers_mut();
                    headers.insert(CAS_INFLATED_SIZE_HEADER, HeaderValue::from(full_size));
                    headers.insert(
                        CAS_CONTENT_ENCODING_HEADER,
                        HeaderValue::from_static(encoding.into()),
                    );

                    let resp = self
                        .http2_client
                        .request(req)
                        .instrument(info_span!("transport.h2_put"))
                        .await
                        .map_err(RetryError::from)?;

                    if retry_http_status_code(&resp.status()) {
                        return Err(RetryError::Status(resp.status()));
                    }
                    Ok(resp)
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(print_final_retry_error)?;
        let status = resp.status();
        if status != hyper::StatusCode::OK {
            return Err(anyhow!(
                "data put status {} received for URL {}",
                status,
                self.get_uri(prefix, hash),
            ));
        }
        debug!("Received Response from HTTP2 POST: {}", status);

        Ok(())
    }
}

fn maybe_decode<'a, T: Into<&'a [u8]>>(
    bytes: T,
    encoding: CompressionScheme,
    uncompressed_size: Option<i32>,
) -> Result<Vec<u8>> {
    if let CompressionScheme::Lz4 = encoding {
        if uncompressed_size.is_none() {
            return Err(anyhow!(
                "Missing uncompressed size when attempting to decompress LZ4"
            ));
        }
        return lz4::block::decompress(bytes.into(), uncompressed_size).map_err(|e| anyhow!(e));
    }
    Ok(bytes.into().to_vec())
}

fn get_encoding_info<T>(response: &Response<T>) -> Option<(CompressionScheme, Option<i32>)> {
    let headers = response.headers();
    let value = headers.get(CAS_CONTENT_ENCODING_HEADER)?;
    let as_str = value.to_str().ok()?;
    let compression_scheme = CompressionScheme::from_str(as_str).ok()?;

    let value = headers.get(CAS_INFLATED_SIZE_HEADER)?;
    let as_str = value.to_str().ok()?;
    let uncompressed_size: Option<i32> = as_str.parse().ok();
    Some((compression_scheme, uncompressed_size))
}

fn maybe_encode<'a, T: Into<&'a [u8]>>(data: T, encoding: CompressionScheme) -> Result<Vec<u8>> {
    if let CompressionScheme::Lz4 = encoding {
        lz4::block::compress(data.into(), Some(CompressionMode::DEFAULT), false)
            .log_error("LZ4 compression error")
            .map_err(|e| anyhow!(e))
    } else {
        // None
        Ok(data.into().to_vec())
    }
}

fn try_from_pem(pem: &[u8]) -> Result<CertificateDer> {
    let (item, _) = rustls_pemfile::read_one_from_slice(pem)
        .map_err(|e| {
            error!("pem error: {e:?}");
            // rustls_pemfile::Error does not impl std::error::Error
            anyhow!("rustls_pemfile error {e:?}")
        })?
        .ok_or_else(|| anyhow!("failed to parse pem"))?;
    match item {
        Item::X509Certificate(cert) => Ok(cert),
        _ => Err(anyhow!("invalid cert format")),
    }
}

pub struct HeaderInjector<'a>(pub &'a mut HeaderMap<HeaderValue>);

impl<'a> Injector for HeaderInjector<'a> {
    /// Set a key and value in the HeaderMap.  Does nothing if the key or value are not valid inputs.
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key_header) = HeaderName::try_from(key) {
            if let Ok(header_value) = HeaderValue::from_str(&value) {
                self.0.insert(key_header, header_value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;
    use std::vec;

    use super::*;

    // cert to use for testing
    lazy_static! {
        static ref CERT: rcgen::Certificate = rcgen::generate_simple_self_signed(vec![]).unwrap();
    }

    #[tokio::test]
    async fn test_from_config() {
        let endpoint = "http://localhost:443";
        let config = CasConnectionConfig {
            endpoint: endpoint.to_string(),
            user_id: "user".to_string(),
            auth: "auth".to_string(),
            repo_paths: "repo".to_string(),
            git_xet_version: "0.1.0".to_string(),
            root_ca: None,
        }
        .with_root_ca(CERT.serialize_pem().unwrap());
        let dt = DataTransport::from_config(config).await.unwrap();
        assert_eq!(dt.authority(), endpoint);
    }

    #[tokio::test]
    async fn repo_path_header_test() {
        let data: Vec<Vec<String>> = vec![
            vec!["user1/repo-😀".to_string(), "user1/répô_123".to_string()],
            vec![
                "user2/👾_repo".to_string(),
                "user2/üникод".to_string(),
                "user2/foobar!@#".to_string(),
            ],
            vec!["user3/sømè_repo".to_string(), "user3/你好-世界".to_string()],
            vec!["user4/✨🌈repo".to_string()],
            vec!["user5/Ω≈ç√repo".to_string()],
            vec!["user6/42°_repo".to_string()],
            vec![
                "user7/äëïöü_repo".to_string(),
                "user7/ĀāĒēĪīŌōŪū".to_string(),
            ],
        ];
        for inner_vec in data {
            let config = CasConnectionConfig::new(
                "".to_string(),
                "".to_string(),
                "".to_string(),
                inner_vec.clone(),
                "".to_string(),
            )
            .with_root_ca(CERT.serialize_pem().unwrap());
            let client = DataTransport::from_config(config).await.unwrap();
            let hello = "hello".as_bytes().to_vec();
            let hash = merklehash::compute_data_hash(&hello[..]);
            let req = client.setup_request(Method::GET, "", &hash, None).unwrap();
            let repo_paths = req.headers().get(REPO_PATHS_HEADER).unwrap();
            let repo_path_str = String::from_utf8(repo_paths.as_bytes().to_vec()).unwrap();
            let vec_of_strings: Vec<String> =
                serde_json::from_str(repo_path_str.as_str()).expect("Failed to deserialize JSON");
            assert_eq!(vec_of_strings, inner_vec);
        }
    }

    #[tokio::test]
    async fn string_headers_test() {
        let user_id = "XET USER";
        let auth = "XET AUTH";
        let git_xet_version = "0.1.0";

        let cas_connection_config = CasConnectionConfig::new(
            "".to_string(),
            user_id.to_string(),
            auth.to_string(),
            vec![],
            git_xet_version.to_string(),
        )
        .with_root_ca(CERT.serialize_pem().unwrap());
        let client = DataTransport::from_config(cas_connection_config)
            .await
            .unwrap();
        let hash = merklehash::compute_data_hash("test".as_bytes());
        let req = client
            .setup_request(Method::POST, "default", &hash, None)
            .unwrap();
        let headers = req.headers();
        // gets header value assuming all well, panic if not
        let get_header_value = |header: &str| headers.get(header).unwrap().to_str().unwrap();

        // check against values in config
        assert_eq!(get_header_value(GIT_XET_VERSION_HEADER), git_xet_version);
        assert_eq!(get_header_value(USER_ID_HEADER), user_id);
    }
}
