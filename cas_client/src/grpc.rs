use crate::error::Result;
use std::env::VarError;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::cas_connection_pool::CasConnectionConfig;
use crate::remote_client::CAS_PROTOCOL_VERSION;
use http::Uri;
use opentelemetry::propagation::{Injector, TextMapPropagator};
use retry_strategy::RetryStrategy;
use tonic::codegen::InterceptedService;
use tonic::metadata::{Ascii, Binary, MetadataKey, MetadataMap, MetadataValue};
use tonic::service::Interceptor;
use tonic::transport::{Certificate, ClientTlsConfig};
use tonic::{transport::Channel, Code, Request, Status};
use tracing::{debug, info, warn, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use cas::common::CompressionScheme;
use cas::{
    cas::{
        cas_client::CasClient, GetRangeRequest, GetRequest, HeadRequest, PutCompleteRequest,
        PutRequest, Range,
    },
    common::{EndpointConfig, InitiateRequest, InitiateResponse, Key, Scheme},
    constants::*,
};
use merklehash::MerkleHash;

use crate::CasClientError;
pub type CasClientType = CasClient<InterceptedService<Channel, MetadataHeaderInterceptor>>;

const DEFAULT_H2_PORT: u16 = 443;
const DEFAULT_PUT_COMPLETE_PORT: u16 = 5000;

const HTTP2_KEEPALIVE_TIMEOUT_SEC: u64 = 20;
const HTTP2_KEEPALIVE_INTERVAL_SEC: u64 = 1;
const NUM_RETRIES: usize = 5;
const BASE_RETRY_DELAY_MS: u64 = 3000;

// production ready settings
const INITIATE_CAS_SCHEME: &str = "https";
const HTTP_CAS_SCHEME: &str = "http";

lazy_static::lazy_static! {
    static ref DEFAULT_UUID: Uuid = Uuid::new_v4();
    static ref REQUEST_COUNTER: AtomicUsize = AtomicUsize::new(0);
    static ref TRACE_FORWARDING: AtomicBool = AtomicBool::new(false);
}

async fn get_channel(endpoint: &str, root_ca: &Option<Arc<String>>) -> Result<Channel> {
    debug!("server name: {}", endpoint);
    let mut server_uri: Uri = endpoint
        .parse()
        .map_err(|e| CasClientError::ConfigurationError(format!("Error parsing endpoint: {e}.")))?;

    // supports an absolute URI (above) or just the host:port (below)
    // only used on first endpoint, all other endpoints should come from CAS
    // with scheme info already included
    // in local/witt modes overridden CAS initial URI should include scheme e.g.
    //  http://localhost:40000
    if server_uri.scheme().is_none() {
        let scheme = if cfg!(test) {
            HTTP_CAS_SCHEME
        } else {
            INITIATE_CAS_SCHEME
        };
        server_uri = format!("{scheme}://{endpoint}").parse().unwrap();
    }

    debug!("Connecting to URI: {}", server_uri);

    let mut builder = Channel::builder(server_uri);
    if let Some(root_ca) = root_ca {
        let tls_config =
            ClientTlsConfig::new().ca_certificate(Certificate::from_pem(root_ca.as_str()));
        builder = builder.tls_config(tls_config)?;
    }
    let channel = builder
        .keep_alive_timeout(Duration::new(HTTP2_KEEPALIVE_TIMEOUT_SEC, 0))
        .http2_keep_alive_interval(Duration::new(HTTP2_KEEPALIVE_INTERVAL_SEC, 0))
        .timeout(Duration::new(GRPC_TIMEOUT_SEC, 0))
        .connect_timeout(Duration::new(GRPC_TIMEOUT_SEC, 0))
        .connect()
        .await?;
    Ok(channel)
}

pub async fn get_client(cas_connection_config: CasConnectionConfig) -> Result<CasClientType> {
    let timeout_channel = get_channel(
        cas_connection_config.endpoint.as_str(),
        &cas_connection_config.root_ca,
    )
    .await?;

    let client: CasClientType = CasClient::with_interceptor(
        timeout_channel,
        MetadataHeaderInterceptor::new(cas_connection_config),
    );
    Ok(client)
}

/// Adds common metadata headers to all requests. Currently, this includes
/// authorization and xet-user-id.
/// TODO: at some point, we should re-evaluate how we authenticate/authorize requests to CAS.
#[derive(Debug, Clone)]
pub struct MetadataHeaderInterceptor {
    config: CasConnectionConfig,
}

impl MetadataHeaderInterceptor {
    fn new(config: CasConnectionConfig) -> MetadataHeaderInterceptor {
        MetadataHeaderInterceptor { config }
    }
}

impl Interceptor for MetadataHeaderInterceptor {
    // note original Interceptor trait accepts non-mut request
    // but may accept mut request like in this case
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        request.set_timeout(Duration::new(GRPC_TIMEOUT_SEC, 0));
        let metadata = request.metadata_mut();

        // pass user_id and repo_paths received from xetconfig
        let user_id = get_metadata_ascii_from_str_with_default(&self.config.user_id, DEFAULT_USER);
        metadata.insert(USER_ID_HEADER, user_id);
        let auth = get_metadata_ascii_from_str_with_default(&self.config.auth, DEFAULT_AUTH);
        metadata.insert(AUTH_HEADER, auth);

        let repo_paths = get_repo_paths_metadata_value(&self.config.repo_paths);
        metadata.insert_bin(REPO_PATHS_HEADER, repo_paths);

        let git_xet_version =
            get_metadata_ascii_from_str_with_default(&self.config.git_xet_version, DEFAULT_VERSION);
        metadata.insert(GIT_XET_VERSION_HEADER, git_xet_version);

        let cas_protocol_version: MetadataValue<Ascii> =
            MetadataValue::from_static(CAS_PROTOCOL_VERSION.as_str());
        metadata.insert(CAS_PROTOCOL_VERSION_HEADER, cas_protocol_version);

        // propagate tracing context (e.g. trace_id, span_id) to service
        if trace_forwarding() {
            let mut injector = HeaderInjector(metadata);
            let propagator = opentelemetry_jaeger::Propagator::new();
            let cur_span = Span::current();
            let ctx = cur_span.context();
            propagator.inject_context(&ctx, &mut injector);
        }

        let request_id = get_request_id();
        metadata.insert(
            REQUEST_ID_HEADER,
            MetadataValue::from_str(&request_id)
                .map_err(|e| Status::internal(format!("Metadata error: {e:?}")))?,
        );

        Ok(request)
    }
}

pub fn set_trace_forwarding(should_enable: bool) {
    TRACE_FORWARDING.store(should_enable, Ordering::Relaxed);
}

pub fn trace_forwarding() -> bool {
    TRACE_FORWARDING.load(Ordering::Relaxed)
}

pub struct HeaderInjector<'a>(pub &'a mut MetadataMap);

impl<'a> Injector for HeaderInjector<'a> {
    /// Set a key and value in the HeaderMap.  Does nothing if the key or value are not valid inputs.
    fn set(&mut self, key: &str, value: String) {
        if let Ok(name) = MetadataKey::from_str(key) {
            if let Ok(val) = MetadataValue::from_str(&value) {
                self.0.insert(name, val);
            }
        }
    }
}

fn get_metadata_ascii_from_str_with_default(
    value: &str,
    default: &'static str,
) -> MetadataValue<Ascii> {
    MetadataValue::from_str(value)
        .map_err(|_| VarError::NotPresent)
        .unwrap_or_else(|_| MetadataValue::from_static(default))
}

fn get_repo_paths_metadata_value(repo_paths: &str) -> MetadataValue<Binary> {
    MetadataValue::from_bytes(repo_paths.as_bytes())
}

pub fn get_request_id() -> String {
    format!(
        "{}.{}",
        *DEFAULT_UUID,
        REQUEST_COUNTER.load(Ordering::Relaxed)
    )
}

fn inc_request_id() {
    REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
}

/// CAS Client that uses GRPC for communication.
///
/// ## Implementation note
/// The GrpcClient is thread-safe and allows multiplexing requests on the
/// underlying gRPC connection. This is done by cheaply cloning the client:
/// https://docs.rs/tonic/0.1.0/tonic/transport/struct.Channel.html#multiplexing-requests
#[derive(Debug)]
pub struct GrpcClient {
    pub endpoint: String,
    client: CasClientType,
    retry_strategy: RetryStrategy,
}

impl Clone for GrpcClient {
    fn clone(&self) -> Self {
        GrpcClient {
            endpoint: self.endpoint.clone(),
            client: self.client.clone(),
            retry_strategy: self.retry_strategy,
        }
    }
}

impl GrpcClient {
    pub fn new(endpoint: String, client: CasClientType, retry_strategy: RetryStrategy) -> Self {
        Self {
            endpoint,
            client,
            retry_strategy,
        }
    }

    pub async fn from_config(cas_connection_config: CasConnectionConfig) -> Result<GrpcClient> {
        let endpoint = cas_connection_config.endpoint.clone();
        let client: CasClientType = get_client(cas_connection_config).await?;
        // Retry policy: Exponential backoff starting at BASE_RETRY_DELAY_MS and retrying NUM_RETRIES times
        let retry_strategy = RetryStrategy::new(NUM_RETRIES, BASE_RETRY_DELAY_MS);
        Ok(GrpcClient::new(endpoint, client, retry_strategy))
    }
}

pub fn is_status_retriable(err: &Status) -> bool {
    match err.code() {
        Code::Ok
        | Code::Cancelled
        | Code::InvalidArgument
        | Code::NotFound
        | Code::AlreadyExists
        | Code::PermissionDenied
        | Code::FailedPrecondition
        | Code::OutOfRange
        | Code::Unimplemented
        | Code::Unauthenticated => false,
        Code::Unknown
        | Code::DeadlineExceeded
        | Code::ResourceExhausted
        | Code::Aborted
        | Code::Internal
        | Code::Unavailable
        | Code::DataLoss => true,
    }
}

pub fn is_status_retriable_and_print(err: &Status) -> bool {
    let ret = is_status_retriable(err);
    if ret {
        info!("GRPC Error {}. Retrying...", err);
    }
    ret
}

pub fn print_final_retry_error(err: Status) -> Status {
    if is_status_retriable(&err) {
        warn!("Many failures {}", err);
    }
    err
}

impl Drop for GrpcClient {
    fn drop(&mut self) {
        debug!("GrpcClient: Dropping GRPC Client.");
    }
}

// DTO for initiate rpc response info
pub struct EndpointsInfo {
    pub data_plane_endpoint: EndpointConfig,
    pub put_complete_endpoint: EndpointConfig,
    pub accepted_encodings: Vec<CompressionScheme>,
}

impl GrpcClient {
    #[tracing::instrument(skip_all, name = "cas.client", err, fields(prefix = prefix, hash = hash.hex().as_str(), api = "put", request_id = tracing::field::Empty))]
    pub async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<()> {
        inc_request_id();
        Span::current().record("request_id", &get_request_id());
        debug!(
            "GrpcClient Req {}: put to {}/{} of length {} bytes",
            get_request_id(),
            prefix,
            hash,
            data.len(),
        );
        let request = PutRequest {
            key: Some(get_key_for_request(prefix, hash)),
            data,
            chunk_boundaries,
        };

        let response = self
            .retry_strategy
            .retry(
                || async {
                    let req = Request::new(request.clone());
                    self.client.clone().put(req).await
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(print_final_retry_error)
            .map_err(|e| {
                info!(
                    "GrpcClient Req {}: Error on Put {}/{} : {:?}",
                    get_request_id(),
                    prefix,
                    hash,
                    e
                );
                CasClientError::Grpc(anyhow::Error::from(e))
            })?;

        debug!(
            "GrpcClient Req {}: put to {}/{} complete.",
            get_request_id(),
            prefix,
            hash,
        );

        if !response.into_inner().was_inserted {
            debug!(
                "GrpcClient Req {}: XORB {}/{} not inserted; already present.",
                get_request_id(),
                prefix,
                hash
            );
        }

        Ok(())
    }

    /// on success returns a type of 2 EndpointConfig's
    /// first the EndpointConfig for the h2 dataplane endpoint
    /// second the EndpointConfig for the grpc endpoint used for put complete rpc
    #[tracing::instrument(skip_all, name = "cas.client", err, fields(prefix = prefix, hash = hash.hex().as_str(), api = "initiate", request_id = tracing::field::Empty))]
    pub async fn initiate(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        payload_size: usize,
    ) -> Result<EndpointsInfo> {
        debug!(
            "GrpcClient Req {}. initiate {}/{}, size={payload_size}",
            get_request_id(),
            prefix,
            hash
        );
        inc_request_id();
        Span::current().record("request_id", &get_request_id());
        let request = InitiateRequest {
            key: Some(get_key_for_request(prefix, hash)),
            payload_size: payload_size as u64,
        };

        let response = self
            .retry_strategy
            .retry(
                || async {
                    let req = Request::new(request.clone());
                    self.client.clone().initiate(req).await
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(print_final_retry_error)
            .map_err(|e| CasClientError::Grpc(anyhow::Error::from(e)))?;

        let InitiateResponse {
            data_plane_endpoint,
            put_complete_endpoint,
            cas_hostname,
            accepted_encodings,
        } = response.into_inner();

        let accepted_encodings = accepted_encodings
            .into_iter()
            .filter_map(|i| CompressionScheme::try_from(i).ok())
            .collect();

        if data_plane_endpoint.is_none() || put_complete_endpoint.is_none() {
            info!("CAS initiate response indicates cas protocol version < v0.2.0, defaulting to v0.1.0 config");
            // default case, relevant for using CAS until prod is synced with v0.2.0
            return Ok(EndpointsInfo {
                data_plane_endpoint: EndpointConfig {
                    host: cas_hostname.clone(),
                    port: DEFAULT_H2_PORT.into(),
                    scheme: Scheme::Http.into(),
                    ..Default::default()
                },
                put_complete_endpoint: EndpointConfig {
                    host: cas_hostname,
                    port: DEFAULT_PUT_COMPLETE_PORT.into(),
                    scheme: Scheme::Http.into(),
                    ..Default::default()
                },
                accepted_encodings,
            });
        }
        debug!(
            "GrpcClient Req {}. initiate {}/{}, size={payload_size} complete",
            get_request_id(),
            prefix,
            hash
        );

        Ok(EndpointsInfo {
            data_plane_endpoint: data_plane_endpoint.unwrap(),
            put_complete_endpoint: put_complete_endpoint.unwrap(),
            accepted_encodings,
        })
    }

    #[tracing::instrument(skip_all, name = "cas.client", err, fields(prefix = prefix, hash = hash.hex().as_str(), api = "put_complete", request_id = tracing::field::Empty))]
    pub async fn put_complete(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        chunk_boundaries: &[u64],
    ) -> Result<()> {
        debug!(
            "GrpcClient Req {}. put_complete of {}/{}",
            get_request_id(),
            prefix,
            hash
        );
        Span::current().record("request_id", &get_request_id());
        let request = PutCompleteRequest {
            key: Some(get_key_for_request(prefix, hash)),
            chunk_boundaries: chunk_boundaries.to_owned(),
        };

        let _ = self
            .retry_strategy
            .retry(
                || async {
                    let req = Request::new(request.clone());
                    self.client.clone().put_complete(req).await
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(print_final_retry_error)
            .map_err(|e| CasClientError::Grpc(anyhow::Error::from(e)))?;

        debug!(
            "GrpcClient Req {}. put_complete of {}/{} complete.",
            get_request_id(),
            prefix,
            hash
        );
        Ok(())
    }

    #[tracing::instrument(skip_all, name = "cas.client", err, fields(prefix = prefix, hash = hash.hex().as_str(), api = "get", request_id = tracing::field::Empty))]
    pub async fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>> {
        inc_request_id();
        Span::current().record("request_id", &get_request_id());
        debug!(
            "GrpcClient Req {}. Get of {}/{}",
            get_request_id(),
            prefix,
            hash
        );
        let request = GetRequest {
            key: Some(get_key_for_request(prefix, hash)),
        };
        let response = self
            .retry_strategy
            .retry(
                || async {
                    let req = Request::new(request.clone());
                    self.client.clone().get(req).await
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(print_final_retry_error)
            .map_err(|e| {
                info!(
                    "GrpcClient Req {}. Error on Get {}/{} : {:?}",
                    get_request_id(),
                    prefix,
                    hash,
                    e
                );
                CasClientError::Grpc(anyhow::Error::from(e))
            })?;

        debug!(
            "GrpcClient Req {}. Get of {}/{} complete.",
            get_request_id(),
            prefix,
            hash
        );

        Ok(response.into_inner().data)
    }

    #[tracing::instrument(skip_all, name = "cas.client", err, fields(prefix = prefix, hash = hash.hex().as_str(), api = "get_range", request_id = tracing::field::Empty))]
    pub async fn get_object_range(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>> {
        inc_request_id();
        Span::current().record("request_id", &get_request_id());
        debug!(
            "GrpcClient Req {}. GetObjectRange of {}/{}",
            get_request_id(),
            prefix,
            hash
        );
        // Handle the case where we aren't asked for any real data.
        if ranges.len() == 1 && ranges[0].0 == ranges[0].1 {
            return Ok(vec![Vec::<u8>::new()]);
        }

        let range_vec: Vec<Range> = ranges
            .into_iter()
            .map(|(start, end)| Range { start, end })
            .collect();
        let request = GetRangeRequest {
            key: Some(get_key_for_request(prefix, hash)),
            ranges: range_vec,
        };
        let response = self
            .retry_strategy
            .retry(
                || async {
                    let req = Request::new(request.clone());
                    self.client.clone().get_range(req).await
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(print_final_retry_error)
            .map_err(|e| {
                info!(
                    "GrpcClient Req {}. Error on GetObjectRange of {}/{} : {:?}",
                    get_request_id(),
                    prefix,
                    hash,
                    e
                );
                CasClientError::Grpc(anyhow::Error::from(e))
            })?;

        debug!(
            "GrpcClient Req {}. GetObjectRange of {}/{} complete.",
            get_request_id(),
            prefix,
            hash
        );

        Ok(response.into_inner().data)
    }

    #[tracing::instrument(skip_all, name = "cas.client", fields(prefix = prefix, hash = hash.hex().as_str(), api = "get_length", request_id = tracing::field::Empty))]
    pub async fn get_length(&self, prefix: &str, hash: &MerkleHash) -> Result<u64> {
        inc_request_id();
        Span::current().record("request_id", &get_request_id());
        debug!(
            "GrpcClient Req {}. GetLength of {}/{}",
            get_request_id(),
            prefix,
            hash
        );
        let request = HeadRequest {
            key: Some(get_key_for_request(prefix, hash)),
        };
        let response = self
            .retry_strategy
            .retry(
                || async {
                    let req = Request::new(request.clone());

                    self.client.clone().head(req).await
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(print_final_retry_error)
            .map_err(|e| {
                debug!(
                    "GrpcClient Req {}. Error on GetLength of {}/{} : {:?}",
                    get_request_id(),
                    prefix,
                    hash,
                    e
                );
                CasClientError::Grpc(anyhow::Error::from(e))
            })?;
        debug!(
            "GrpcClient Req {}. GetLength of {}/{} complete.",
            get_request_id(),
            prefix,
            hash
        );
        Ok(response.into_inner().size)
    }
}

pub fn get_key_for_request(prefix: &str, hash: &MerkleHash) -> Key {
    Key {
        prefix: prefix.to_string(),
        hash: hash.as_bytes().to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    use tonic::Response;

    use cas::cas::PutResponse;

    use crate::util::grpc_mock::{MockService, ShutdownHook};

    use super::*;

    #[tokio::test]
    async fn test_put_with_retry() {
        let count = Arc::new(AtomicU32::new(0));
        let put_count = count.clone();
        let put_api = move |req: Request<PutRequest>| {
            assert_eq!(req.into_inner().chunk_boundaries, vec![32, 54, 63]);
            if 0 == put_count.fetch_add(1, Ordering::SeqCst) {
                return Err(Status::internal("Failed"));
            }
            Ok(Response::new(PutResponse { was_inserted: true }))
        };

        let (mut hook, client): (ShutdownHook, GrpcClient) =
            MockService::default().with_put(put_api).start().await;

        let resp = client
            .put("pre1", &MerkleHash::default(), vec![0], vec![32, 54, 63])
            .await;
        assert_eq!(2, count.load(Ordering::SeqCst));
        assert!(resp.is_ok());
        hook.async_drop().await;
    }

    #[tokio::test]
    async fn test_put_exhausted_retries() {
        let count = Arc::new(AtomicU32::new(0));
        let put_count = count.clone();
        let put_api = move |req: Request<PutRequest>| {
            assert_eq!(req.into_inner().chunk_boundaries, vec![31, 54, 63]);
            put_count.fetch_add(1, Ordering::SeqCst);
            Err(Status::internal("Failed"))
        };

        let (mut hook, client) = MockService::default().with_put(put_api).start().await;

        let resp = client
            .put("pre1", &MerkleHash::default(), vec![0], vec![31, 54, 63])
            .await;
        assert_eq!(3, count.load(Ordering::SeqCst));
        assert!(resp.is_err());
        hook.async_drop().await
    }

    #[tokio::test]
    async fn test_put_no_retries() {
        let count = Arc::new(AtomicU32::new(0));
        let put_count = count.clone();
        let put_api = move |req: Request<PutRequest>| {
            assert_eq!(req.into_inner().chunk_boundaries, vec![32, 95, 63]);
            put_count.fetch_add(1, Ordering::SeqCst);
            Err(Status::internal("Failed"))
        };
        let (mut hook, client) = MockService::default()
            .with_put(put_api)
            .start_with_retry_strategy(RetryStrategy::new(0, 1))
            .await;

        let resp = client
            .put("pre1", &MerkleHash::default(), vec![0], vec![32, 95, 63])
            .await;
        assert_eq!(1, count.load(Ordering::SeqCst));
        assert!(resp.is_err());
        hook.async_drop().await
    }

    #[tokio::test]
    async fn test_put_application_error() {
        let count = Arc::new(AtomicU32::new(0));
        let put_count = count.clone();
        let put_api = move |req: Request<PutRequest>| {
            assert_eq!(req.into_inner().chunk_boundaries, vec![32, 56, 63]);
            put_count.fetch_add(1, Ordering::SeqCst);
            Err(Status::failed_precondition("Failed precondition"))
        };
        let (mut hook, client) = MockService::default().with_put(put_api).start().await;

        let resp = client
            .put("pre1", &MerkleHash::default(), vec![0], vec![32, 56, 63])
            .await;
        assert_eq!(1, count.load(Ordering::SeqCst));
        assert!(resp.is_err());
        hook.async_drop().await
    }

    #[test]
    fn metadata_header_interceptor_test() {
        const XET_VERSION: &str = "0.1.0";
        let cas_connection_cofig: CasConnectionConfig = CasConnectionConfig::new(
            "".to_string(),
            "xet_user".to_string(),
            "xet_auth".to_string(),
            vec!["example".to_string()],
            XET_VERSION.to_string(),
        );
        let mut mh_interceptor = MetadataHeaderInterceptor::new(cas_connection_cofig);
        let request = Request::new(());

        {
            // scoped so md reference to request is dropped
            let md = request.metadata();
            assert!(md.get(USER_ID_HEADER).is_none());
            assert!(md.get(REQUEST_ID_HEADER).is_none());
            assert!(md.get(REPO_PATHS_HEADER).is_none());
            assert!(md.get(GIT_XET_VERSION_HEADER).is_none());
            assert!(md.get(CAS_PROTOCOL_VERSION_HEADER).is_none());
        }
        let request = mh_interceptor.call(request).unwrap();

        let md = request.metadata();
        let user_id_val = md.get(USER_ID_HEADER).unwrap();
        assert_eq!(user_id_val.to_str().unwrap(), "xet_user");
        let repo_path_val = md.get_bin(REPO_PATHS_HEADER).unwrap();
        assert_eq!(repo_path_val.to_bytes().unwrap().as_ref(), b"[\"example\"]");
        assert!(md.get(REQUEST_ID_HEADER).is_some());

        assert!(md.get(GIT_XET_VERSION_HEADER).is_some());
        let xet_version = md.get(GIT_XET_VERSION_HEADER).unwrap().to_str().unwrap();
        assert_eq!(xet_version, XET_VERSION);

        // check that global static CAS_PROTOCOL_VERSION is what's set in the header
        assert!(md.get(CAS_PROTOCOL_VERSION_HEADER).is_some());
        let cas_protocol_version = md
            .get(CAS_PROTOCOL_VERSION_HEADER)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(cas_protocol_version, CAS_PROTOCOL_VERSION.as_str());

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
            );
            let mut mh_interceptor = MetadataHeaderInterceptor::new(config);
            let request = Request::new(());
            let request = mh_interceptor.call(request).unwrap();
            let md = request.metadata();
            let repo_path_val = md.get_bin(REPO_PATHS_HEADER).unwrap();
            let repo_path_str =
                String::from_utf8(repo_path_val.to_bytes().unwrap().to_vec()).unwrap();
            let vec_of_strings: Vec<String> =
                serde_json::from_str(repo_path_str.as_str()).expect("Failed to deserialize JSON");
            assert_eq!(vec_of_strings, inner_vec);
        }
    }
}
