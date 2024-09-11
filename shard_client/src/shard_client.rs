use async_trait::async_trait;
use http::Uri;
use itertools::Itertools;
use mdb_shard::error::MDBShardError;
use mdb_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use mdb_shard::shard_dedup_probe::ShardDedupProber;
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use merkledb::aggregate_hashes::with_salt;
use opentelemetry::propagation::{Injector, TextMapPropagator};
use retry_strategy::RetryStrategy;
use std::env::VarError;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tonic::codegen::InterceptedService;
use tonic::metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue};
use tonic::service::Interceptor;
use tonic::Response;
use tonic::{transport::Channel, Request, Status};
use tracing::{debug, info, warn, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use cas::{
    constants::*,
    shard::{
        shard_client::ShardClient, QueryChunkRequest, QueryChunkResponse, QueryFileRequest,
        QueryFileResponse, SyncShardRequest, SyncShardResponse, SyncShardWithSaltRequest,
    },
};
use cas_client::grpc::{
    get_key_for_request, is_status_retriable_and_print, print_final_retry_error,
};
use merklehash::MerkleHash;

use crate::{
    error::{Result, ShardClientError},
    RegistrationClient, ShardClientInterface, ShardConnectionConfig,
};
pub type ShardClientType = ShardClient<InterceptedService<Channel, MetadataHeaderInterceptor>>;

const DEFAULT_VERSION: &str = "0.0.0";

const HTTP2_KEEPALIVE_TIMEOUT_SEC: u64 = 20;
const HTTP2_KEEPALIVE_INTERVAL_SEC: u64 = 1;
const NUM_RETRIES: usize = 5;
const BASE_RETRY_DELAY_MS: u64 = 3000;

// production ready settings
const INITIATE_CAS_SCHEME: &str = "https";
const HTTP_CAS_SCHEME: &str = "http";

// up from default 4MB which is not enough for reconstructing _really_ large files
const GRPC_MESSAGE_LIMIT: usize = 256 * 1024 * 1024;

lazy_static::lazy_static! {
    static ref DEFAULT_UUID: Uuid = Uuid::new_v4();
    static ref REQUEST_COUNTER: AtomicUsize = AtomicUsize::new(0);
    static ref TRACE_FORWARDING: AtomicBool = AtomicBool::new(false);
}

async fn get_channel(endpoint: &str) -> anyhow::Result<Channel> {
    debug!("shard client get_channel: server name: {}", endpoint);
    let mut server_uri: Uri = endpoint.parse()?;

    // supports an absolute URI (above) or just the host:port (below)
    // only used on first endpoint, all other endpoints should come from CAS
    // with scheme info already included
    // in local/witt modes overriden CAS initial URI should include scheme e.g.
    //  http://localhost:40000
    if server_uri.scheme().is_none() {
        let scheme = if cfg!(test) {
            HTTP_CAS_SCHEME
        } else {
            INITIATE_CAS_SCHEME
        };
        server_uri = format!("{scheme}://{endpoint}").parse().unwrap();
    }

    debug!("Server URI: {}", server_uri);

    let channel = Channel::builder(server_uri)
        .keep_alive_timeout(Duration::new(HTTP2_KEEPALIVE_TIMEOUT_SEC, 0))
        .http2_keep_alive_interval(Duration::new(HTTP2_KEEPALIVE_INTERVAL_SEC, 0))
        .timeout(Duration::new(GRPC_TIMEOUT_SEC, 0))
        .connect_timeout(Duration::new(GRPC_TIMEOUT_SEC, 0))
        .connect()
        .await?;
    Ok(channel)
}

pub async fn get_client(shard_connection_config: ShardConnectionConfig) -> Result<ShardClientType> {
    let endpoint = shard_connection_config.endpoint.as_str();

    if endpoint.starts_with("local://") {
        return Err(ShardClientError::Other(
            "Cannot connect to shard client using local:// CAS config.".to_owned(),
        ));
    }

    let timeout_channel = get_channel(endpoint).await?;

    let client: ShardClientType = ShardClient::with_interceptor(
        timeout_channel,
        MetadataHeaderInterceptor::new(shard_connection_config),
    )
    .max_decoding_message_size(GRPC_MESSAGE_LIMIT)
    .max_encoding_message_size(GRPC_MESSAGE_LIMIT);
    Ok(client)
}

/// Adds common metadata headers to all requests. Currently, this includes
/// authorization and xet-user-id.
/// TODO: at some point, we should re-evaluate how we authenticate/authorize requests to CAS.
#[derive(Debug, Clone)]
pub struct MetadataHeaderInterceptor {
    config: ShardConnectionConfig,
}

impl MetadataHeaderInterceptor {
    fn new(config: ShardConnectionConfig) -> MetadataHeaderInterceptor {
        MetadataHeaderInterceptor { config }
    }
}

impl Interceptor for MetadataHeaderInterceptor {
    // note original Interceptor trait accepts non-mut request
    // but may accept mut request like in this case
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        request.set_timeout(Duration::new(GRPC_TIMEOUT_SEC, 0));
        let metadata = request.metadata_mut();

        let token = MetadataValue::from_static("Bearer some-secret-token");
        metadata.insert(AUTHORIZATION_HEADER, token);

        // pass user_id and repo_paths received from xetconfig
        let user_id = get_metadata_ascii_from_str_with_default(&self.config.user_id, DEFAULT_USER);
        metadata.insert(USER_ID_HEADER, user_id);

        let git_xet_version =
            get_metadata_ascii_from_str_with_default(&self.config.git_xet_version, DEFAULT_VERSION);
        metadata.insert(GIT_XET_VERSION_HEADER, git_xet_version);

        let cas_protocol_version: MetadataValue<Ascii> =
            MetadataValue::from_static(&cas_client::CAS_PROTOCOL_VERSION);
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
            MetadataValue::from_str(&request_id).unwrap(),
        );

        Ok(request)
    }
}

pub fn _set_trace_forwarding(should_enable: bool) {
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
pub struct GrpcShardClient {
    pub endpoint: String,
    client: ShardClientType,
    retry_strategy: RetryStrategy,
}

impl Clone for GrpcShardClient {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            client: self.client.clone(),
            retry_strategy: self.retry_strategy,
        }
    }
}

impl GrpcShardClient {
    pub fn new(endpoint: String, client: ShardClientType, retry_strategy: RetryStrategy) -> Self {
        Self {
            endpoint,
            client,
            retry_strategy,
        }
    }

    pub async fn from_config(
        shard_connection_config: ShardConnectionConfig,
    ) -> Result<GrpcShardClient> {
        debug!("Creating GrpcShardClient from config: {shard_connection_config:?}");
        let endpoint = shard_connection_config.endpoint.clone();
        let client: ShardClientType = get_client(shard_connection_config).await?;
        // Retry policy: Exponential backoff starting at BASE_RETRY_DELAY_MS and retrying NUM_RETRIES times
        let retry_strategy = RetryStrategy::new(NUM_RETRIES, BASE_RETRY_DELAY_MS);
        Ok(GrpcShardClient::new(endpoint, client, retry_strategy))
    }
}

#[async_trait]
impl RegistrationClient for GrpcShardClient {
    #[tracing::instrument(skip_all, name = "shard.client", err, fields(prefix = prefix, hash = hash.hex().as_str(), api = "register_shard", request_id = tracing::field::Empty))]
    async fn register_shard_v1(&self, prefix: &str, hash: &MerkleHash, force: bool) -> Result<()> {
        info!("Registering shard {prefix}/{hash:?}");
        inc_request_id();
        Span::current().record("request_id", &get_request_id());
        debug!(
            "GrpcShardClient Req {}: register {}/{} as shard",
            get_request_id(),
            prefix,
            hash,
        );
        let request = SyncShardRequest {
            key: Some(get_key_for_request(prefix, hash)),
            force_sync: force,
        };

        let response: Response<SyncShardResponse> = self
            .retry_strategy
            .retry(
                || async {
                    let req = Request::new(request.clone());
                    debug!("GrpcShardClient register_shard: Attemping call to sync_shard, req = {req:?})");
                    self.client.clone().sync_shard(req).await
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(print_final_retry_error)
            .map_err(|e| {
                warn!(
                    "GrpcShardClient Req {}: Error on shard register {}/{:?} : {:?}",
                    get_request_id(),
                    prefix,
                    hash,
                    e
                );
                ShardClientError::GrpcClientError(anyhow::Error::from(e))
            })?;

        // It appears that both exists and sync_performed achieve the correct results.
        if response.into_inner().response == 0
        /*SyncShardResponseType::Exists */
        {
            info!("Shard {prefix:?}/{hash:?} already synced; skipping.");
        } else {
            info!("Shard {prefix:?}/{hash:?} synced.");
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, name = "shard.client", err, fields(prefix = prefix, hash = format!("{hash}"), salt = format!("{salt:x?}"), api = "register_shard_with_salt", request_id = tracing::field::Empty))]
    async fn register_shard_with_salt(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        force: bool,
        salt: &[u8; 32],
    ) -> Result<()> {
        info!("Registering shard {prefix}/{hash} (w/ salt)");
        inc_request_id();
        Span::current().record("request_id", &get_request_id());
        debug!(
            "GrpcShardClient Req {}: register {prefix}/{hash} as shard",
            get_request_id(),
        );
        let request = SyncShardWithSaltRequest {
            ssr: Some(SyncShardRequest {
                key: Some(get_key_for_request(prefix, hash)),
                force_sync: force,
            }),
            salt: salt.to_vec(),
        };

        let response: Response<SyncShardResponse> = self
            .retry_strategy
            .retry(
                || async {
                    let req = Request::new(request.clone());
                    debug!("GrpcShardClient register_shard_with_salt: Attemping call to sync_shard_with_salt, req = {req:?})");
                    self.client.clone().sync_shard_with_salt(req).await
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(print_final_retry_error)
            .map_err(|e| {
                warn!(
                    "GrpcShardClient Req {}: Error on shard register {prefix}/{hash} with salt {salt:x?} : {e:?}",
                    get_request_id(),
                );
                ShardClientError::GrpcClientError(anyhow::Error::from(e))
            })?;

        // It appears that both exists and sync_performed achieve the correct results.
        if response.into_inner().response == 0
        /*SyncShardResponseType::Exists */
        {
            info!("Shard {prefix}/{hash} already synced; skipping.");
        } else {
            info!("Shard {prefix}/{hash} synced with global dedup.");
        }
        Ok(())
    }
}

#[async_trait]
impl FileReconstructor for GrpcShardClient {
    /// Query the shard server for the file reconstruction info.
    /// Returns the FileInfo for reconstructing the file and the shard ID that
    /// defines the file info.
    ///
    /// TODO: record the shards that are  
    #[tracing::instrument(skip_all, name = "shard.client", err, fields(file_hash = file_hash.hex().as_str(), api = "get_file_reconstruction_info", request_id = tracing::field::Empty))]
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> mdb_shard::error::Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        inc_request_id();
        Span::current().record("request_id", &get_request_id());
        debug!(
            "GrpcShardClient Req {}. get_file_reconstruction_info for fileid of {}",
            get_request_id(),
            file_hash
        );
        let request = QueryFileRequest {
            file_id: file_hash.into(),
        };
        let response: Response<QueryFileResponse> = self
            .retry_strategy
            .retry(
                || async {
                    let req = Request::new(request.clone());
                    self.client.clone().query_file(req).await
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(print_final_retry_error)
            .map_err(|e| {
                warn!(
                    "GrpcShardClient Req {}. Error on get_reconstruction {} : {:?}",
                    get_request_id(),
                    file_hash,
                    e
                );
                MDBShardError::GrpcClientError(anyhow::Error::from(e))
            })?;

        let response_info = response.into_inner();

        Ok(Some((
            MDBFileInfo {
                metadata: FileDataSequenceHeader::new(
                    *file_hash,
                    response_info.reconstruction.len(),
                ),
                segments: response_info
                    .reconstruction
                    .into_iter()
                    .map(|ce| {
                        FileDataSequenceEntry::new(
                            ce.cas_id[..].try_into().unwrap(),
                            ce.unpacked_length,
                            ce.range.as_ref().unwrap().start,
                            ce.range.as_ref().unwrap().end,
                        )
                    })
                    .collect(),
            },
            response_info
                .shard_id
                .and_then(|k| MerkleHash::try_from(&k.hash[..]).ok()),
        )))
    }
}

#[async_trait]
impl ShardDedupProber for GrpcShardClient {
    #[tracing::instrument(skip_all, name = "shard.client", err, fields(prefix = prefix, chunk_hash = format!("{chunk_hash:?}"), api = "register_shard_with_salt", request_id = tracing::field::Empty))]
    async fn get_dedup_shards(
        &self,
        prefix: &str,
        chunk_hash: &[MerkleHash],
        salt: &[u8; 32],
    ) -> mdb_shard::error::Result<Vec<MerkleHash>> {
        inc_request_id();
        Span::current().record("request_id", &get_request_id());
        debug!(
            "GrpcShardClient Req {}. get_dedup_shards for chunk hashes {prefix} / {chunk_hash:?}",
            get_request_id(),
        );
        let request = QueryChunkRequest {
            prefix: prefix.into(),
            chunk: chunk_hash
                .iter()
                .filter_map(|chunk| with_salt(chunk, salt).ok())
                .map(|salted_chunk| salted_chunk.as_bytes().to_vec())
                .collect_vec(),
        };
        let response: Response<QueryChunkResponse> = self
            .retry_strategy
            .retry(
                || async {
                    let req = Request::new(request.clone());
                    self.client.clone().query_chunk(req).await
                },
                is_status_retriable_and_print,
            )
            .await
            .map_err(print_final_retry_error)
            .map_err(|e| {
                warn!(
                    "GrpcShardClient Req {}. Error on get_dedup_shards {prefix} / {chunk_hash:?} : {e:?}",
                    get_request_id(),
                );
                MDBShardError::GrpcClientError(anyhow::Error::from(e))
            })?;

        let response_info = response.into_inner();

        Ok(response_info
            .shard
            .iter()
            .flat_map(|hash| MerkleHash::try_from(&hash[..]))
            .collect_vec())
    }
}

impl ShardClientInterface for GrpcShardClient {}

// TODO: copy tests from grpc
