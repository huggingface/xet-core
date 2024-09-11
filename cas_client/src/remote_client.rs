use async_trait::async_trait;
use cas::singleflight;
use itertools::Itertools;
use lazy_static::lazy_static;
use tracing::{debug, debug_span, error, info, info_span, Instrument};

use merklehash::MerkleHash;

use cas::common::CompressionScheme;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::cas_connection_pool::{self, CasConnectionConfig, FromConnectionConfig};
use crate::data_transport::DataTransport;
use crate::error::{CasClientError, Result};
use crate::grpc::{EndpointsInfo, GrpcClient};
use crate::Client;
use retry_strategy::RetryStrategy;

/// cas protocol version as seen from the client
/// cas protocol determines the parameters and protocols used for
/// cas operations
///
/// 0.1.0:
///     grpc initiate; port 443; scheme https
///     h2 get + h2 put; port 443; scheme http; host from initiate rpc response
///     grpc put_complete; port 5000; scheme http; host from initiate rpc response
/// 0.2.0:
///     grpc initiate; port 443; scheme https
///     h2 get + h2 put; port, host, and scheme from initiate rpc response
///     grpc put_complete; port, host, and scheme from initiate rpc response
///     defaults to 0.1.0 if initiate does not respond with required info
/// 0.3.0:
///     grpc initiate; port 443; scheme https; widely trusted certificate
///     h2 get + h2 put; port, host and scheme from initiate rpc response; includes custom root certificate authority
///     grpc put_complete; port, host and scheme from initiate rpc response; includes custom root certificate authority
///     defaults to 0.2.0 if initiate does not respond with correct info
const _CAS_PROTOCOL_VERSION: &str = "0.3.0";

lazy_static! {
    pub static ref CAS_PROTOCOL_VERSION: String =
        std::env::var("XET_CAS_PROTOCOL_VERSION").unwrap_or(_CAS_PROTOCOL_VERSION.to_string());
}

// Completely arbitrary CAS size for using a single-hit put call.
// This should be tuned after performance testing.
const _MINIMUM_DATA_TRANSPORT_UPLOAD_SIZE: usize = 500;

const PUT_MAX_RETRIES: usize = 3;
const PUT_RETRY_DELAY_MS: u64 = 1000;

// We have different pool sizes since the GRPC connections are shorter-lived and
// thus, not as many of them are needed. This helps reduce the impact that connection
// creation can have (which, on MacOS, can be significant (hundreds of ms)).
const H2_TRANSPORT_POOL_SIZE: usize = 16;

type DataTransportPoolMap = cas_connection_pool::ConnectionPoolMap<DataTransport>;

// Apply an id for instrumentation when new connections are created to help with
// debugging / investigating performance issues related to connection creation.
lazy_static::lazy_static! {
    static ref GRPC_CLIENT_ID: AtomicUsize = AtomicUsize::new(0);
    static ref H2_CLIENT_ID: AtomicUsize = AtomicUsize::new(0);
}

#[async_trait]
impl FromConnectionConfig for DataTransport {
    async fn new_from_connection_config(config: CasConnectionConfig) -> Result<DataTransport> {
        let id = H2_CLIENT_ID.fetch_add(1, Ordering::SeqCst);
        Ok(DataTransport::from_config(config)
            .instrument(info_span!("transport.connect", id))
            .await?)
    }
}

#[async_trait]
impl FromConnectionConfig for GrpcClient {
    async fn new_from_connection_config(config: CasConnectionConfig) -> Result<GrpcClient> {
        let id = GRPC_CLIENT_ID.fetch_add(1, Ordering::SeqCst);
        Ok(GrpcClient::from_config(config)
            .instrument(info_span!("grpc.connect", id))
            .await?)
    }
}

/// CAS Remote client. This negotiates between the control plane (gRPC)
/// and data plane (HTTP) to optimize the uploads and fetches according to
/// the network, file size, and other dynamic qualities.
#[derive(Debug)]
pub struct RemoteClient {
    lb_endpoint: String,
    user_id: String,
    auth: String,
    repo_paths: Vec<String>,
    grpc_connection_map: Arc<Mutex<HashMap<String, GrpcClient>>>,
    dt_connection_map: DataTransportPoolMap,
    length_singleflight: singleflight::Group<u64, CasClientError>,
    length_cache: Arc<Mutex<HashMap<String, u64>>>,
    git_xet_version: String,
}

// DTO's for organization moving around endpoint info
#[derive(Clone)]
struct InitiateResponseEndpointInfo {
    endpoint: String,
    root_ca: String,
}

#[derive(Clone)]
struct InitiateResponseEndpoints {
    h2: InitiateResponseEndpointInfo,
    put_complete: InitiateResponseEndpointInfo,
    accepted_encodings: Vec<CompressionScheme>,
}

impl RemoteClient {
    pub fn new(
        lb_endpoint: String,
        user_id: String,
        auth: String,
        repo_paths: Vec<String>,
        grpc_connection_map: Mutex<HashMap<String, GrpcClient>>,
        dt_connection_map: DataTransportPoolMap,
        git_xet_version: String,
    ) -> Self {
        Self {
            lb_endpoint,
            user_id,
            auth,
            repo_paths,
            grpc_connection_map: Arc::new(grpc_connection_map),
            dt_connection_map,
            length_singleflight: singleflight::Group::new(),
            length_cache: Arc::new(Mutex::new(HashMap::new())),
            git_xet_version,
        }
    }

    pub async fn from_config(
        endpoint: &str,
        user_id: &str,
        auth: &str,
        repo_paths: Vec<String>,
        git_xet_version: String,
    ) -> Self {
        // optionally switch between a CAS and a local server running on CAS_GRPC_PORT and
        // CAS_HTTP_PORT
        Self::new(
            endpoint.to_string(),
            String::from(user_id),
            String::from(auth),
            repo_paths,
            Mutex::new(HashMap::new()),
            cas_connection_pool::ConnectionPoolMap::new_with_pool_size(H2_TRANSPORT_POOL_SIZE),
            git_xet_version,
        )
    }

    /// utility to generate connection config for an endpoint and other owned information
    /// currently only other owned info is `user_id`
    fn get_cas_connection_config_for_endpoint(&self, endpoint: String) -> CasConnectionConfig {
        CasConnectionConfig::new(
            endpoint,
            self.user_id.clone(),
            self.auth.clone(),
            self.repo_paths.clone(),
            self.git_xet_version.clone(),
        )
    }

    async fn get_grpc_connection_for_config(
        &self,
        cas_connection_config: CasConnectionConfig,
    ) -> Result<GrpcClient> {
        Self::get_grpc_connection_for_config_from_map(
            self.grpc_connection_map.clone(),
            cas_connection_config,
        )
        .await
    }

    /// makes an initiate call to the ALB endpoint and returns
    /// a tuple of 2 strings, the first being the http direct endpoint
    /// and the second is the grpc direct endpoint
    async fn initiate_cas_server_query(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        len: usize,
    ) -> Result<InitiateResponseEndpoints> {
        let cas_connection_config =
            self.get_cas_connection_config_for_endpoint(self.lb_endpoint.clone());
        let lb_grpc_client = self
            .get_grpc_connection_for_config(cas_connection_config)
            .await?;

        let EndpointsInfo {
            data_plane_endpoint,
            put_complete_endpoint,
            accepted_encodings,
        } = lb_grpc_client.initiate(prefix, hash, len).await?;
        drop(lb_grpc_client);

        debug!("cas initiate response; data plane endpoint: {data_plane_endpoint}; put complete endpoint: {put_complete_endpoint}");

        Ok(InitiateResponseEndpoints {
            h2: InitiateResponseEndpointInfo {
                endpoint: data_plane_endpoint.to_string(),
                root_ca: data_plane_endpoint.root_ca_certificate,
            },
            put_complete: InitiateResponseEndpointInfo {
                endpoint: put_complete_endpoint.to_string(),
                root_ca: put_complete_endpoint.root_ca_certificate,
            },
            accepted_encodings,
        })
    }

    async fn put_impl_h2(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: &[u8],
        chunk_boundaries: &[u64],
    ) -> Result<()> {
        debug!("H2 Put executed with {} {}", prefix, hash);
        let InitiateResponseEndpoints {
            h2,
            put_complete,
            accepted_encodings,
        } = self
            .initiate_cas_server_query(prefix, hash, data.len())
            .instrument(debug_span!("remote_client.initiate"))
            .await?;

        let encoding = choose_encoding(accepted_encodings);

        debug!("H2 Put initiate response h2 endpoint: {}, put complete endpoint {}\nh2 cert: {}, put complete cert {}", h2.endpoint, put_complete.endpoint, h2.root_ca, put_complete.root_ca);

        {
            // separate scoped to drop transport so that the connection can be reclaimed by the pool
            let transport = self
                .dt_connection_map
                .get_connection_for_config(
                    self.get_cas_connection_config_for_endpoint(h2.endpoint)
                        .with_root_ca(h2.root_ca),
                )
                .await?;
            transport
                .put(prefix, hash, data, encoding)
                .instrument(debug_span!("remote_client.put_h2"))
                .await?;
        }

        debug!("Data transport completed");

        let cas_connection_config = self
            .get_cas_connection_config_for_endpoint(put_complete.endpoint)
            .with_root_ca(put_complete.root_ca);
        let grpc_client = self
            .get_grpc_connection_for_config(cas_connection_config)
            .await?;

        debug!(
            "Received grpc connection from pool: {}",
            grpc_client.endpoint
        );

        grpc_client
            .put_complete(prefix, hash, chunk_boundaries)
            .await
    }

    // default implementation, parallel unary
    #[allow(dead_code)]
    async fn put_impl_unary(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<()> {
        debug!("Unary Put executed with {} {}", prefix, hash);

        let cas_connection_config =
            self.get_cas_connection_config_for_endpoint(self.lb_endpoint.clone());
        let grpc_client = self
            .get_grpc_connection_for_config(cas_connection_config)
            .await?;

        grpc_client.put(prefix, hash, data, chunk_boundaries).await
    }

    // Default implementation, parallel unary
    #[allow(dead_code)]
    async fn get_impl_unary(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>> {
        let cas_connection_config =
            self.get_cas_connection_config_for_endpoint(self.lb_endpoint.clone());
        let grpc_client = self
            .get_grpc_connection_for_config(cas_connection_config)
            .await?;

        grpc_client.get(prefix, hash).await
    }

    async fn get_impl_h2(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>> {
        debug!("H2 Get executed with {} {}", prefix, hash);

        let InitiateResponseEndpoints { h2, .. } = self
            .initiate_cas_server_query(prefix, hash, 0)
            .instrument(debug_span!("remote_client.initiate"))
            .await?;

        let cas_connection_config = self
            .get_cas_connection_config_for_endpoint(h2.endpoint)
            .with_root_ca(h2.root_ca);
        let transport = self
            .dt_connection_map
            .get_connection_for_config(cas_connection_config)
            .instrument(debug_span!("remote_client.get_transport_connection"))
            .await?;
        let data = transport
            .get(prefix, hash)
            .instrument(debug_span!("remote_client.h2_get"))
            .await?;
        drop(transport);

        debug!("Data transport completed");
        Ok(data)
    }

    // Default implementation, parallel unary
    #[allow(dead_code)]
    async fn get_object_range_impl_unary(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>> {
        debug!("Unary GetRange executed with {} {}", prefix, hash);

        let cas_connection_config =
            self.get_cas_connection_config_for_endpoint(self.lb_endpoint.clone());
        let grpc_client = self
            .get_grpc_connection_for_config(cas_connection_config)
            .await?;

        grpc_client.get_object_range(prefix, hash, ranges).await
    }

    async fn get_object_range_impl_h2(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>> {
        debug!("H2 GetRange executed with {} {}", prefix, hash);

        let InitiateResponseEndpoints { h2, .. } = self
            .initiate_cas_server_query(prefix, hash, 0)
            .instrument(debug_span!("remote_client.initiate"))
            .await?;

        let cas_connection_config = self
            .get_cas_connection_config_for_endpoint(h2.endpoint)
            .with_root_ca(h2.root_ca);
        let transport = self
            .dt_connection_map
            .get_connection_for_config(cas_connection_config)
            .await?;

        let mut handlers = Vec::new();
        for range in ranges {
            handlers.push(transport.get_range(prefix, hash, range));
        }
        let results = futures::future::join_all(handlers).await;
        let errors: Vec<String> = results
            .iter()
            .filter_map(|r| r.as_deref().err().map(|s| s.to_string()))
            .collect();
        if !errors.is_empty() {
            let error_description: String = errors.join("-");
            Err(CasClientError::BatchError(error_description))?;
        }
        let data = results
            .into_iter()
            // unwrap is safe since we verified in the above if that no elements have an error
            .map(|r| r.unwrap())
            .collect_vec();
        Ok(data)
    }
}

fn choose_encoding(accepted_encodings: Vec<CompressionScheme>) -> CompressionScheme {
    if accepted_encodings.is_empty() {
        return CompressionScheme::None;
    }
    if accepted_encodings.contains(&CompressionScheme::Lz4) {
        return CompressionScheme::Lz4;
    }
    CompressionScheme::None
}

fn cas_client_error_retriable(err: &CasClientError) -> bool {
    // we do not retry the logical errors
    !matches!(
        err,
        CasClientError::InvalidRange
            | CasClientError::InvalidArguments
            | CasClientError::HashMismatch
    )
}

#[async_trait]
impl Client for RemoteClient {
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_boundaries: Vec<u64>,
    ) -> Result<()> {
        // We first check if the block already exists, to avoid an unnecessary upload
        if let Ok(xorb_size) = self.get_length(prefix, hash).await {
            if xorb_size > 0 {
                return Ok(());
            }
        }
        // We could potentially narrow down the error conditions
        // further, but that gets complicated.
        // So we just do something pretty coarse-grained
        let strategy = RetryStrategy::new(PUT_MAX_RETRIES, PUT_RETRY_DELAY_MS);
        let res = strategy
            .retry(
                || async {
                    self.put_impl_h2(prefix, hash, &data, &chunk_boundaries)
                        .await
                },
                |e| {
                    let retry = cas_client_error_retriable(e);
                    if retry {
                        info!("Put error {:?}. Retrying...", e);
                    }
                    retry
                },
            )
            .await;

        if let Err(ref e) = res {
            if cas_client_error_retriable(e) {
                error!("Too many failures writing {:?}: {:?}.", hash, e);
            }
        }
        res
    }

    async fn flush(&self) -> Result<()> {
        // this client does not background so no flush is needed
        Ok(())
    }

    async fn get(&self, prefix: &str, hash: &MerkleHash) -> Result<Vec<u8>> {
        self.get_impl_h2(prefix, hash).await
    }

    async fn get_object_range(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        ranges: Vec<(u64, u64)>,
    ) -> Result<Vec<Vec<u8>>> {
        self.get_object_range_impl_h2(prefix, hash, ranges).await
    }

    async fn get_length(&self, prefix: &str, hash: &MerkleHash) -> Result<u64> {
        let key = format!("{}:{}", prefix, hash.hex());

        let cache = self.length_cache.clone();

        // See if it's in the cache first before we try to launch it; this is cheap.
        {
            let cache = cache.lock().await;
            if let Some(v) = cache.get(&key) {
                return Ok(*v);
            }
        }
        let cas_connection_config =
            self.get_cas_connection_config_for_endpoint(self.lb_endpoint.clone());
        let connection_map = self.grpc_connection_map.clone();

        let (res, _dedup) = self
            .length_singleflight
            .work(
                &key,
                Self::get_length_from_remote(
                    connection_map,
                    cas_connection_config,
                    cache,
                    prefix.to_string(),
                    *hash,
                ),
            )
            .await;

        return match res {
            Ok(v) => Ok(v),
            Err(singleflight::SingleflightError::InternalError(e)) => Err(e),
            Err(e) => Err(CasClientError::InternalError(anyhow::Error::from(e))),
        };
    }
}

// static functions that can be used in spawned tasks
impl RemoteClient {
    async fn get_length_from_remote(
        connection_map: Arc<Mutex<HashMap<String, GrpcClient>>>,
        cas_connection_config: CasConnectionConfig,
        cache: Arc<Mutex<HashMap<String, u64>>>,
        prefix: String,
        hash: MerkleHash,
    ) -> Result<u64> {
        let key = format!("{}:{}", prefix, hash.hex());
        {
            let cache = cache.lock().await;
            if let Some(v) = cache.get(&key) {
                return Ok(*v);
            }
        }

        let grpc_client =
            Self::get_grpc_connection_for_config_from_map(connection_map, cas_connection_config)
                .await?;

        debug!("RemoteClient: GetLength of {}/{}", prefix, hash);

        let res = grpc_client.get_length(&prefix, &hash).await?;

        debug!(
            "RemoteClient: GetLength of {}/{} request complete",
            prefix, hash
        );

        // See if it's in the cache
        {
            let mut cache = cache.lock().await;
            let _ = cache.insert(key.clone(), res);
        }

        Ok(res)
    }

    async fn get_grpc_connection_for_config_from_map(
        grpc_connection_map: Arc<Mutex<HashMap<String, GrpcClient>>>,
        cas_connection_config: CasConnectionConfig,
    ) -> Result<GrpcClient> {
        let mut map = grpc_connection_map.lock().await;
        if let Some(client) = map.get(&cas_connection_config.endpoint) {
            return Ok(client.clone());
        }
        // yes the lock is held through to endpoint creation.
        // While strictly by locking patterns we should release the
        // lock here, create the client, then re-acquire the lock to insert
        // into the map, in practice *thousands* of threads could call this
        // method simultaneously leading to a "race" where we create
        // thousands of connections.
        //
        // Really we need to "single-flight" connection creation per endpoint.
        // Since each RemoteClient really connects to only 1 endpoint,
        // just locking the whole method here pretty much does what we need.
        let endpoint = cas_connection_config.endpoint.clone();
        let new_client = GrpcClient::new_from_connection_config(cas_connection_config).await?;
        map.insert(endpoint, new_client.clone());
        Ok(new_client)
    }
}
