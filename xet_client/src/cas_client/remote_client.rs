use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::anyhow;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use futures::TryStreamExt;
use http::HeaderValue;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderName, RANGE};
use reqwest::{Body, Response, StatusCode, Url};
use reqwest_middleware::ClientWithMiddleware;
use sha2::{Digest, Sha256};
use tracing::{debug, event, info, instrument, warn};
use xet_core_structures::merklehash::MerkleHash;
use xet_core_structures::metadata_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use xet_core_structures::xorb_object::SerializedXorbObject;
use xet_runtime::core::XetContext;

use super::adaptive_concurrency::{
    AdaptiveConcurrencyController, ConnectionPermit, download_controller, upload_controller,
};
use super::auth::AuthConfig;
use super::interface::{ShardUploadProgressCallback, URLProvider};
use super::progress_tracked_streams::{
    DownloadProgressStream, ProgressCallback, StreamProgressReporter, UploadProgressStream,
};
use super::retry_wrapper::{RetryWrapper, RetryableReqwestError};
#[cfg(not(target_family = "wasm"))]
use super::shard_upload_v2::read_shard_upload_ndjson;
#[cfg(not(target_family = "wasm"))]
use super::telemetry::TransferTelemetry;
use super::{Client, INFORMATION_LOG_LEVEL};
use crate::cas_client::ShardUploadProgressType;
use crate::cas_types::{
    BatchQueryReconstructionResponse, FileChunkHashesResponse, FileRange, HttpRange, Key, QueryReconstructionResponse,
    QueryReconstructionResponseV2, ShardUploadEvent, UploadShardResponse, UploadShardResponseType, UploadXorbResponse,
    X_RANGE_DIRTY_HEADER, XorbCommitItem, XorbCommitResult, XorbCommitStatus, XorbGrant, XorbGrantItem,
    XorbGrantRequest, XorbGrantResponse,
};
use crate::common::http_client::{self, Api};
use crate::error::{ClientError, Result};

pub const CAS_ENDPOINT: &str = "http://localhost:8080";
pub const PREFIX_DEFAULT: &str = "default";

static FN_CALL_ID: AtomicU64 = AtomicU64::new(1);

pub struct RemoteClient {
    pub(crate) ctx: XetContext,
    endpoint: String,
    dry_run: bool,
    http_client: Arc<ClientWithMiddleware>,
    authenticated_http_client: Arc<ClientWithMiddleware>,
    /// Client for the presigned PUTs of the direct upload path: no auth, no logging, no redirects.
    bucket_http_client: Arc<ClientWithMiddleware>,
    /// Set once the endpoint answered 404 to a grant request: no staging bucket there, so the
    /// direct path is skipped for the rest of the session instead of costing one request per xorb.
    direct_upload_unavailable: AtomicBool,
    /// Xorbs staged through the direct path whose commit has not been asked for yet. Flushed by
    /// [`RemoteClient::flush_pending_commits`]; never held across an await.
    pending_commits: Mutex<Vec<PendingCommit>>,
    /// Authenticated client with no read_timeout, used for shard uploads where server-side
    /// processing time scales with file entry count and can exceed the global read_timeout.
    #[cfg(not(target_family = "wasm"))]
    shard_upload_http_client: Arc<ClientWithMiddleware>,
    upload_concurrency_controller: Arc<AdaptiveConcurrencyController>,
    download_concurrency_controller: Arc<AdaptiveConcurrencyController>,
    /// Caches the discovered reconstruction API version (0 = not yet probed, 1 = V1, 2 = V2).
    detected_reconstruction_api_version: AtomicU32,
    /// Caches the discovered shard upload API version (0 = not yet probed, 1 = V1, 2 = V2).
    detected_shard_api_version: AtomicU32,
    /// Per-transfer performance telemetry, or `None` when telemetry is disabled, this is a dry
    /// run, or the endpoint is not http/https. See [`TransferTelemetry::maybe_new`].
    #[cfg(not(target_family = "wasm"))]
    telemetry: Option<Arc<TransferTelemetry>>,
}

impl RemoteClient {
    /// Creates a new RemoteClient with an explicit Unix socket path.
    ///
    /// # Arguments
    /// * `endpoint` - The CAS endpoint URL
    /// * `auth` - Optional authentication configuration
    /// * `session_id` - Session identifier
    /// * `dry_run` - Whether to run in dry-run mode
    /// * `unix_socket_path` - Optional Unix socket path for proxying connections (ignored on non-Unix platforms)
    /// * `custom_headers` - Optional custom headers to include in HTTP requests (should include User-Agent)
    pub fn new_with_socket(
        ctx: XetContext,
        endpoint: &str,
        auth: &Option<AuthConfig>,
        session_id: &str,
        dry_run: bool,
        unix_socket_path: Option<&str>,
        custom_headers: Option<Arc<HeaderMap>>,
    ) -> Arc<Self> {
        let authenticated_http_client = Arc::new(
            http_client::build_auth_http_client(&ctx, auth, session_id, unix_socket_path, custom_headers.clone())
                .unwrap(),
        );

        // Telemetry shares the authenticated client rather than building its own: a second
        // `build_auth_http_client` would create a second `AuthMiddleware` with its own
        // `TokenProvider`, giving telemetry an independent token-refresh cycle against the Hub.
        #[cfg(not(target_family = "wasm"))]
        let telemetry = TransferTelemetry::maybe_new(
            &ctx,
            endpoint,
            session_id,
            dry_run,
            authenticated_http_client.clone(),
            custom_headers.as_deref(),
        );

        let http_client = Arc::new(
            http_client::build_http_client(&ctx, session_id, unix_socket_path, custom_headers.clone()).unwrap(),
        );
        #[cfg(not(target_family = "wasm"))]
        let bucket_http_client = Arc::new(http_client::build_bucket_http_client(custom_headers.clone()).unwrap());
        #[cfg(target_family = "wasm")]
        let bucket_http_client = http_client.clone();

        Arc::new(Self {
            ctx: ctx.clone(),
            endpoint: endpoint.to_string(),
            dry_run,
            authenticated_http_client,
            http_client,
            bucket_http_client,
            direct_upload_unavailable: AtomicBool::new(false),
            pending_commits: Mutex::new(Vec::new()),
            #[cfg(not(target_family = "wasm"))]
            shard_upload_http_client: Arc::new(
                http_client::build_auth_http_client_no_read_timeout(
                    &ctx,
                    auth,
                    session_id,
                    unix_socket_path,
                    custom_headers,
                )
                .unwrap(),
            ),
            upload_concurrency_controller: upload_controller(&ctx, endpoint),
            download_concurrency_controller: download_controller(&ctx, endpoint),
            detected_reconstruction_api_version: AtomicU32::new(0),
            detected_shard_api_version: AtomicU32::new(0),
            #[cfg(not(target_family = "wasm"))]
            telemetry,
        })
    }

    /// Creates a new RemoteClient.
    ///
    /// If `HF_XET_CLIENT_UNIX_SOCKET_PATH` is set in the configuration, this will
    /// automatically use the Unix socket for connections (checked by build_http_client).
    ///
    /// # Arguments
    /// * `endpoint` - The CAS endpoint URL
    /// * `auth` - Optional authentication configuration
    /// * `session_id` - Session identifier
    /// * `dry_run` - Whether to run in dry-run mode
    /// * `custom_headers` - Optional custom headers to include in HTTP requests (should include User-Agent)
    pub fn new(
        ctx: XetContext,
        endpoint: &str,
        auth: &Option<AuthConfig>,
        session_id: &str,
        dry_run: bool,
        custom_headers: Option<Arc<HeaderMap>>,
    ) -> Arc<Self> {
        Self::new_with_socket(ctx, endpoint, auth, session_id, dry_run, None, custom_headers)
    }

    /// Get the endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[cfg(feature = "simulation")]
    pub(crate) fn http_client(&self) -> Arc<ClientWithMiddleware> {
        self.http_client.clone()
    }

    async fn query_dedup_api(&self, prefix: &str, chunk_hash: &MerkleHash) -> Result<Option<Response>> {
        // The API endpoint now only supports non-batched dedup request and
        let key = Key {
            prefix: prefix.into(),
            hash: *chunk_hash,
        };

        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let url = Url::parse(&format!("{}/v1/chunks/{key}", self.endpoint))?;
        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            prefix,
            %chunk_hash,
            "Starting query_dedup API call",
        );

        let client = self.authenticated_http_client.clone();
        let api_tag = "cas::query_dedup";

        let result = RetryWrapper::new(self.ctx.clone(), api_tag)
            .with_429_no_retry()
            .with_expected_404()
            .log_errors_as_info()
            .run(move || client.get(url.clone()).with_extension(Api(api_tag)).send())
            .await;

        if result.as_ref().is_err_and(|e| e.status().is_some()) {
            event!(
                INFORMATION_LOG_LEVEL,
                call_id,
                prefix,
                %chunk_hash,
                result="not_found",
                "Completed query_dedup API call",
            );
            return Ok(None);
        }

        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            prefix,
            %chunk_hash,
            result="found",
            "Completed query_dedup API call",
        );
        Ok(Some(result?))
    }
}

impl RemoteClient {
    async fn get_reconstruction_impl<T>(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
        api_version: &str,
    ) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned + 'static,
    {
        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let url = Url::parse(&format!("{}/{api_version}/reconstructions/{}", self.endpoint, file_id.hex()))?;
        let api_tag = match api_version {
            "v1" => "cas::get_reconstruction_v1",
            "v2" => "cas::get_reconstruction_v2",
            _ => {
                return Err(ClientError::InternalError(anyhow!(
                    "unsupported reconstruction API version: {api_version}"
                )));
            },
        };

        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            %file_id,
            ?bytes_range,
            api_version,
            "Starting get_reconstruction API call",
        );

        let client = self.authenticated_http_client.clone();

        let result: Result<T> = RetryWrapper::new(self.ctx.clone(), api_tag)
            .with_expected_416()
            .run_and_extract_json(move || {
                let mut request = client.get(url.clone()).with_extension(Api(api_tag));
                if let Some(range) = bytes_range {
                    request = request.header(RANGE, HttpRange::from(range).range_header())
                }
                request.send()
            })
            .await;

        match result {
            Ok(response) => {
                event!(
                    INFORMATION_LOG_LEVEL,
                    call_id,
                    %file_id,
                    ?bytes_range,
                    api_version,
                    "Completed get_reconstruction API call"
                );
                Ok(Some(response))
            },
            Err(ClientError::ReqwestError(ref e, _)) if e.status() == Some(StatusCode::RANGE_NOT_SATISFIABLE) => {
                Ok(None)
            },
            Err(e) => Err(e),
        }
    }

    /// V1 reconstruction: returns per-range presigned URLs.
    pub async fn get_reconstruction_v1(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>> {
        self.get_reconstruction_impl(file_id, bytes_range, "v1").await
    }

    /// V2 reconstruction: returns per-xorb multi-range fetch descriptors.
    pub async fn get_reconstruction_v2(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponseV2>> {
        self.get_reconstruction_impl(file_id, bytes_range, "v2").await
    }

    pub(crate) async fn get_reconstruction_with_version_override(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
        forced_version: Option<u32>,
    ) -> Result<Option<QueryReconstructionResponseV2>> {
        // Prefer V2; fall back to V1 on 404/501; persist detected version to
        // avoid repeated fallback attempts.
        let version = match forced_version {
            Some(v) => v,
            None => {
                let detected = self.detected_reconstruction_api_version.load(Ordering::Relaxed);
                if detected != 0 { detected } else { 2 }
            },
        };

        match version {
            2 => match self.get_reconstruction_v2(file_id, bytes_range).await {
                Ok(result) => {
                    if forced_version.is_none() {
                        self.detected_reconstruction_api_version.store(2, Ordering::Relaxed);
                    }
                    Ok(result)
                },
                Err(e)
                    if forced_version.is_none()
                        && matches!(e.status(), Some(StatusCode::NOT_FOUND) | Some(StatusCode::NOT_IMPLEMENTED)) =>
                {
                    info!(status = ?e.status(), "V2 reconstruction not available, falling back to V1");
                    let result = self.get_reconstruction_v1(file_id, bytes_range).await?.map(Into::into);
                    // Store after success to make sure we don't mess up on e.g. network failure.
                    self.detected_reconstruction_api_version.store(1, Ordering::Relaxed);
                    Ok(result)
                },
                Err(e) => Err(e),
            },
            1 => Ok(self.get_reconstruction_v1(file_id, bytes_range).await?.map(Into::into)),
            other => Err(ClientError::InternalError(anyhow!("unsupported reconstruction API version: {other}"))),
        }
    }

    pub(crate) async fn upload_shard_v1(
        &self,
        shard_data: Bytes,
        upload_permit: ConnectionPermit,
        progress_callback: Option<ShardUploadProgressCallback>,
    ) -> Result<()> {
        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let n_upload_bytes = shard_data.len();
        event!(INFORMATION_LOG_LEVEL, call_id, size = n_upload_bytes, "Starting upload_shard API call",);

        let api_tag = "cas::upload_shard";
        let url = Url::parse(&format!("{}/v1/shards", self.endpoint))?;

        #[cfg(not(target_family = "wasm"))]
        let client = self.shard_upload_http_client.clone();

        #[cfg(target_family = "wasm")]
        let client = self.authenticated_http_client.clone();

        let response: UploadShardResponse = RetryWrapper::new(self.ctx.clone(), api_tag)
            .with_connection_permit(upload_permit, Some(shard_data.len() as u64))
            .run_and_extract_json(move || {
                client
                    .post(url.clone())
                    .with_extension(Api(api_tag))
                    .body(shard_data.clone())
                    .send()
            })
            .await?;

        let result = match response.result {
            UploadShardResponseType::Exists => "exists",
            UploadShardResponseType::SyncPerformed => "sync performed",
        };
        event!(INFORMATION_LOG_LEVEL, call_id, size = n_upload_bytes, result, "Completed upload_shard API call",);

        // V1 has no NDJSON progress stream; synthesize transfer + terminal Result so counters
        // (and hub UI) do not remain incomplete after a successful upload / V2→V1 fallback.
        if let Some(cb) = &progress_callback {
            cb(ShardUploadProgressType::Transfer(n_upload_bytes as u64));
            cb(ShardUploadProgressType::Response(&ShardUploadEvent::Result));
        }

        Ok(())
    }

    #[cfg(not(target_family = "wasm"))]
    pub(crate) async fn upload_shard_v2(
        &self,
        shard_data: Bytes,
        upload_permit: ConnectionPermit,
        progress_callback: Option<ShardUploadProgressCallback>,
    ) -> Result<()> {
        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let n_upload_bytes = shard_data.len();
        let api_tag = "cas::upload_shard_v2";
        let url = Url::parse(&format!("{}/v2/shards", self.endpoint))?;

        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            size = n_upload_bytes,
            api_version = "v2",
            "Starting upload_shard API call",
        );

        // Uses `authenticated_http_client` (read timeout enabled). Unlike v1, which needs the
        // no-read-timeout client because server-side validation can be silent for a long time,
        // `/v2/shards` streams NDJSON and the CAS server re-emits the last frame as a heartbeat
        // every ~20s during quiet validation — so the normal client read timeout is sufficient.
        let client = self.authenticated_http_client.clone();

        let block_size = self.ctx.config.client.upload_reporting_block_size;

        // Track reported body bytes so a final failure (e.g. 404 before V1 fallback) can erase
        // them. Retries within this call share `upload_reporter`; StreamProgressReporter's
        // high-water mark already prevents Transfer double-counting across attempts.
        let transferred = Arc::new(AtomicU64::new(0));
        let mut upload_reporter = StreamProgressReporter::new(n_upload_bytes as u64)
            .with_adaptive_concurrency_reporter(upload_permit.get_partial_completion_reporting_function());
        if let Some(cb) = &progress_callback {
            let cb = cb.clone();
            let transferred = transferred.clone();
            upload_reporter = upload_reporter.with_progress_callback(Arc::new(move |delta, _, _| {
                transferred.fetch_add(delta, Ordering::Relaxed);
                cb(ShardUploadProgressType::Transfer(delta));
            }));
        }

        let progress_callback_for_closure = progress_callback.clone();

        // NDJSON parsing runs inside `run_and_process` so `error` frames with
        // `retryable: true` (and transient stream I/O) retry the whole upload via RetryWrapper.
        let result = RetryWrapper::new(self.ctx.clone(), api_tag)
            .with_connection_permit(upload_permit, Some(shard_data.len() as u64))
            .run_and_process(
                move || {
                    let upload_stream = UploadProgressStream::wrap_bytes_as_stream(
                        shard_data.clone(),
                        block_size,
                        upload_reporter.clone(),
                    );
                    client
                        .post(url.clone())
                        .with_extension(Api(api_tag))
                        .header(CONTENT_LENGTH, HeaderValue::from(n_upload_bytes)) // must be set because of streaming
                        .body(Body::wrap_stream(upload_stream))
                        .send()
                },
                move |response| read_shard_upload_ndjson(response, progress_callback_for_closure.clone()),
            )
            .await;

        // V1 fallback synthesizes Transfer(full); undo any V2 body progress first.
        if result.is_err() {
            let already_transferred = transferred.load(Ordering::Relaxed);
            if already_transferred > 0
                && let Some(cb) = &progress_callback
            {
                cb(ShardUploadProgressType::DecrementTransfer(already_transferred));
            }
        }

        result?;

        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            size = n_upload_bytes,
            api_version = "v2",
            result = "sync performed",
            "Completed upload_shard API call",
        );

        Ok(())
    }

    #[cfg(not(target_family = "wasm"))]
    pub(crate) async fn upload_shard_with_version_override(
        &self,
        shard_data: Bytes,
        upload_permit: ConnectionPermit,
        forced_version: Option<u32>,
        progress_callback: Option<ShardUploadProgressCallback>,
    ) -> Result<()> {
        // Prefer V2; fall back to V1 on 404/501; persist detected version to
        // avoid repeated fallback attempts.
        let version = match forced_version {
            Some(v) => v,
            None => {
                let detected = self.detected_shard_api_version.load(Ordering::Relaxed);
                if detected != 0 { detected } else { 2 }
            },
        };

        match version {
            2 => match self
                .upload_shard_v2(shard_data.clone(), upload_permit, progress_callback.clone())
                .await
            {
                Ok(()) => {
                    if forced_version.is_none() {
                        self.detected_shard_api_version.store(2, Ordering::Relaxed);
                    }
                    Ok(())
                },
                Err(e)
                    if forced_version.is_none()
                        && matches!(e.status(), Some(StatusCode::NOT_FOUND) | Some(StatusCode::NOT_IMPLEMENTED)) =>
                {
                    info!(status = ?e.status(), "V2 shard upload not available, falling back to V1");
                    let fallback_permit = self.upload_concurrency_controller.acquire_connection_permit().await?;
                    self.upload_shard_v1(shard_data, fallback_permit, progress_callback).await?;
                    // Store after success to make sure we don't mess up on e.g. network failure.
                    self.detected_shard_api_version.store(1, Ordering::Relaxed);
                    Ok(())
                },
                Err(e) => Err(e),
            },
            1 => self.upload_shard_v1(shard_data, upload_permit, progress_callback).await,
            other => Err(ClientError::InternalError(anyhow!("unsupported shard upload API version: {other}"))),
        }
    }
}

/// A flush is triggered by the upload that brings the pending queue to this length, so the queue
/// stays bounded and a verdict never lags more than a few xorbs behind its PUT.
const FLUSH_AT: usize = 8;
/// The grants endpoint accepts at most this many commits per call.
const MAX_COMMITS_PER_CALL: usize = 64;

/// A xorb staged in the bucket by a presigned PUT and not yet committed. `bytes` is the
/// chunks-only serialization that was staged, kept so a `missing` verdict can be re-uploaded
/// through CAS.
struct PendingCommit {
    call_id: u64,
    hash: MerkleHash,
    grant_id: String,
    bytes: Bytes,
    n_bytes: u64,
    prefix: String,
}

/// Outcome of one direct-to-bucket xorb upload attempt.
enum DirectUploadOutcome {
    /// The staged bytes are in the bucket under this grant. The commit is deferred to the next
    /// flush, which happens before any shard that references the xorb is uploaded.
    Staged { grant_id: String },
    /// Direct upload did not go through for this xorb; upload it through CAS instead. Carries the
    /// upload permit back when the staging PUT did not consume it.
    Fallback {
        reason: String,
        permit: Option<ConnectionPermit>,
    },
}

/// Builds the progress reporter shared by the user callback and the adaptive concurrency
/// controller of `upload_permit`.
fn xorb_upload_reporter(
    n_upload_bytes: u64,
    upload_permit: &ConnectionPermit,
    progress_callback: Option<&ProgressCallback>,
) -> StreamProgressReporter {
    let mut upload_reporter = StreamProgressReporter::new(n_upload_bytes)
        .with_adaptive_concurrency_reporter(upload_permit.get_partial_completion_reporting_function());
    if let Some(cb) = progress_callback {
        upload_reporter = upload_reporter.with_progress_callback(cb.clone());
    }
    upload_reporter
}

/// Turns a grant into the URL and header map of the presigned PUT. Every header is part of the
/// SigV4 signature and is forwarded verbatim.
/// Turns a grant into the URL and header map of the presigned PUT. CAS is trusted, but a grant
/// must not turn the client into a relay for arbitrary requests: the URL must be https unless the
/// CAS endpoint itself is plain http (a local stack), and only the headers a presigned object
/// store PUT can sign are forwarded (`if-none-match` is how a grant onto the canonical key
/// forbids overwriting an existing object).
fn presigned_put_request(grant: &XorbGrant, allow_http: bool) -> std::result::Result<(Url, HeaderMap), String> {
    let url = Url::parse(&grant.url).map_err(|err| format!("invalid presigned url: {err}"))?;
    match url.scheme() {
        "https" => {},
        "http" if allow_http => {},
        scheme => return Err(format!("presigned url scheme {scheme:?} refused")),
    }
    let mut headers = HeaderMap::with_capacity(grant.headers.len());
    for (name, value) in &grant.headers {
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|err| format!("invalid presigned header {name:?}: {err}"))?;
        let lowered = header_name.as_str();
        if !(lowered.starts_with("x-amz-")
            || lowered == "content-length"
            || lowered == "content-type"
            || lowered == "if-none-match")
        {
            return Err(format!("presigned header {name:?} refused"));
        }
        let header_value = HeaderValue::from_str(value)
            .map_err(|err| format!("invalid value for presigned header {name:?}: {err}"))?;
        headers.insert(header_name, header_value);
    }
    Ok((url, headers))
}

impl RemoteClient {
    /// Stages the chunks-only serialized xorb (exactly the bytes `POST /v1/xorbs` would carry) in
    /// the bucket through a presigned PUT obtained from CAS. The commit, where CAS validates the
    /// staged bytes and writes the canonical object with its footer, is left to
    /// [`RemoteClient::flush_pending_commits`].
    ///
    /// Any failure before the PUT succeeds is reported as [`DirectUploadOutcome::Fallback`] so
    /// the caller uploads through CAS as before.
    async fn upload_xorb_direct(
        &self,
        call_id: u64,
        hash: MerkleHash,
        serialized_data: Bytes,
        upload_reporter: StreamProgressReporter,
        upload_permit: ConnectionPermit,
    ) -> DirectUploadOutcome {
        let n_upload_bytes = serialized_data.len() as u64;
        let phase_start = std::time::Instant::now();
        let url = match Url::parse(&format!("{}/v1/xorbs/grants", self.endpoint)) {
            Ok(url) => url,
            Err(err) => {
                return DirectUploadOutcome::Fallback {
                    reason: format!("invalid grants url: {err}"),
                    permit: Some(upload_permit),
                };
            },
        };

        // The grant binds the presigned PUT to these exact bytes.
        let sha256 = BASE64_STANDARD.encode(Sha256::digest(&serialized_data));
        let grant_request = XorbGrantRequest {
            grants: vec![XorbGrantItem {
                hash: hash.into(),
                size: n_upload_bytes,
                sha256,
            }],
            commits: vec![],
        };
        let grant_body = match serde_json::to_vec(&grant_request) {
            Ok(body) => Bytes::from(body),
            Err(err) => {
                return DirectUploadOutcome::Fallback {
                    reason: format!("could not encode grant request: {err}"),
                    permit: Some(upload_permit),
                };
            },
        };

        let client = self.authenticated_http_client.clone();
        let api_tag = "cas::xorb_grant";
        let grant_response: Result<XorbGrantResponse> = RetryWrapper::new(self.ctx.clone(), api_tag)
            .log_errors_as_info()
            .run_and_extract_json(move || {
                client
                    .post(url.clone())
                    .with_extension(Api(api_tag))
                    .header(CONTENT_TYPE, "application/json")
                    .body(grant_body.clone())
                    .send()
            })
            .await;
        let grant = match grant_response {
            Ok(response) => match response.grants.into_iter().find(|grant| MerkleHash::from(grant.hash) == hash) {
                Some(grant) => grant,
                None => {
                    return DirectUploadOutcome::Fallback {
                        reason: "grant response carried no grant for this xorb".to_string(),
                        permit: Some(upload_permit),
                    };
                },
            },
            Err(err) => {
                if err.status() == Some(StatusCode::NOT_FOUND) {
                    self.direct_upload_unavailable.store(true, Ordering::Relaxed);
                    return DirectUploadOutcome::Fallback {
                        reason: "no direct upload on this endpoint (404 on grants); direct path off for this session"
                            .to_string(),
                        permit: Some(upload_permit),
                    };
                }
                return DirectUploadOutcome::Fallback {
                    reason: format!("grant request failed: {err}"),
                    permit: Some(upload_permit),
                };
            },
        };
        // The presigned URL is a capability: debug level only.
        debug!(
            call_id,
            %hash,
            grant_id = %grant.grant_id,
            url = %grant.url,
            expires_in_secs = grant.expires_in_secs,
            "Obtained xorb staging grant"
        );

        // The PUT goes through the plain client: the CAS bearer token must not reach the bucket.
        let allow_http = self.endpoint.starts_with("http://");
        let (put_url, put_headers) = match presigned_put_request(&grant, allow_http) {
            Ok(request) => request,
            Err(reason) => {
                return DirectUploadOutcome::Fallback {
                    reason,
                    permit: Some(upload_permit),
                };
            },
        };
        let put_client = self.bucket_http_client.clone();
        let api_tag = "s3::put_staged_xorb";
        let body = serialized_data.clone();
        let grant_ms = phase_start.elapsed().as_millis() as u64;
        let put_start = std::time::Instant::now();
        let put_result = RetryWrapper::new(self.ctx.clone(), api_tag)
            .log_errors_as_info()
            .with_redacted_url()
            .with_connection_permit(upload_permit, Some(n_upload_bytes))
            .run(move || {
                put_client
                    .put(put_url.clone())
                    .headers(put_headers.clone())
                    .with_extension(Api(api_tag))
                    .body(body.clone())
                    .send()
            })
            .await;
        if let Err(err) = put_result {
            // A reqwest error prints its URL, and this one carries the signature: strip the query
            // before the reason reaches the fallback warning.
            let err = match err {
                ClientError::ReqwestMiddlewareError(reqwest_middleware::Error::Reqwest(err)) => ClientError::from(err),
                other => other,
            };
            return DirectUploadOutcome::Fallback {
                reason: format!("staging PUT failed: {err}"),
                permit: None,
            };
        }

        // The bytes are in the bucket: report them now. A `missing` verdict at flush streams them
        // again through CAS without a progress callback, so they are counted once.
        upload_reporter.report_progress(n_upload_bytes as usize);
        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            %hash,
            size = n_upload_bytes,
            grant_ms,
            put_ms = put_start.elapsed().as_millis() as u64,
            "Direct xorb upload phases",
        );
        DirectUploadOutcome::Staged {
            grant_id: grant.grant_id,
        }
    }

    /// Uploads the serialized xorb through `POST /v1/xorbs/{prefix}/{hash}`; CAS validates it and
    /// writes it to the bucket. A footer in the body is ignored and regenerated server side.
    async fn upload_xorb_via_cas(
        &self,
        call_id: u64,
        prefix: &str,
        hash: MerkleHash,
        serialized_data: Bytes,
        progress_callback: Option<ProgressCallback>,
        upload_permit: ConnectionPermit,
    ) -> Result<u64> {
        let key = Key {
            prefix: prefix.to_string(),
            hash,
        };
        let url = Url::parse(&format!("{}/v1/xorbs/{key}", self.endpoint))?;
        let n_upload_bytes = serialized_data.len() as u64;

        #[cfg(not(target_family = "wasm"))]
        let block_size = self.ctx.config.client.upload_reporting_block_size;

        let upload_reporter = xorb_upload_reporter(n_upload_bytes, &upload_permit, progress_callback.as_ref());

        let xorb_uploaded = {
            if !self.dry_run {
                let client = self.authenticated_http_client.clone();

                let api_tag = "cas::upload_xorb";

                let response: UploadXorbResponse = RetryWrapper::new(self.ctx.clone(), api_tag)
                    .with_connection_permit(upload_permit, Some(n_upload_bytes))
                    .run_and_extract_json(move || {
                        let url = url.clone();
                        let serialized_data = serialized_data.clone();

                        let request = {
                            #[cfg(not(target_family = "wasm"))]
                            {
                                let upload_stream = UploadProgressStream::wrap_bytes_as_stream(
                                    serialized_data,
                                    block_size,
                                    upload_reporter.clone(),
                                );
                                client
                                    .post(url)
                                    .with_extension(Api(api_tag))
                                    .header(CONTENT_LENGTH, HeaderValue::from(n_upload_bytes)) // must be set because of streaming
                                    .body(Body::wrap_stream(upload_stream))
                            }

                            // reqwest's wasm backend does not support streaming request bodies;
                            // pass the raw Bytes directly (CONTENT_LENGTH is set by reqwest from the body length).
                            #[cfg(target_family = "wasm")]
                            {
                                client.post(url).with_extension(Api(api_tag)).body(serialized_data)
                            }
                        };

                        request.send()
                    })
                    .await?;

                // Wasm has no per-chunk progress hook (no streaming body); emit one bulk
                // event after success so the user callback and adaptive-concurrency
                // reporter both observe the full byte count.
                #[cfg(target_family = "wasm")]
                upload_reporter.report_progress(n_upload_bytes as usize);

                response.was_inserted
            } else {
                true
            }
        };

        log_upload_xorb_completed(call_id, prefix, hash, n_upload_bytes, xorb_uploaded, "cas");

        Ok(n_upload_bytes)
    }
}

impl RemoteClient {
    /// The lock only guards a single push or drain, so a poisoned lock (a task panicked while
    /// holding it) still protects a consistent queue and is recovered rather than propagated.
    fn pending_commits(&self) -> MutexGuard<'_, Vec<PendingCommit>> {
        self.pending_commits.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Queues the commit of a staged xorb; returns the queue length after the push.
    fn enqueue_pending_commit(&self, pending: PendingCommit) -> usize {
        let mut queue = self.pending_commits();
        queue.push(pending);
        queue.len()
    }

    /// Asks CAS for a verdict on every staged xorb whose commit is pending, in batches of at most
    /// [`MAX_COMMITS_PER_CALL`]. `inserted` and `exists` complete the upload; `missing`, or no
    /// verdict at all, re-uploads the xorb through CAS; `rejected` is an error, returned once the
    /// other verdicts of the batch are processed. A commit call that fails before delivering
    /// verdicts falls back to CAS for the whole batch.
    ///
    /// [`Client::upload_shard`] calls this first, so a shard is only registered once every xorb it
    /// references has a verdict.
    pub async fn flush_pending_commits(&self) -> Result<()> {
        let mut first_error = None;
        loop {
            let batch: Vec<PendingCommit> = {
                let mut queue = self.pending_commits();
                let n = queue.len().min(MAX_COMMITS_PER_CALL);
                queue.drain(..n).collect()
            };
            if batch.is_empty() {
                break;
            }
            if let Err(err) = self.commit_batch(batch).await
                && first_error.is_none()
            {
                first_error = Some(err);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn commit_batch(&self, batch: Vec<PendingCommit>) -> Result<()> {
        let flush_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let batch_size = batch.len();
        let commit_start = std::time::Instant::now();
        let response = self.post_commits(&batch).await;
        event!(
            INFORMATION_LOG_LEVEL,
            call_id = flush_id,
            batch_size,
            commit_ms = commit_start.elapsed().as_millis() as u64,
            ok = response.is_ok(),
            "Direct xorb commit flush",
        );
        let verdicts = match response {
            Ok(response) => response.commits,
            Err(err) => {
                let reason = format!("commit request failed: {err}");
                for pending in batch {
                    self.reupload_through_cas(pending, &reason).await?;
                }
                return Ok(());
            },
        };

        let mut rejected = None;
        for pending in batch {
            let verdict = verdicts
                .iter()
                .find(|verdict| MerkleHash::from(verdict.hash) == pending.hash && verdict.grant_id == pending.grant_id);
            match verdict {
                Some(XorbCommitResult {
                    status: XorbCommitStatus::Inserted,
                    ..
                }) => log_upload_xorb_completed(
                    pending.call_id,
                    &pending.prefix,
                    pending.hash,
                    pending.n_bytes,
                    true,
                    "direct",
                ),
                Some(XorbCommitResult {
                    status: XorbCommitStatus::Exists,
                    ..
                }) => log_upload_xorb_completed(
                    pending.call_id,
                    &pending.prefix,
                    pending.hash,
                    pending.n_bytes,
                    false,
                    "direct",
                ),
                Some(XorbCommitResult {
                    status: XorbCommitStatus::Rejected,
                    error,
                    ..
                }) => {
                    let error = error.clone().unwrap_or_else(|| "no reason given".to_string());
                    warn!(call_id = pending.call_id, hash = %pending.hash, error, "Direct xorb upload rejected at commit");
                    if rejected.is_none() {
                        rejected = Some(format!("xorb {} rejected at commit: {error}", pending.hash.hex()));
                    }
                },
                Some(XorbCommitResult {
                    status: XorbCommitStatus::Missing,
                    ..
                }) => {
                    let reason = format!("no staged object for grant {} at commit", pending.grant_id);
                    self.reupload_through_cas(pending, &reason).await?;
                },
                None => {
                    self.reupload_through_cas(pending, "commit response carried no verdict for this grant")
                        .await?;
                },
            }
        }
        rejected.map_or(Ok(()), |message| Err(ClientError::Other(message)))
    }

    async fn post_commits(&self, batch: &[PendingCommit]) -> Result<XorbGrantResponse> {
        let url = Url::parse(&format!("{}/v1/xorbs/grants", self.endpoint))?;
        let request = XorbGrantRequest {
            grants: vec![],
            commits: batch
                .iter()
                .map(|pending| XorbCommitItem {
                    hash: pending.hash.into(),
                    grant_id: pending.grant_id.clone(),
                })
                .collect(),
        };
        let body = serde_json::to_vec(&request)
            .map(Bytes::from)
            .map_err(|err| ClientError::Other(format!("could not encode commit request: {err}")))?;
        let client = self.authenticated_http_client.clone();
        let api_tag = "cas::xorb_commit";
        RetryWrapper::new(self.ctx.clone(), api_tag)
            .log_errors_as_info()
            .run_and_extract_json(move || {
                client
                    .post(url.clone())
                    .with_extension(Api(api_tag))
                    .header(CONTENT_TYPE, "application/json")
                    .body(body.clone())
                    .send()
            })
            .await
    }

    /// Uploads a staged xorb through CAS when the direct path delivered no verdict for it. Its
    /// bytes were reported when the PUT succeeded, so there is no progress callback here.
    async fn reupload_through_cas(&self, pending: PendingCommit, reason: &str) -> Result<()> {
        warn!(
            call_id = pending.call_id,
            prefix = %pending.prefix,
            hash = %pending.hash,
            reason,
            "Direct xorb upload unavailable; uploading through CAS"
        );
        let permit = self.acquire_upload_permit().await?;
        self.upload_xorb_via_cas(pending.call_id, &pending.prefix, pending.hash, pending.bytes, None, permit)
            .await?;
        Ok(())
    }
}

fn log_upload_xorb_completed(call_id: u64, prefix: &str, hash: MerkleHash, size: u64, inserted: bool, path: &str) {
    if !inserted {
        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            prefix,
            %hash,
            path,
            result = "not_inserted",
            "Completed upload_xorb API call",
        );
    } else {
        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            prefix,
            %hash,
            size,
            path,
            result = "inserted",
            "Completed upload_xorb API call",
        );
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl Client for RemoteClient {
    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponseV2>> {
        let forced_version = self.ctx.config.client.reconstruction_api_version;
        self.get_reconstruction_with_version_override(file_id, bytes_range, forced_version)
            .await
    }

    async fn batch_get_reconstruction(&self, file_ids: &[MerkleHash]) -> Result<BatchQueryReconstructionResponse> {
        let mut url_str = format!("{}/v1/reconstructions?", self.endpoint);
        let mut is_first = true;
        let mut file_id_list = Vec::new();
        for hash in file_ids {
            file_id_list.push(hash.hex());
            if is_first {
                is_first = false;
            } else {
                url_str.push('&');
            }
            url_str.push_str("file_id=");
            url_str.push_str(hash.hex().as_str());
        }
        let url: Url = url_str.parse()?;

        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        info!(call_id, file_ids=?file_id_list, "Starting batch_get_reconstruction API call");

        let api_tag = "cas::batch_get_reconstruction";
        let client = self.authenticated_http_client.clone();

        let response: BatchQueryReconstructionResponse = RetryWrapper::new(self.ctx.clone(), api_tag)
            .run_and_extract_json(move || client.get(url.clone()).with_extension(Api(api_tag)).send())
            .await?;

        info!(call_id,
            file_ids=?file_id_list,
            response_count=response.files.len(),
            "Completed batch_get_reconstruction API call",
        );

        Ok(response)
    }

    async fn acquire_download_permit(&self) -> Result<ConnectionPermit> {
        let permit = self.download_concurrency_controller.acquire_connection_permit().await;
        #[cfg(not(target_family = "wasm"))]
        if let Some(telemetry) = &self.telemetry {
            telemetry.record_concurrency(self.download_concurrency_controller.active_permits());
        }
        permit
    }

    async fn get_file_term_data(
        &self,
        url_info: Box<dyn URLProvider>,
        download_permit: ConnectionPermit,
        progress_callback: Option<ProgressCallback>,
        uncompressed_size_if_known: Option<usize>,
    ) -> Result<(Bytes, Vec<u32>)> {
        let api_tag = "s3::get_range";
        let http_client = self.http_client.clone();
        let url_info = Arc::new(url_info);

        let (_, url_ranges) = url_info.retrieve_url().await?;
        let total_download_bytes: u64 = url_ranges.iter().map(|r| r.length()).sum();

        let mut transfer_reporter = StreamProgressReporter::new(total_download_bytes)
            .with_adaptive_concurrency_reporter(download_permit.get_partial_completion_reporting_function());
        if let Some(cb) = progress_callback {
            transfer_reporter = transfer_reporter.with_progress_callback(cb);
        }

        let result = RetryWrapper::new(self.ctx.clone(), api_tag)
            .with_retry_on_403()
            .with_connection_permit(download_permit, None)
            .run_and_process(
                move || {
                    let http_client = http_client.clone();
                    let url_info = url_info.clone();

                    async move {
                        let (url_string, url_ranges) = url_info
                            .retrieve_url()
                            .await
                            .map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?;
                        let url =
                            Url::parse(&url_string).map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?;

                        // RFC 7233 §2.1: single-range uses "bytes=S-E", multi-range uses "bytes=S1-E1,S2-E2,..."
                        let range_header_value = if url_ranges.len() == 1 {
                            url_ranges[0].range_header()
                        } else {
                            let joined = url_ranges
                                .iter()
                                .map(|r| format!("{}-{}", r.start, r.end))
                                .collect::<Vec<_>>()
                                .join(",");
                            format!("bytes={joined}")
                        };

                        let response = http_client
                            .get(url)
                            .header(RANGE, range_header_value)
                            .with_extension(Api(api_tag))
                            .send()
                            .await?;

                        if response.status() == reqwest::StatusCode::FORBIDDEN {
                            url_info
                                .refresh_url()
                                .await
                                .map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?;
                        }

                        Ok(response)
                    }
                },
                move |resp: Response| {
                    let transfer_reporter = transfer_reporter.clone();
                    async move {
                        let content_type = resp
                            .headers()
                            .get("content-type")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("")
                            .to_string();

                        let is_multipart = content_type.contains("multipart/byteranges");

                        if is_multipart {
                            let body = resp
                                .bytes()
                                .await
                                .map_err(|e| RetryableReqwestError::RetryableError(ClientError::from(e)))?;

                            let multipart_parts = crate::cas_client::multipart::parse_multipart_byteranges(&content_type, body)
                                .map_err(RetryableReqwestError::FatalError)?;

                            let mut all_decompressed = Vec::with_capacity(uncompressed_size_if_known.unwrap_or(0));
                            let mut all_chunk_indices = Vec::<u32>::new();
                            let mut total_compressed_bytes = 0u64;

                            for part in multipart_parts {
                                total_compressed_bytes += part.data.len() as u64;

                                let (data, chunk_indices) =
                                    xet_core_structures::xorb_object::deserialize_chunks(&mut std::io::Cursor::new(part.data.as_ref()))
                                        .map_err(|e| {
                                            RetryableReqwestError::RetryableError(ClientError::FormatError(e))
                                        })?;

                                xet_core_structures::xorb_object::append_chunk_segment(
                                    &mut all_decompressed,
                                    &mut all_chunk_indices,
                                    &data,
                                    &chunk_indices,
                                );

                                transfer_reporter.report_progress(total_compressed_bytes as usize);
                            }

                            if let Some(expected) = uncompressed_size_if_known
                                && expected != all_decompressed.len()
                            {
                                return Err(RetryableReqwestError::RetryableError(ClientError::Other(format!(
                                    "get_file_term_data: expected {expected} uncompressed bytes, got {}",
                                    all_decompressed.len()
                                ))));
                            }
                            Ok((Bytes::from(all_decompressed), all_chunk_indices))
                        } else {
                            let incoming_stream = DownloadProgressStream::wrap_stream(
                                resp.bytes_stream().map_err(std::io::Error::other),
                                transfer_reporter,
                            );

                            let capacity = uncompressed_size_if_known.unwrap_or(0);
                            let mut buffer = Vec::with_capacity(capacity);
                            let mut writer = std::io::Cursor::new(&mut buffer);

                            let result = xet_core_structures::xorb_object::deserialize_async::deserialize_chunks_to_writer_from_stream(
                                incoming_stream,
                                &mut writer,
                            )
                            .await;

                            match result {
                                Ok((_compressed_len, chunk_byte_indices)) => {
                                    if let Some(expected) = uncompressed_size_if_known
                                        && expected != buffer.len()
                                    {
                                        return Err(RetryableReqwestError::RetryableError(ClientError::Other(format!(
                                            "get_file_term_data: expected {expected} uncompressed bytes, got {}",
                                            buffer.len()
                                        ))));
                                    }
                                    Ok((Bytes::from(buffer), chunk_byte_indices))
                                },
                                Err(e) => Err(RetryableReqwestError::RetryableError(ClientError::FormatError(e))),
                            }
                        }
                    }
                },
            )
            .await?;

        Ok(result)
    }

    #[instrument(skip_all, name = "RemoteClient::get_file_reconstruction", fields(file.hash = file_hash.hex()
    ))]
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let url = Url::parse(&format!("{}/v1/reconstructions/{}", self.endpoint, file_hash.hex()))?;
        event!(INFORMATION_LOG_LEVEL, call_id, %file_hash, "Starting get_file_reconstruction_info API call");

        let api_tag = "cas::get_reconstruction_info";
        let client = self.authenticated_http_client.clone();

        let response: QueryReconstructionResponse = RetryWrapper::new(self.ctx.clone(), api_tag)
            .run_and_extract_json(move || client.get(url.clone()).with_extension(Api(api_tag)).send())
            .await?;

        let terms_count = response.terms.len();
        let result = Some((
            MDBFileInfo {
                metadata: FileDataSequenceHeader::new(*file_hash, terms_count, false, false),
                segments: response
                    .terms
                    .into_iter()
                    .map(|ce| {
                        FileDataSequenceEntry::new(ce.hash.into(), ce.unpacked_length, ce.range.start, ce.range.end)
                    })
                    .collect(),
                verification: vec![],
                metadata_ext: None,
            },
            None,
        ));

        event!(INFORMATION_LOG_LEVEL, call_id, %file_hash, terms_count, "Completed get_file_reconstruction_info API call");

        Ok(result)
    }

    async fn query_for_global_dedup_shard(&self, prefix: &str, chunk_hash: &MerkleHash) -> Result<Option<Bytes>> {
        let Some(response) = self.query_dedup_api(prefix, chunk_hash).await? else {
            return Ok(None);
        };

        Ok(Some(response.bytes().await?))
    }

    async fn acquire_upload_permit(&self) -> Result<ConnectionPermit> {
        let permit = self.upload_concurrency_controller.acquire_connection_permit().await;
        #[cfg(not(target_family = "wasm"))]
        if let Some(telemetry) = &self.telemetry {
            telemetry.record_concurrency(self.upload_concurrency_controller.active_permits());
        }
        permit
    }

    #[cfg(not(target_family = "wasm"))]
    fn transfer_telemetry(&self) -> Option<Arc<TransferTelemetry>> {
        self.telemetry.clone()
    }

    #[instrument(skip_all, name = "RemoteClient::upload_shard", fields(shard.len = shard_data.len()))]
    async fn upload_shard(
        &self,
        shard_data: Bytes,
        upload_permit: ConnectionPermit,
        progress_callback: Option<ShardUploadProgressCallback>,
    ) -> Result<()> {
        if self.dry_run {
            return Ok(());
        }

        // Every xorb the shard references needs a commit verdict before the shard is registered.
        self.flush_pending_commits().await?;

        #[cfg(target_family = "wasm")]
        {
            self.upload_shard_v1(shard_data, upload_permit, progress_callback).await
        }

        #[cfg(not(target_family = "wasm"))]
        {
            let forced_version = self.ctx.config.client.shard_api_version;
            self.upload_shard_with_version_override(shard_data, upload_permit, forced_version, progress_callback)
                .await
        }
    }

    #[instrument(skip_all, name = "RemoteClient::upload_xorb", fields(key = Key{prefix : prefix.to_string(), hash : serialized_xorb_object.hash}.to_string(),
                 xorb.len = serialized_xorb_object.serialized_data.len(), xorb.num_chunks = serialized_xorb_object.num_chunks
    ))]
    async fn upload_xorb(
        &self,
        prefix: &str,
        mut serialized_xorb_object: SerializedXorbObject,
        progress_callback: Option<ProgressCallback>,
        upload_permit: ConnectionPermit,
    ) -> Result<u64> {
        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let hash = serialized_xorb_object.hash;
        let n_upload_bytes = serialized_xorb_object.serialized_data.len() as u64;
        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            prefix,
            %hash,
            size=n_upload_bytes,
            num_chunks=serialized_xorb_object.num_chunks,
            "Starting upload_xorb API call",
        );

        let serialized_data = Bytes::from(std::mem::take(&mut serialized_xorb_object.serialized_data));

        let direct_upload = self.ctx.config.xorb.direct_upload
            && !self.dry_run
            && !self.direct_upload_unavailable.load(Ordering::Relaxed);

        let upload_permit = if direct_upload {
            let upload_reporter = xorb_upload_reporter(n_upload_bytes, &upload_permit, progress_callback.as_ref());
            match self
                .upload_xorb_direct(call_id, hash, serialized_data.clone(), upload_reporter, upload_permit)
                .await
            {
                DirectUploadOutcome::Staged { grant_id } => {
                    let queued = self.enqueue_pending_commit(PendingCommit {
                        call_id,
                        hash,
                        grant_id,
                        bytes: serialized_data,
                        n_bytes: n_upload_bytes,
                        prefix: prefix.to_string(),
                    });
                    if queued >= FLUSH_AT {
                        self.flush_pending_commits().await?;
                    }
                    return Ok(n_upload_bytes);
                },
                DirectUploadOutcome::Fallback { reason, permit } => {
                    warn!(call_id, prefix, %hash, reason, "Direct xorb upload unavailable; uploading through CAS");
                    match permit {
                        Some(permit) => permit,
                        None => self.acquire_upload_permit().await?,
                    }
                },
            }
        } else {
            upload_permit
        };

        self.upload_xorb_via_cas(call_id, prefix, hash, serialized_data, progress_callback, upload_permit)
            .await
    }

    #[instrument(skip_all, name = "RemoteClient::get_file_chunk_hashes", fields(file.hash = file_id.hex(), n_ranges = dirty_ranges.len()))]
    async fn get_file_chunk_hashes(
        &self,
        file_id: &MerkleHash,
        dirty_ranges: Vec<FileRange>,
    ) -> Result<FileChunkHashesResponse> {
        if dirty_ranges.is_empty() {
            return Err(ClientError::Other("get_file_chunk_hashes requires at least one dirty range".into()));
        }

        let url = Url::parse(&format!("{}/v2/file-chunk-hashes/{}", self.endpoint, file_id.hex()))?;

        // Multi-range `bytes=A-B,C-D` value. `HttpRange` is inclusive-end and `Display`s as
        // `start-end`; conversion from `FileRange` does the +1/-1 for us.
        let header_value = HeaderValue::from_str(&format!(
            "bytes={}",
            dirty_ranges
                .iter()
                .copied()
                .map(HttpRange::from)
                .map(|r| r.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ))
        .map_err(|err| ClientError::Other(format!("invalid X-Range-Dirty header value: {err}")))?;

        let api_tag = "cas::get_file_chunk_hashes";
        let client = self.authenticated_http_client.clone();

        let response: FileChunkHashesResponse = RetryWrapper::new(self.ctx.clone(), api_tag)
            .run_and_extract_json(move || {
                client
                    .get(url.clone())
                    .header(X_RANGE_DIRTY_HEADER, header_value.clone())
                    .with_extension(Api(api_tag))
                    .send()
            })
            .await?;

        Ok(response)
    }
}

#[cfg(test)]
#[cfg(not(target_family = "wasm"))]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use tracing_test::traced_test;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Match, Mock, MockServer, Request, Respond, ResponseTemplate};
    use xet_core_structures::xorb_object::CompressionScheme;
    use xet_core_structures::xorb_object::xorb_format_test_utils::{
        ChunkSize, build_and_verify_xorb_object, build_raw_xorb,
    };
    use xet_runtime::config::XetConfig;

    use super::*;

    const GRANT_ID: &str = "0123456789abcdef0123456789abcdef";
    const TOKEN: &str = "write-token";

    fn direct_upload_ctx() -> XetContext {
        let mut config = XetConfig::default();
        config.xorb.direct_upload = true;
        config.telemetry.enabled = false;
        config.client.retry_max_attempts = 1;
        config.client.retry_base_delay = Duration::from_millis(10);
        // The shard goes to `/v1/shards` directly: the barrier under test does not depend on the
        // shard API version.
        config.client.shard_api_version = Some(1);
        XetContext::with_config(config).unwrap()
    }

    fn direct_upload_client(ctx: &XetContext, server: &MockServer) -> Arc<RemoteClient> {
        let auth = AuthConfig::maybe_new(Some(TOKEN.to_string()), None, None);
        RemoteClient::new(ctx.clone(), &server.uri(), &auth, "test-session", false, None)
    }

    /// The chunks-only serialization the upload session produces; the footer is written by CAS.
    fn chunks_only_xorb(num_chunks: u32) -> SerializedXorbObject {
        let cfg = XetConfig::default();
        let xorb_obj = SerializedXorbObject::from_xorb(
            build_raw_xorb(num_chunks, ChunkSize::Fixed(1024)),
            false,
            cfg.xorb.compression_policy.as_str(),
            cfg.xorb.compression_scheme_retest_interval,
        )
        .unwrap();
        assert!(xorb_obj.footer_start.is_none());
        xorb_obj
    }

    /// `count` distinct xorbs (random chunk data, so distinct hashes).
    fn chunks_only_xorbs(count: u32) -> Vec<SerializedXorbObject> {
        (0..count).map(|index| chunks_only_xorb(3 + index)).collect()
    }

    fn staged_path(hash: &MerkleHash) -> String {
        format!("/cas-staging/staging/{}/{GRANT_ID}", hash.hex())
    }

    /// The grant request the client must send for this xorb: one grant bound to the exact bytes,
    /// and no commits.
    fn grant_request(xorb_obj: &SerializedXorbObject) -> XorbGrantRequest {
        XorbGrantRequest {
            grants: vec![XorbGrantItem {
                hash: xorb_obj.hash.into(),
                size: xorb_obj.serialized_data.len() as u64,
                sha256: BASE64_STANDARD.encode(Sha256::digest(&xorb_obj.serialized_data)),
            }],
            commits: vec![],
        }
    }

    fn grant_response(server: &MockServer, hash: MerkleHash) -> XorbGrantResponse {
        XorbGrantResponse {
            grants: vec![XorbGrant {
                hash: hash.into(),
                grant_id: GRANT_ID.to_string(),
                url: format!("{}{}?X-Amz-Signature=test", server.uri(), staged_path(&hash)),
                headers: HashMap::from([
                    ("x-amz-checksum-sha256".to_string(), "checksum".to_string()),
                    // Mixed case on purpose: header names are forwarded case-insensitively.
                    ("X-Amz-Sdk-Checksum-Algorithm".to_string(), "SHA256".to_string()),
                ]),
                expires_in_secs: 900,
            }],
            commits: vec![],
        }
    }

    /// The bearer token is for CAS only; the presigned PUT must not carry it.
    struct NoAuthorizationHeader;

    impl Match for NoAuthorizationHeader {
        fn matches(&self, request: &Request) -> bool {
            !request.headers.contains_key("authorization")
        }
    }

    /// A commit call of exactly `size` items and no grants.
    struct CommitBatch(usize);

    impl Match for CommitBatch {
        fn matches(&self, request: &Request) -> bool {
            serde_json::from_slice::<XorbGrantRequest>(&request.body)
                .is_ok_and(|body| body.grants.is_empty() && body.commits.len() == self.0)
        }
    }

    /// The order in which the commit and shard requests reached the server.
    #[derive(Clone, Default)]
    struct RequestLog(Arc<Mutex<Vec<String>>>);

    impl RequestLog {
        fn record(&self, entry: String) {
            self.0.lock().unwrap().push(entry);
        }

        fn entries(&self) -> Vec<String> {
            self.0.lock().unwrap().clone()
        }
    }

    type Verdicts = HashMap<MerkleHash, (XorbCommitStatus, Option<&'static str>)>;

    /// Answers a commit call with one verdict per item: `Exists` unless `verdicts` says otherwise.
    struct CommitVerdicts {
        verdicts: Verdicts,
        log: RequestLog,
    }

    impl Respond for CommitVerdicts {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let body: XorbGrantRequest = serde_json::from_slice(&request.body).unwrap();
            self.log.record(format!("commit:{}", body.commits.len()));
            let commits = body
                .commits
                .into_iter()
                .map(|item| {
                    let (status, error) = self
                        .verdicts
                        .get(&MerkleHash::from(item.hash))
                        .copied()
                        .unwrap_or((XorbCommitStatus::Exists, None));
                    XorbCommitResult {
                        hash: item.hash,
                        grant_id: item.grant_id,
                        status,
                        error: error.map(str::to_string),
                    }
                })
                .collect();
            ResponseTemplate::new(200).set_body_json(XorbGrantResponse {
                grants: vec![],
                commits,
            })
        }
    }

    struct ShardAccepted(RequestLog);

    impl Respond for ShardAccepted {
        fn respond(&self, _request: &Request) -> ResponseTemplate {
            self.0.record("shard".to_string());
            ResponseTemplate::new(200).set_body_json(UploadShardResponse {
                result: UploadShardResponseType::SyncPerformed,
            })
        }
    }

    /// The grant call for one xorb, then the presigned PUT of its exact bytes.
    async fn mount_staged(server: &MockServer, xorb_obj: &SerializedXorbObject) {
        let hash = xorb_obj.hash;
        Mock::given(method("POST"))
            .and(path("/v1/xorbs/grants"))
            .and(header("authorization", format!("Bearer {TOKEN}").as_str()))
            .and(header("content-type", "application/json"))
            .and(body_json(grant_request(xorb_obj)))
            .respond_with(ResponseTemplate::new(200).set_body_json(grant_response(server, hash)))
            .expect(1)
            .mount(server)
            .await;
        Mock::given(method("PUT"))
            .and(path(staged_path(&hash)))
            .and(header("x-amz-checksum-sha256", "checksum"))
            .and(header("x-amz-sdk-checksum-algorithm", "SHA256"))
            .and(NoAuthorizationHeader)
            .and(wiremock::matchers::body_bytes(xorb_obj.serialized_data.clone()))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(server)
            .await;
    }

    /// Exactly one commit call of `size` items, answered from `verdicts`.
    async fn mount_commit(server: &MockServer, log: &RequestLog, size: usize, verdicts: Verdicts) {
        Mock::given(method("POST"))
            .and(path("/v1/xorbs/grants"))
            .and(header("authorization", format!("Bearer {TOKEN}").as_str()))
            .and(CommitBatch(size))
            .respond_with(CommitVerdicts {
                verdicts,
                log: log.clone(),
            })
            .expect(1)
            .mount(server)
            .await;
    }

    async fn mount_cas_upload(server: &MockServer, hash: MerkleHash, expected_calls: u64) {
        Mock::given(method("POST"))
            .and(path(format!("/v1/xorbs/default/{}", hash.hex())))
            .and(header("authorization", format!("Bearer {TOKEN}").as_str()))
            .respond_with(ResponseTemplate::new(200).set_body_json(UploadXorbResponse { was_inserted: true }))
            .expect(expected_calls)
            .mount(server)
            .await;
    }

    async fn mount_shard_upload(server: &MockServer, log: &RequestLog, expected_calls: u64) {
        Mock::given(method("POST"))
            .and(path("/v1/shards"))
            .and(header("authorization", format!("Bearer {TOKEN}").as_str()))
            .respond_with(ShardAccepted(log.clone()))
            .expect(expected_calls)
            .mount(server)
            .await;
    }

    /// Uploads one xorb; `reported` accumulates what the progress callback sees, across the
    /// upload and any later flush.
    async fn upload(client: &RemoteClient, xorb_obj: SerializedXorbObject, reported: &Arc<AtomicU64>) -> Result<u64> {
        let reported = reported.clone();
        let progress: ProgressCallback = Arc::new(move |delta, _, _| {
            reported.fetch_add(delta, Ordering::Relaxed);
        });
        let permit = client.acquire_upload_permit().await.unwrap();
        client.upload_xorb(PREFIX_DEFAULT, xorb_obj, Some(progress), permit).await
    }

    async fn upload_all(client: &RemoteClient, xorbs: Vec<SerializedXorbObject>) -> u64 {
        let reported = Arc::new(AtomicU64::new(0));
        let mut total = 0;
        for xorb_obj in xorbs {
            let n_bytes = xorb_obj.serialized_data.len() as u64;
            assert_eq!(upload(client, xorb_obj, &reported).await.unwrap(), n_bytes);
            total += n_bytes;
        }
        assert_eq!(reported.load(Ordering::Relaxed), total, "the progress callback sees every PUT in full");
        total
    }

    async fn upload_shard(client: &RemoteClient) -> Result<()> {
        let permit = client.acquire_upload_permit().await.unwrap();
        client.upload_shard(Bytes::from_static(b"shard"), permit, None).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_direct_upload_defers_the_commits_to_the_shard_upload() {
        let server = MockServer::start().await;
        let log = RequestLog::default();
        let xorbs = chunks_only_xorbs(3);
        for xorb_obj in &xorbs {
            mount_staged(&server, xorb_obj).await;
            mount_cas_upload(&server, xorb_obj.hash, 0).await;
        }
        mount_commit(&server, &log, 3, Verdicts::new()).await;
        mount_shard_upload(&server, &log, 1).await;

        let ctx = direct_upload_ctx();
        let client = direct_upload_client(&ctx, &server);
        upload_all(&client, xorbs).await;
        assert!(log.entries().is_empty(), "no commit before the shard upload: {:?}", log.entries());

        upload_shard(&client).await.unwrap();
        assert_eq!(log.entries(), ["commit:3", "shard"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_direct_upload_flushes_a_full_batch_during_the_uploads() {
        let server = MockServer::start().await;
        let log = RequestLog::default();
        let xorbs = chunks_only_xorbs(FLUSH_AT as u32 + 1);
        for xorb_obj in &xorbs {
            mount_staged(&server, xorb_obj).await;
            mount_cas_upload(&server, xorb_obj.hash, 0).await;
        }
        mount_commit(&server, &log, FLUSH_AT, Verdicts::new()).await;
        mount_commit(&server, &log, 1, Verdicts::new()).await;
        mount_shard_upload(&server, &log, 1).await;

        let ctx = direct_upload_ctx();
        let client = direct_upload_client(&ctx, &server);
        upload_all(&client, xorbs).await;
        assert_eq!(log.entries(), ["commit:8"], "the eighth xorb flushes the batch; the ninth waits");

        upload_shard(&client).await.unwrap();
        assert_eq!(log.entries(), ["commit:8", "commit:1", "shard"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_direct_upload_missing_at_commit_is_resent_through_cas() {
        let server = MockServer::start().await;
        let log = RequestLog::default();
        let xorb_obj = chunks_only_xorb(3);
        let (hash, n_bytes) = (xorb_obj.hash, xorb_obj.serialized_data.len() as u64);

        mount_staged(&server, &xorb_obj).await;
        mount_commit(&server, &log, 1, Verdicts::from([(hash, (XorbCommitStatus::Missing, None))])).await;
        // The PUT consumed the upload permit; the fallback acquires a fresh one and goes through.
        mount_cas_upload(&server, hash, 1).await;
        mount_shard_upload(&server, &log, 1).await;

        let ctx = direct_upload_ctx();
        let client = direct_upload_client(&ctx, &server);
        let reported = Arc::new(AtomicU64::new(0));
        assert_eq!(upload(&client, xorb_obj, &reported).await.unwrap(), n_bytes);

        upload_shard(&client).await.unwrap();
        assert_eq!(log.entries(), ["commit:1", "shard"]);
        assert_eq!(
            reported.load(Ordering::Relaxed),
            n_bytes,
            "the bytes staged and then re-sent through CAS are reported once"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_direct_upload_rejected_at_commit_fails_the_shard_upload() {
        let server = MockServer::start().await;
        let log = RequestLog::default();
        let xorb_obj = chunks_only_xorb(3);
        let (hash, n_bytes) = (xorb_obj.hash, xorb_obj.serialized_data.len() as u64);

        mount_staged(&server, &xorb_obj).await;
        mount_commit(
            &server,
            &log,
            1,
            Verdicts::from([(hash, (XorbCommitStatus::Rejected, Some("xorb hash mismatch")))]),
        )
        .await;
        // The regular upload would refuse the bytes too: no fallback, and no shard.
        mount_cas_upload(&server, hash, 0).await;
        mount_shard_upload(&server, &log, 0).await;

        let ctx = direct_upload_ctx();
        let client = direct_upload_client(&ctx, &server);
        let reported = Arc::new(AtomicU64::new(0));
        assert_eq!(upload(&client, xorb_obj, &reported).await.unwrap(), n_bytes);

        let message = upload_shard(&client).await.unwrap_err().to_string();
        assert!(message.contains("rejected"), "{message}");
        assert!(message.contains("xorb hash mismatch"), "{message}");
        assert!(message.contains(&hash.hex()), "{message}");
        assert_eq!(log.entries(), ["commit:1"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_direct_upload_commit_failure_resends_the_batch_through_cas() {
        let server = MockServer::start().await;
        let log = RequestLog::default();
        let ctx = direct_upload_ctx();
        let xorbs = chunks_only_xorbs(2);
        for xorb_obj in &xorbs {
            mount_staged(&server, xorb_obj).await;
            mount_cas_upload(&server, xorb_obj.hash, 1).await;
        }
        // A 5xx is retried; the batch falls back to CAS once every attempt has failed.
        Mock::given(method("POST"))
            .and(path("/v1/xorbs/grants"))
            .and(CommitBatch(2))
            .respond_with(ResponseTemplate::new(500))
            .expect(1 + ctx.config.client.retry_max_attempts as u64)
            .mount(&server)
            .await;
        mount_shard_upload(&server, &log, 1).await;

        let client = direct_upload_client(&ctx, &server);
        upload_all(&client, xorbs).await;

        upload_shard(&client).await.unwrap();
        assert_eq!(log.entries(), ["shard"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_direct_upload_falls_back_to_cas_when_grants_are_unavailable() {
        let server = MockServer::start().await;
        let xorb_obj = chunks_only_xorb(3);
        let (hash, n_bytes) = (xorb_obj.hash, xorb_obj.serialized_data.len() as u64);

        // A CAS without a staging bucket answers 404; the xorb goes through the regular route.
        Mock::given(method("POST"))
            .and(path("/v1/xorbs/grants"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        mount_cas_upload(&server, hash, 1).await;

        let ctx = direct_upload_ctx();
        let client = direct_upload_client(&ctx, &server);
        let reported = Arc::new(AtomicU64::new(0));
        assert_eq!(upload(&client, xorb_obj, &reported).await.unwrap(), n_bytes);
        assert_eq!(reported.load(Ordering::Relaxed), n_bytes);

        // The 404 is remembered: the next xorb goes straight through CAS (the grants mock
        // expects exactly one call over the whole test), and a flush with nothing pending makes
        // no call either.
        let second = chunks_only_xorb(4);
        let (second_hash, second_bytes) = (second.hash, second.serialized_data.len() as u64);
        mount_cas_upload(&server, second_hash, 1).await;
        assert_eq!(upload(&client, second, &reported).await.unwrap(), second_bytes);
        client.flush_pending_commits().await.unwrap();
    }

    #[test]
    fn test_clients_share_controllers_per_ctx_and_endpoint() {
        let ctx = XetContext::default().unwrap();
        let c1 = RemoteClient::new(ctx.clone(), "https://cas-a.example.com", &None, "", false, None);
        let c2 = RemoteClient::new(ctx.clone(), "https://cas-a.example.com", &None, "", false, None);

        // Same ctx + same endpoint: shared upload and download controllers.
        assert!(Arc::ptr_eq(&c1.upload_concurrency_controller, &c2.upload_concurrency_controller));
        assert!(Arc::ptr_eq(&c1.download_concurrency_controller, &c2.download_concurrency_controller));

        // Same ctx, different endpoint: independent controllers.
        let c3 = RemoteClient::new(ctx.clone(), "https://cas-b.example.com", &None, "", false, None);
        assert!(!Arc::ptr_eq(&c1.upload_concurrency_controller, &c3.upload_concurrency_controller));

        // Creating a second endpoint must not evict the first: re-fetching cas-a still shares with c1.
        let c5 = RemoteClient::new(ctx.clone(), "https://cas-a.example.com", &None, "", false, None);
        assert!(Arc::ptr_eq(&c1.upload_concurrency_controller, &c5.upload_concurrency_controller));

        // Different ctx (different session), same endpoint: independent controllers.
        let ctx2 = XetContext::default().unwrap();
        let c4 = RemoteClient::new(ctx2, "https://cas-a.example.com", &None, "", false, None);
        assert!(!Arc::ptr_eq(&c1.upload_concurrency_controller, &c4.upload_concurrency_controller));
    }

    #[ignore = "requires a running CAS server"]
    #[traced_test]
    #[test]
    fn test_basic_put() {
        // Arrange
        let prefix = PREFIX_DEFAULT;
        let raw_xorb = build_raw_xorb(3, ChunkSize::Random(512, 10248));

        let ctx = XetContext::default().unwrap();
        let client = RemoteClient::new(ctx.clone(), CAS_ENDPOINT, &None, "", false, None);

        let xorb_obj = build_and_verify_xorb_object(raw_xorb, CompressionScheme::LZ4);

        // Act
        let result = ctx
            .runtime
            .bridge_sync(async move {
                let permit = client.acquire_upload_permit().await.unwrap();
                client.upload_xorb(prefix, xorb_obj, None, permit).await
            })
            .unwrap();

        // Assert
        assert!(result.is_ok());
    }
}
