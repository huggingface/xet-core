use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use anyhow::anyhow;
use bytes::Bytes;
use futures::TryStreamExt;
use http::HeaderValue;
use http::header::{CONTENT_LENGTH, HeaderMap, RANGE};
use reqwest::{Body, Response, StatusCode, Url};
use reqwest_middleware::ClientWithMiddleware;
use tracing::{event, info, instrument};
use xet_core_structures::merklehash::MerkleHash;
use xet_core_structures::metadata_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use xet_core_structures::xorb_object::SerializedXorbObject;
use xet_runtime::core::XetContext;

use super::adaptive_concurrency::{AdaptiveConcurrencyController, ConnectionPermit};
use super::auth::AuthConfig;
use super::interface::{URLProvider, split_into_single_range_providers};
use super::progress_tracked_streams::{
    DownloadProgressStream, ProgressCallback, StreamProgressReporter, UploadProgressStream,
};
use super::retry_wrapper::{RetryWrapper, RetryableReqwestError};
use super::{Client, INFORMATION_LOG_LEVEL};
use crate::cas_types::{
    BatchQueryReconstructionResponse, FileRange, HttpRange, Key, QueryReconstructionResponse,
    QueryReconstructionResponseV2, UploadShardResponse, UploadShardResponseType, UploadXorbResponse,
};
use crate::common::http_client::{self, Api};
use crate::error::{ClientError, Result};

pub const CAS_ENDPOINT: &str = "http://localhost:8080";
pub const PREFIX_DEFAULT: &str = "default";

use lazy_static::lazy_static;

lazy_static! {
    static ref FN_CALL_ID: AtomicU64 = AtomicU64::new(1);
}

pub struct RemoteClient {
    pub(crate) ctx: XetContext,
    endpoint: String,
    dry_run: bool,
    http_client: Arc<ClientWithMiddleware>,
    authenticated_http_client: Arc<ClientWithMiddleware>,
    /// Authenticated client with no read_timeout, used for shard uploads where server-side
    /// processing time scales with file entry count and can exceed the global read_timeout.
    #[cfg(not(target_family = "wasm"))]
    shard_upload_http_client: Arc<ClientWithMiddleware>,
    upload_concurrency_controller: Arc<AdaptiveConcurrencyController>,
    download_concurrency_controller: Arc<AdaptiveConcurrencyController>,
    /// Caches the discovered reconstruction API version (0 = not yet probed, 1 = V1, 2 = V2).
    detected_reconstruction_api_version: AtomicU32,
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
        Arc::new(Self {
            ctx: ctx.clone(),
            endpoint: endpoint.to_string(),
            dry_run,
            authenticated_http_client: Arc::new(
                http_client::build_auth_http_client(&ctx, auth, session_id, unix_socket_path, custom_headers.clone())
                    .unwrap(),
            ),
            http_client: Arc::new(
                http_client::build_http_client(&ctx, session_id, unix_socket_path, custom_headers.clone()).unwrap(),
            ),
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
            upload_concurrency_controller: AdaptiveConcurrencyController::new_upload(ctx.clone(), "upload"),
            download_concurrency_controller: AdaptiveConcurrencyController::new_download(ctx.clone(), "download"),
            detected_reconstruction_api_version: AtomicU32::new(0),
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

    /// Issue a (possibly multi-range) GET against `url_info`, parse the response into
    /// decompressed bytes + chunk byte indices, retrying on transient errors.
    ///
    /// This is the per-xorb fetch primitive used both by the default multi-range path and
    /// by the multi-range 403 fallback (which calls this once per subrange).
    async fn fetch_xorb_via_provider(
        &self,
        url_info: Arc<Box<dyn URLProvider>>,
        download_permit: ConnectionPermit,
        progress_callback: Option<ProgressCallback>,
        uncompressed_size_if_known: Option<usize>,
    ) -> Result<(Bytes, Vec<u32>)> {
        let api_tag = "s3::get_range";
        let http_client = self.http_client.clone();

        let (_, url_ranges) = url_info.retrieve_url().await?;
        let total_download_bytes: u64 = url_ranges.iter().map(|r| r.length()).sum();

        let mut transfer_reporter = StreamProgressReporter::new(total_download_bytes)
            .with_adaptive_concurrency_reporter(download_permit.get_partial_completion_reporting_function());
        if let Some(cb) = progress_callback {
            transfer_reporter = transfer_reporter.with_progress_callback(cb);
        }

        RetryWrapper::new(self.ctx.clone(), api_tag)
            .with_retry_on_403()
            .with_connection_permit(download_permit, None)
            .run_and_extract_custom(
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
            .await
    }

    /// Multi-range 403 fallback: split the multi-range provider into N single-range providers
    /// and fetch each as an independent single-range GET. Concatenate the decompressed bytes
    /// in range order to produce the same result the original multi-range fetch would have.
    ///
    /// Each subrange fetch goes through the standard single-range retry path. If any subrange
    /// fetch ultimately fails (including 403 after retries), the error is propagated rather
    /// than recursively re-attempted — there is no further client-side fallback.
    async fn fetch_xorb_via_single_range_fallback(
        &self,
        url_info: Arc<Box<dyn URLProvider>>,
        progress_callback: Option<ProgressCallback>,
        uncompressed_size_if_known: Option<usize>,
    ) -> Result<(Bytes, Vec<u32>)> {
        let single_range_providers = split_into_single_range_providers(url_info).await?;
        if single_range_providers.is_empty() {
            return Err(ClientError::Other(
                "multi-range 403 fallback: URLProvider returned no ranges to split".to_string(),
            ));
        }

        let mut all_decompressed = Vec::with_capacity(uncompressed_size_if_known.unwrap_or(0));
        let mut all_chunk_indices = Vec::<u32>::new();

        for (idx, provider) in single_range_providers.into_iter().enumerate() {
            // Each subrange fetch gets its own permit so that the adaptive concurrency
            // controller observes each as an independent successful/failed transfer.
            let permit = self.acquire_download_permit_internal().await?;
            let provider_arc: Arc<Box<dyn URLProvider>> = Arc::new(provider);

            // Pass through the original progress callback; it observes only `delta` so it is
            // safe to share across the N subrange fetches.
            let (data, chunk_indices) = self
                .fetch_xorb_via_provider(provider_arc, permit, progress_callback.clone(), None)
                .await
                .map_err(|e| {
                    info!("multi-range 403 fallback: subrange {idx} fetch failed: {e}");
                    e
                })?;

            xet_core_structures::xorb_object::append_chunk_segment(
                &mut all_decompressed,
                &mut all_chunk_indices,
                &data,
                &chunk_indices,
            );
        }

        if let Some(expected) = uncompressed_size_if_known
            && expected != all_decompressed.len()
        {
            return Err(ClientError::Other(format!(
                "get_file_term_data: expected {expected} uncompressed bytes after single-range fallback, got {}",
                all_decompressed.len()
            )));
        }

        Ok((Bytes::from(all_decompressed), all_chunk_indices))
    }

    async fn acquire_download_permit_internal(&self) -> Result<ConnectionPermit> {
        self.download_concurrency_controller.acquire_connection_permit().await
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
        let mut url_str = format!("{}/reconstructions?", self.endpoint);
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
        self.download_concurrency_controller.acquire_connection_permit().await
    }

    async fn get_file_term_data(
        &self,
        url_info: Box<dyn URLProvider>,
        download_permit: ConnectionPermit,
        progress_callback: Option<ProgressCallback>,
        uncompressed_size_if_known: Option<usize>,
    ) -> Result<(Bytes, Vec<u32>)> {
        let url_info = Arc::new(url_info);

        let (_, url_ranges) = url_info.retrieve_url().await?;
        let is_multi_range = url_ranges.len() > 1;

        // Try the multi-range fetch first. This is the fast path that minimizes the number
        // of storage requests; if the URL is single-range to begin with, this is the only
        // path we take.
        match self
            .fetch_xorb_via_provider(
                url_info.clone(),
                download_permit,
                progress_callback.clone(),
                uncompressed_size_if_known,
            )
            .await
        {
            Ok(result) => Ok(result),
            Err(e) if is_multi_range && e.status() == Some(StatusCode::FORBIDDEN) => {
                // Some storage backends (CloudFront edges in particular) reject multi-range
                // Range headers with 403 before the request reaches the origin. After the
                // multi-range retry budget (`with_retry_on_403`) is exhausted, fall back to
                // fetching each subrange as an independent single-range GET against the
                // same xorb. This travels the existing V1 single-range code path.
                info!("Multi-range GET 403'd after retries; falling back to {} single-range fetches", url_ranges.len());
                self.fetch_xorb_via_single_range_fallback(url_info, progress_callback, uncompressed_size_if_known)
                    .await
            },
            Err(e) => Err(e),
        }
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
        self.upload_concurrency_controller.acquire_connection_permit().await
    }

    #[instrument(skip_all, name = "RemoteClient::upload_shard", fields(shard.len = shard_data.len()))]
    async fn upload_shard(&self, shard_data: Bytes, upload_permit: ConnectionPermit) -> Result<bool> {
        if self.dry_run {
            return Ok(true);
        }

        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let n_upload_bytes = shard_data.len();
        event!(INFORMATION_LOG_LEVEL, call_id, size = n_upload_bytes, "Starting upload_shard API call",);

        let api_tag = "cas::upload_shard";
        let url = Url::parse(&format!("{}/shards", self.endpoint))?;

        // Use the no-read-timeout client for shard uploads. reqwest's per-request timeout()
        // does NOT override the client-level read_timeout(), so we use a separate client
        // with no read_timeout. Server-side shard processing scales linearly with file entry
        // count and can exceed the global read_timeout (120s) for large shards.
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

        match response.result {
            UploadShardResponseType::Exists => {
                event!(
                    INFORMATION_LOG_LEVEL,
                    call_id,
                    size = n_upload_bytes,
                    result = "exists",
                    "Completed upload_shard API call",
                );
                Ok(false)
            },
            UploadShardResponseType::SyncPerformed => {
                event!(
                    INFORMATION_LOG_LEVEL,
                    call_id,
                    size = n_upload_bytes,
                    result = "sync performed",
                    "Completed upload_shard API call",
                );
                Ok(true)
            },
        }
    }

    #[cfg(not(target_family = "wasm"))]
    #[instrument(skip_all, name = "RemoteClient::upload_xorb", fields(key = Key{prefix : prefix.to_string(), hash : serialized_xorb_object.hash}.to_string(),
                 xorb.len = serialized_xorb_object.serialized_data.len(), xorb.num_chunks = serialized_xorb_object.num_chunks
    ))]
    async fn upload_xorb(
        &self,
        prefix: &str,
        serialized_xorb_object: SerializedXorbObject,
        progress_callback: Option<ProgressCallback>,
        upload_permit: ConnectionPermit,
    ) -> Result<u64> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: serialized_xorb_object.hash,
        };

        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let url = Url::parse(&format!("{}/v1/xorbs/{key}", self.endpoint))?;

        let n_upload_bytes = serialized_xorb_object.serialized_data.len() as u64;
        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            prefix,
            hash=%serialized_xorb_object.hash,
            size=n_upload_bytes,
            num_chunks=serialized_xorb_object.num_chunks,
            "Starting upload_xorb API call",
        );

        let n_transfer_bytes = serialized_xorb_object.serialized_data.len() as u64;

        let serialized_data = serialized_xorb_object.serialized_data.clone();
        let block_size = self.ctx.config.client.upload_reporting_block_size;

        let mut upload_reporter = StreamProgressReporter::new(n_transfer_bytes)
            .with_adaptive_concurrency_reporter(upload_permit.get_partial_completion_reporting_function());
        if let Some(cb) = progress_callback {
            upload_reporter = upload_reporter.with_progress_callback(cb);
        }

        let xorb_uploaded = {
            if !self.dry_run {
                let client = self.authenticated_http_client.clone();

                let api_tag = "cas::upload_xorb";

                let response: UploadXorbResponse = RetryWrapper::new(self.ctx.clone(), api_tag)
                    .with_connection_permit(upload_permit, Some(n_transfer_bytes))
                    .run_and_extract_json(move || {
                        let upload_stream = UploadProgressStream::wrap_bytes_as_stream(
                            serialized_data.clone(),
                            block_size,
                            upload_reporter.clone(),
                        );
                        let url = url.clone();

                        client
                            .post(url)
                            .with_extension(Api(api_tag))
                            .header(CONTENT_LENGTH, HeaderValue::from(n_upload_bytes)) // must be set because of streaming
                            .body(Body::wrap_stream(upload_stream))
                            .send()
                    })
                    .await?;

                response.was_inserted
            } else {
                true
            }
        };

        if !xorb_uploaded {
            event!(
                INFORMATION_LOG_LEVEL,
                call_id,
                prefix,
                hash=%serialized_xorb_object.hash,
                result="not_inserted",
                "Completed upload_xorb API call",
            );
        } else {
            event!(
                INFORMATION_LOG_LEVEL,
                call_id,
                prefix,
                hash=%serialized_xorb_object.hash,
                size=n_upload_bytes,
                result="inserted",
                "Completed upload_xorb API call",
            );
        }

        Ok(n_upload_bytes)
    }

    #[cfg(target_family = "wasm")]
    async fn upload_xorb(
        &self,
        prefix: &str,
        serialized_xorb_object: SerializedXorbObject,
        _progress_callback: Option<ProgressCallback>,
        _upload_permit: ConnectionPermit,
    ) -> Result<u64> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: serialized_xorb_object.hash,
        };

        let url = Url::parse(&format!("{}/v1/xorbs/{key}", self.endpoint))?;

        let n_upload_bytes = serialized_xorb_object.serialized_data.len() as u64;

        let xorb_uploaded = self
            .authenticated_http_client
            .post(url)
            .with_extension(Api("cas::upload_xorb"))
            .body(serialized_xorb_object.serialized_data)
            .send()
            .await?;

        Ok(n_upload_bytes)
    }
}

#[cfg(test)]
#[cfg(not(target_family = "wasm"))]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use tracing_test::traced_test;
    use wiremock::matchers::{header, headers, method, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    use xet_core_structures::xorb_object::xorb_format_test_utils::{
        ChunkSize, build_and_verify_xorb_object, build_raw_xorb,
    };
    use xet_core_structures::xorb_object::{CompressionScheme, serialize_chunk};
    use xet_runtime::config::XetConfig;

    use super::*;
    use crate::cas_client::interface::URLProvider;

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

    // --- multi-range 403 fallback tests --------------------------------------------------

    /// Test-only URLProvider that returns a fixed URL + ranges. `refresh_url` is a no-op
    /// counter so tests can assert how many times refresh was triggered without forcing a
    /// real reconstruction re-query.
    struct StaticURLProvider {
        url: String,
        ranges: Vec<HttpRange>,
        refresh_count: AtomicUsize,
    }

    impl StaticURLProvider {
        fn new(url: String, ranges: Vec<HttpRange>) -> Self {
            Self {
                url,
                ranges,
                refresh_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl URLProvider for StaticURLProvider {
        async fn retrieve_url(&self) -> Result<(String, Vec<HttpRange>)> {
            Ok((self.url.clone(), self.ranges.clone()))
        }

        async fn refresh_url(&self) -> Result<()> {
            self.refresh_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    /// Build a single uncompressed chunk in xorb chunk format (header + body). Each "subrange"
    /// in the multi-range fetch is independently parseable by `deserialize_chunks`.
    fn build_one_chunk(payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        serialize_chunk(payload, &mut buf, CompressionScheme::None).expect("serialize_chunk");
        buf
    }

    fn test_remote_client() -> Arc<RemoteClient> {
        // Tight retry budget to keep the failure-path tests fast.
        let config = XetConfig::new()
            .with_config("client.retry_max_attempts", "2")
            .unwrap()
            .with_config("client.retry_base_delay", "5ms")
            .unwrap();
        let ctx = XetContext::from_external(tokio::runtime::Handle::current(), config);
        // The CAS endpoint isn't exercised in get_file_term_data tests (we call the storage GET
        // path directly via a URLProvider pointing at the mock server).
        RemoteClient::new(ctx, "http://unused", &None, "test-session", false, None)
    }

    #[tokio::test]
    async fn test_get_file_term_data_multi_range_happy_path() {
        let server = MockServer::start().await;

        let chunk_a = b"chunk-a-payload-bytes!".to_vec();
        let chunk_b = b"chunk-b-payload-bytes!!!".to_vec();
        let serialized_a = build_one_chunk(&chunk_a);
        let serialized_b = build_one_chunk(&chunk_b);

        // The mock storage object is the two serialized chunks concatenated. The multi-range
        // GET asks for [0..serialized_a.len()-1] and [serialized_a.len()..total-1].
        let len_a = serialized_a.len();
        let object = [serialized_a.as_slice(), serialized_b.as_slice()].concat();
        let total = object.len();

        // wiremock's `header()` matcher splits the actual header value on commas, so a
        // multi-range header `bytes=0-29,30-61` is matched as two parts: `bytes=0-29` and
        // `30-61`. Use the `headers()` (multi-value) matcher to match accordingly.
        let multi_range_parts = vec![format!("bytes=0-{}", len_a - 1), format!("{}-{}", len_a, total - 1)];

        // Build a multipart/byteranges response so the multi-range parser path is exercised.
        let boundary = "TESTBOUNDARY";
        let mut body = Vec::new();
        for (start, end, slice) in [
            (0u64, (len_a - 1) as u64, &object[..len_a]),
            (len_a as u64, (total - 1) as u64, &object[len_a..]),
        ] {
            body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            body.extend_from_slice(b"Content-Type: application/octet-stream\r\n");
            body.extend_from_slice(format!("Content-Range: bytes {start}-{end}/{total}\r\n\r\n").as_bytes());
            body.extend_from_slice(slice);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

        Mock::given(method("GET"))
            .and(path_regex(r"^/storage/xorb$"))
            .and(headers("range", multi_range_parts))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header("content-type", format!("multipart/byteranges; boundary={boundary}"))
                    .set_body_bytes(body),
            )
            .expect(1)
            .mount(&server)
            .await;

        let url = format!("{}/storage/xorb", server.uri());
        let provider: Box<dyn URLProvider> = Box::new(StaticURLProvider::new(
            url,
            vec![
                HttpRange::new(0, (len_a - 1) as u64),
                HttpRange::new(len_a as u64, (total - 1) as u64),
            ],
        ));

        let client = test_remote_client();
        let permit = client.acquire_download_permit().await.unwrap();
        let (bytes, _indices) = client.get_file_term_data(provider, permit, None, None).await.unwrap();

        // Concatenated uncompressed chunk bodies.
        let expected: Vec<u8> = [chunk_a.as_slice(), chunk_b.as_slice()].concat();
        assert_eq!(bytes.as_ref(), expected.as_slice(), "happy-path bytes should match");
    }

    #[tokio::test]
    async fn test_get_file_term_data_multi_range_403_falls_back_to_single_range() {
        let server = MockServer::start().await;

        let chunk_a = b"first-range-bytes".to_vec();
        let chunk_b = b"second-range-bytes!!".to_vec();
        let serialized_a = build_one_chunk(&chunk_a);
        let serialized_b = build_one_chunk(&chunk_b);

        let len_a = serialized_a.len();
        let len_b = serialized_b.len();
        let total = len_a + len_b;

        let multi_range_parts = vec![format!("bytes=0-{}", len_a - 1), format!("{}-{}", len_a, total - 1)];
        let single_range_header_a = format!("bytes=0-{}", len_a - 1);
        let single_range_header_b = format!("bytes={}-{}", len_a, total - 1);

        // The multi-range request always 403s — modeling the CloudFront-edge behavior.
        Mock::given(method("GET"))
            .and(path_regex(r"^/storage/xorb$"))
            .and(headers("range", multi_range_parts))
            .respond_with(ResponseTemplate::new(403).set_body_string("Invalid Range Specified"))
            // 2 retry_max_attempts => up to 3 attempts on the multi-range path.
            .expect(1..=3)
            .mount(&server)
            .await;

        // Each single-range subrange GET returns its slice as a plain 206.
        Mock::given(method("GET"))
            .and(path_regex(r"^/storage/xorb$"))
            .and(header("range", single_range_header_a.as_str()))
            .respond_with(ResponseTemplate::new(206).set_body_bytes(serialized_a.clone()))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path_regex(r"^/storage/xorb$"))
            .and(header("range", single_range_header_b.as_str()))
            .respond_with(ResponseTemplate::new(206).set_body_bytes(serialized_b.clone()))
            .expect(1)
            .mount(&server)
            .await;

        let url = format!("{}/storage/xorb", server.uri());
        let provider: Box<dyn URLProvider> = Box::new(StaticURLProvider::new(
            url,
            vec![
                HttpRange::new(0, (len_a - 1) as u64),
                HttpRange::new(len_a as u64, (total - 1) as u64),
            ],
        ));

        let client = test_remote_client();
        let permit = client.acquire_download_permit().await.unwrap();
        let (bytes, _indices) = client.get_file_term_data(provider, permit, None, None).await.unwrap();

        let expected: Vec<u8> = [chunk_a.as_slice(), chunk_b.as_slice()].concat();
        assert_eq!(bytes.as_ref(), expected.as_slice(), "fallback should concatenate single-range results in order");
    }

    #[tokio::test]
    async fn test_get_file_term_data_single_range_fallback_errors_propagate() {
        let server = MockServer::start().await;

        // Use tiny payloads — they don't have to decode successfully, since the test exits with
        // a 403 before reaching the parser.
        let len_a: usize = 16;
        let len_b: usize = 32;
        let total = len_a + len_b;

        let multi_range_parts = vec![format!("bytes=0-{}", len_a - 1), format!("{}-{}", len_a, total - 1)];

        // The multi-range request 403s.
        Mock::given(method("GET"))
            .and(path_regex(r"^/storage/xorb$"))
            .and(headers("range", multi_range_parts))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        // Every single-range request also 403s — fallback should propagate, not recurse.
        Mock::given(method("GET"))
            .and(path_regex(r"^/storage/xorb$"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let url = format!("{}/storage/xorb", server.uri());
        let provider: Box<dyn URLProvider> = Box::new(StaticURLProvider::new(
            url,
            vec![
                HttpRange::new(0, (len_a - 1) as u64),
                HttpRange::new(len_a as u64, (total - 1) as u64),
            ],
        ));

        let client = test_remote_client();
        let permit = client.acquire_download_permit().await.unwrap();
        let result = client.get_file_term_data(provider, permit, None, None).await;

        assert!(result.is_err(), "expected error when all paths 403, got Ok");
        let err = result.unwrap_err();
        assert_eq!(err.status(), Some(StatusCode::FORBIDDEN), "error should preserve 403 status: {err:?}");

        // Sanity: the server received a bounded number of requests overall — no infinite loop.
        let recv_count = server.received_requests().await.unwrap().len();
        assert!(recv_count < 20, "too many requests, possible loop: {recv_count}");
    }
}
