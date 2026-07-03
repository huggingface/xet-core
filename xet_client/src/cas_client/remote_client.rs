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

use super::adaptive_concurrency::{
    AdaptiveConcurrencyController, ConnectionPermit, download_controller, upload_controller,
};
use super::auth::AuthConfig;
use super::interface::{ShardUploadProgressCallback, URLProvider};
use super::progress_tracked_streams::{
    DownloadProgressStream, ProgressCallback, StreamProgressReporter, UploadProgressStream,
};
use super::retry_wrapper::{RetryWrapper, RetryableReqwestError};
use super::shard_upload_v2::read_shard_upload_ndjson;
use super::{Client, INFORMATION_LOG_LEVEL};
use crate::cas_client::ShardUploadProgressType;
use crate::cas_types::{
    BatchQueryReconstructionResponse, FileChunkHashesResponse, FileRange, HttpRange, Key, QueryReconstructionResponse,
    QueryReconstructionResponseV2, UploadShardResponse, UploadShardResponseType, UploadXorbResponse,
    X_RANGE_DIRTY_HEADER,
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
    /// Caches the discovered shard upload API version (0 = not yet probed, 1 = V1, 2 = V2).
    detected_shard_api_version: AtomicU32,
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
            upload_concurrency_controller: upload_controller(&ctx, endpoint),
            download_concurrency_controller: download_controller(&ctx, endpoint),
            detected_reconstruction_api_version: AtomicU32::new(0),
            detected_shard_api_version: AtomicU32::new(0),
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

    pub(crate) async fn upload_shard_v1(&self, shard_data: Bytes, upload_permit: ConnectionPermit) -> Result<bool> {
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
    pub(crate) async fn upload_shard_v2(
        &self,
        shard_data: Bytes,
        upload_permit: ConnectionPermit,
        progress_callback: Option<ShardUploadProgressCallback>,
    ) -> Result<bool> {
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

        // One target of "/v2/shards" API compared to "v1/shards" is to eliminate
        // the http client without read timeout.
        let client = self.authenticated_http_client.clone();

        let block_size = self.ctx.config.client.upload_reporting_block_size;

        let mut upload_reporter = StreamProgressReporter::new(n_upload_bytes as u64)
            .with_adaptive_concurrency_reporter(upload_permit.get_partial_completion_reporting_function());
        if let Some(cb) = &progress_callback {
            let cb = cb.clone();
            upload_reporter = upload_reporter
                .with_progress_callback(Arc::new(move |delta, _, _| cb(ShardUploadProgressType::Transfer(delta))));
        }

        let response = RetryWrapper::new(self.ctx.clone(), api_tag)
            .with_connection_permit(upload_permit, Some(shard_data.len() as u64))
            .run(move || {
                let upload_stream =
                    UploadProgressStream::wrap_bytes_as_stream(shard_data.clone(), block_size, upload_reporter.clone());

                client
                    .post(url.clone())
                    .with_extension(Api(api_tag))
                    .header(CONTENT_LENGTH, HeaderValue::from(n_upload_bytes)) // must be set because of streaming
                    .body(Body::wrap_stream(upload_stream))
                    .send()
            })
            .await?;

        let was_new = read_shard_upload_ndjson(response, progress_callback).await?;

        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            size = n_upload_bytes,
            api_version = "v2",
            result = if was_new { "sync performed" } else { "exists" },
            "Completed upload_shard API call",
        );

        Ok(was_new)
    }

    #[cfg(not(target_family = "wasm"))]
    pub(crate) async fn upload_shard_with_version_override(
        &self,
        shard_data: Bytes,
        upload_permit: ConnectionPermit,
        forced_version: Option<u32>,
        progress_callback: Option<ShardUploadProgressCallback>,
    ) -> Result<bool> {
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
                Ok(result) => {
                    if forced_version.is_none() {
                        self.detected_shard_api_version.store(2, Ordering::Relaxed);
                    }
                    Ok(result)
                },
                Err(e)
                    if forced_version.is_none()
                        && matches!(e.status(), Some(StatusCode::NOT_FOUND) | Some(StatusCode::NOT_IMPLEMENTED)) =>
                {
                    info!(status = ?e.status(), "V2 shard upload not available, falling back to V1");
                    let fallback_permit = self.upload_concurrency_controller.acquire_connection_permit().await?;
                    let result = self.upload_shard_v1(shard_data, fallback_permit).await?;
                    // Store after success to make sure we don't mess up on e.g. network failure.
                    self.detected_shard_api_version.store(1, Ordering::Relaxed);
                    Ok(result)
                },
                Err(e) => Err(e),
            },
            1 => self.upload_shard_v1(shard_data, upload_permit).await,
            other => Err(ClientError::InternalError(anyhow!("unsupported shard upload API version: {other}"))),
        }
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
        self.download_concurrency_controller.acquire_connection_permit().await
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
        self.upload_concurrency_controller.acquire_connection_permit().await
    }

    #[instrument(skip_all, name = "RemoteClient::upload_shard", fields(shard.len = shard_data.len()))]
    async fn upload_shard(
        &self,
        shard_data: Bytes,
        upload_permit: ConnectionPermit,
        progress_callback: Option<ShardUploadProgressCallback>,
    ) -> Result<bool> {
        if self.dry_run {
            return Ok(true);
        }

        #[cfg(target_family = "wasm")]
        {
            self.upload_shard_v1(shard_data, upload_permit).await
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

        let serialized_data = Bytes::from(std::mem::take(&mut serialized_xorb_object.serialized_data));

        #[cfg(not(target_family = "wasm"))]
        let block_size = self.ctx.config.client.upload_reporting_block_size;

        let mut upload_reporter = StreamProgressReporter::new(n_upload_bytes)
            .with_adaptive_concurrency_reporter(upload_permit.get_partial_completion_reporting_function());
        if let Some(cb) = progress_callback {
            upload_reporter = upload_reporter.with_progress_callback(cb);
        }

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
                upload_reporter.report_progress(n_transfer_bytes as usize);

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
    use tracing_test::traced_test;
    use xet_core_structures::xorb_object::CompressionScheme;
    use xet_core_structures::xorb_object::xorb_format_test_utils::{
        ChunkSize, build_and_verify_xorb_object, build_raw_xorb,
    };

    use super::*;

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
