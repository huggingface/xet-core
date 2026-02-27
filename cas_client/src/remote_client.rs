use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use cas_object::SerializedCasObject;
use cas_types::{
    BatchQueryReconstructionResponse, FileRange, HttpRange, Key, QueryReconstructionResponse,
    QueryReconstructionResponseV2, ReconstructionResponse, UploadShardResponse, UploadShardResponseType,
    UploadXorbResponse,
};
use futures::TryStreamExt;
use http::HeaderValue;
use http::header::{CONTENT_LENGTH, HeaderMap, RANGE};
use mdb_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use merklehash::MerkleHash;
use reqwest::{Body, Response, StatusCode, Url};
use reqwest_middleware::ClientWithMiddleware;
use tracing::{event, info, instrument, warn};
use utils::auth::AuthConfig;
use xet_runtime::xet_config;

use crate::adaptive_concurrency::{AdaptiveConcurrencyController, ConnectionPermit};
use crate::error::{CasClientError, Result};
use crate::http_client::Api;
use crate::interface::URLProvider;
use crate::progress_tracked_streams::{
    DownloadProgressStream, ProgressCallback, StreamProgressReporter, UploadProgressStream,
};
use crate::retry_wrapper::{RetryWrapper, RetryableReqwestError};
use crate::{Client, INFORMATION_LOG_LEVEL, http_client};

pub const CAS_ENDPOINT: &str = "http://localhost:8080";
pub const PREFIX_DEFAULT: &str = "default";

use lazy_static::lazy_static;

lazy_static! {
    static ref FN_CALL_ID: AtomicU64 = AtomicU64::new(1);
}

pub struct RemoteClient {
    endpoint: String,
    dry_run: bool,
    http_client: Arc<ClientWithMiddleware>,
    authenticated_http_client: Arc<ClientWithMiddleware>,
    upload_concurrency_controller: Arc<AdaptiveConcurrencyController>,
    download_concurrency_controller: Arc<AdaptiveConcurrencyController>,
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
        endpoint: &str,
        auth: &Option<AuthConfig>,
        session_id: &str,
        dry_run: bool,
        unix_socket_path: Option<&str>,
        custom_headers: Option<Arc<HeaderMap>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            endpoint: endpoint.to_string(),
            dry_run,
            authenticated_http_client: Arc::new(
                http_client::build_auth_http_client(auth, session_id, unix_socket_path, custom_headers.clone())
                    .unwrap(),
            ),
            http_client: Arc::new(
                http_client::build_http_client(session_id, unix_socket_path, custom_headers).unwrap(),
            ),
            upload_concurrency_controller: AdaptiveConcurrencyController::new_upload("upload"),
            download_concurrency_controller: AdaptiveConcurrencyController::new_download("download"),
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
        endpoint: &str,
        auth: &Option<AuthConfig>,
        session_id: &str,
        dry_run: bool,
        custom_headers: Option<Arc<HeaderMap>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            endpoint: endpoint.to_string(),
            dry_run,
            authenticated_http_client: Arc::new(
                http_client::build_auth_http_client(auth, session_id, None, custom_headers.clone()).unwrap(),
            ),
            http_client: Arc::new(http_client::build_http_client(session_id, None, custom_headers).unwrap()),
            upload_concurrency_controller: AdaptiveConcurrencyController::new_upload("upload"),
            download_concurrency_controller: AdaptiveConcurrencyController::new_download("download"),
        })
    }

    /// Get the endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
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

        let result = RetryWrapper::new(api_tag)
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
    #[instrument(skip_all, name = "RemoteClient::batch_get_reconstruction")]
    #[allow(dead_code)]
    async fn batch_get_reconstruction_internal(
        &self,
        file_ids: impl Iterator<Item = &MerkleHash>,
    ) -> Result<BatchQueryReconstructionResponse> {
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
        event!(INFORMATION_LOG_LEVEL, call_id, file_ids=?file_id_list, "Starting batch_get_reconstruction API call");

        let api_tag = "cas::batch_get_reconstruction";
        let client = self.authenticated_http_client.clone();

        let response: BatchQueryReconstructionResponse = RetryWrapper::new(api_tag)
            .run_and_extract_json(move || client.get(url.clone()).with_extension(Api(api_tag)).send())
            .await?;

        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            file_ids=?file_id_list,
            response_count=response.files.len(),
            "Completed batch_get_reconstruction API call",
        );

        Ok(response)
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl Client for RemoteClient {
    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>> {
        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let url = Url::parse(&format!("{}/v1/reconstructions/{}", self.endpoint, file_id.hex()))?;
        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            %file_id,
            ?bytes_range,
            "Starting get_reconstruction API call",
        );

        let api_tag = "cas::get_reconstruction";
        let client = self.authenticated_http_client.clone();

        let result: Result<QueryReconstructionResponse> = RetryWrapper::new(api_tag)
            .run_and_extract_json(move || {
                let mut request = client.get(url.clone()).with_extension(Api(api_tag));
                if let Some(range) = bytes_range {
                    // convert exclusive-end to inclusive-end range
                    request = request.header(RANGE, HttpRange::from(range).range_header())
                }

                request.send()
            })
            .await;

        match result {
            Ok(query_reconstruction_response) => {
                event!(
                    INFORMATION_LOG_LEVEL,
                    call_id,
                    %file_id,
                    ?bytes_range,
                    "Completed get_reconstruction API call"
                );
                Ok(Some(query_reconstruction_response))
            },
            Err(CasClientError::ReqwestError(ref e, _)) if e.status() == Some(StatusCode::RANGE_NOT_SATISFIABLE) => {
                // bytes_range not satisfiable
                Ok(None)
            },
            Err(e) => Err(e),
        }
    }

    async fn get_reconstruction_v2(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<ReconstructionResponse>> {
        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let url = Url::parse(&format!("{}/v1/reconstructions/{}", self.endpoint, file_id.hex()))?;
        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            %file_id,
            ?bytes_range,
            "Starting get_reconstruction_v2 API call",
        );

        let api_tag = "cas::get_reconstruction_v2";
        let client = self.authenticated_http_client.clone();

        let result: Result<ReconstructionResponse> = RetryWrapper::new(api_tag)
            .run_and_extract_custom(
                move || {
                    let mut request = client
                        .get(url.clone())
                        .with_extension(Api(api_tag))
                        .header("X-Xet-Reconstruction-Version", "2");
                    if let Some(range) = bytes_range {
                        request = request.header(RANGE, HttpRange::from(range).range_header())
                    }
                    request.send()
                },
                |resp: Response| async move {
                    let version = resp
                        .headers()
                        .get("X-Xet-Reconstruction-Version")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("1")
                        .to_string();

                    let body = resp
                        .bytes()
                        .await
                        .map_err(|e| RetryableReqwestError::RetryableError(e.into()))?;

                    if version == "2" {
                        let v2: QueryReconstructionResponseV2 = serde_json::from_slice(&body).map_err(|e| {
                            RetryableReqwestError::RetryableError(CasClientError::Other(format!(
                                "Failed to parse V2 reconstruction response: {e}"
                            )))
                        })?;
                        Ok(ReconstructionResponse::V2(v2))
                    } else {
                        let v1: QueryReconstructionResponse = serde_json::from_slice(&body).map_err(|e| {
                            RetryableReqwestError::RetryableError(CasClientError::Other(format!(
                                "Failed to parse V1 reconstruction response: {e}"
                            )))
                        })?;
                        Ok(ReconstructionResponse::V1(v1))
                    }
                },
            )
            .await;

        match result {
            Ok(response) => {
                event!(
                    INFORMATION_LOG_LEVEL,
                    call_id,
                    %file_id,
                    ?bytes_range,
                    "Completed get_reconstruction_v2 API call"
                );
                Ok(Some(response))
            },
            Err(CasClientError::ReqwestError(ref e, _)) if e.status() == Some(StatusCode::RANGE_NOT_SATISFIABLE) => {
                Ok(None)
            },
            Err(e) => Err(e),
        }
    }

    async fn get_multi_range_term_data(
        &self,
        url: &str,
        range_header: &str,
        expected_parts: usize,
        download_permit: ConnectionPermit,
        progress_callback: Option<ProgressCallback>,
    ) -> Result<Vec<(Bytes, Vec<u32>)>> {
        let api_tag = "s3::get_multi_range";
        let http_client = self.http_client.clone();
        let url_string = url.to_string();
        let range_header_string = range_header.to_string();

        let mut transfer_reporter = StreamProgressReporter::new(0)
            .with_adaptive_concurrency_reporter(download_permit.get_partial_completion_reporting_function());
        if let Some(cb) = progress_callback {
            transfer_reporter = transfer_reporter.with_progress_callback(cb);
        }

        let result = RetryWrapper::new(api_tag)
            .with_retry_on_403()
            .with_connection_permit(download_permit, None)
            .run_and_extract_custom(
                move || {
                    let http_client = http_client.clone();
                    let url_string = url_string.clone();
                    let range_header_string = range_header_string.clone();

                    async move {
                        let url =
                            Url::parse(&url_string).map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?;

                        http_client
                            .get(url)
                            .header(RANGE, &range_header_string)
                            .with_extension(Api(api_tag))
                            .send()
                            .await
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

                        let incoming_stream = DownloadProgressStream::wrap_stream(
                            resp.bytes_stream().map_err(std::io::Error::other),
                            transfer_reporter,
                        );

                        // Collect the full body through the progress stream
                        use futures::StreamExt;
                        let mut body_parts = Vec::new();
                        futures::pin_mut!(incoming_stream);
                        while let Some(chunk) = incoming_stream.next().await {
                            let chunk =
                                chunk.map_err(|e| RetryableReqwestError::RetryableError(CasClientError::from(e)))?;
                            body_parts.push(chunk);
                        }
                        let body: Bytes = body_parts.into_iter().flatten().collect::<Vec<u8>>().into();

                        // If the response is multipart/byteranges, parse into parts.
                        // Otherwise (single range → 206 with plain body), treat the entire body as one part.
                        let part_bodies: Vec<Bytes> = if content_type.contains("multipart/byteranges") {
                            let multipart_parts =
                                crate::multipart::parse_multipart_byteranges(&content_type, body)
                                    .map_err(|e| RetryableReqwestError::FatalError(e))?;

                            if multipart_parts.len() != expected_parts {
                                return Err(RetryableReqwestError::FatalError(CasClientError::Other(format!(
                                    "Expected {expected_parts} parts in multipart response, got {}",
                                    multipart_parts.len()
                                ))));
                            }

                            multipart_parts.into_iter().map(|p| p.data).collect()
                        } else {
                            if expected_parts != 1 {
                                return Err(RetryableReqwestError::FatalError(CasClientError::Other(format!(
                                    "Expected multipart response with {expected_parts} parts, but got single-range response"
                                ))));
                            }
                            vec![body]
                        };

                        let mut results = Vec::with_capacity(part_bodies.len());
                        for part_data in part_bodies {
                            let mut buffer = Vec::new();
                            let mut writer = std::io::Cursor::new(&mut buffer);

                            let stream = futures::stream::iter(std::iter::once(Ok::<_, std::io::Error>(part_data)));

                            let result = cas_object::deserialize_async::deserialize_chunks_to_writer_from_stream(
                                stream,
                                &mut writer,
                            )
                            .await;

                            match result {
                                Ok((_compressed_len, chunk_byte_indices)) => {
                                    results.push((Bytes::from(buffer), chunk_byte_indices));
                                },
                                Err(e) => {
                                    return Err(RetryableReqwestError::RetryableError(CasClientError::CasObjectError(
                                        e,
                                    )));
                                },
                            }
                        }

                        Ok(results)
                    }
                },
            )
            .await?;

        Ok(result)
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

        let response: BatchQueryReconstructionResponse = RetryWrapper::new(api_tag)
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

        let (_, url_range) = url_info.retrieve_url().await?;
        let total_download_bytes = url_range.length();

        let mut transfer_reporter = StreamProgressReporter::new(total_download_bytes)
            .with_adaptive_concurrency_reporter(download_permit.get_partial_completion_reporting_function());
        if let Some(cb) = progress_callback {
            transfer_reporter = transfer_reporter.with_progress_callback(cb);
        }

        let result = RetryWrapper::new(api_tag)
            .with_retry_on_403()
            .with_connection_permit(download_permit, None)
            .run_and_extract_custom(
                move || {
                    let http_client = http_client.clone();
                    let url_info = url_info.clone();

                    async move {
                        let (url_string, url_range) = url_info
                            .retrieve_url()
                            .await
                            .map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?;
                        let url =
                            Url::parse(&url_string).map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?;

                        let response = http_client
                            .get(url)
                            .header(RANGE, url_range.range_header())
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
                        let incoming_stream = DownloadProgressStream::wrap_stream(
                            resp.bytes_stream().map_err(std::io::Error::other),
                            transfer_reporter,
                        );

                        let capacity = uncompressed_size_if_known.unwrap_or(0);
                        let mut buffer = Vec::with_capacity(capacity);
                        let mut writer = std::io::Cursor::new(&mut buffer);

                        let result = cas_object::deserialize_async::deserialize_chunks_to_writer_from_stream(
                            incoming_stream,
                            &mut writer,
                        )
                        .await;

                        match result {
                            Ok((_compressed_len, chunk_byte_indices)) => {
                                if let Some(expected) = uncompressed_size_if_known {
                                    debug_assert_eq!(
                                        buffer.len(),
                                        expected,
                                        "get_file_term_data: expected {} bytes, got {}",
                                        expected,
                                        buffer.len()
                                    );
                                    if expected != buffer.len() {
                                        warn!("get_file_term_data: expected {} bytes, got {}", expected, buffer.len());
                                    }
                                }
                                Ok((Bytes::from(buffer), chunk_byte_indices))
                            },
                            Err(e) => Err(RetryableReqwestError::RetryableError(CasClientError::CasObjectError(e))),
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

        let response: QueryReconstructionResponse = RetryWrapper::new(api_tag)
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
        let client = self.authenticated_http_client.clone();

        let url = Url::parse(&format!("{}/shards", self.endpoint))?;

        let response: UploadShardResponse = RetryWrapper::new(api_tag)
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
    #[instrument(skip_all, name = "RemoteClient::upload_xorb", fields(key = Key{prefix : prefix.to_string(), hash : serialized_cas_object.hash}.to_string(),
                 xorb.len = serialized_cas_object.serialized_data.len(), xorb.num_chunks = serialized_cas_object.num_chunks
    ))]
    async fn upload_xorb(
        &self,
        prefix: &str,
        serialized_cas_object: SerializedCasObject,
        progress_callback: Option<ProgressCallback>,
        upload_permit: ConnectionPermit,
    ) -> Result<u64> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: serialized_cas_object.hash,
        };

        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let url = Url::parse(&format!("{}/v1/xorbs/{key}", self.endpoint))?;

        let n_upload_bytes = serialized_cas_object.serialized_data.len() as u64;
        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            prefix,
            hash=%serialized_cas_object.hash,
            size=n_upload_bytes,
            num_chunks=serialized_cas_object.num_chunks,
            "Starting upload_xorb API call",
        );

        let n_transfer_bytes = serialized_cas_object.serialized_data.len() as u64;

        let serialized_data = serialized_cas_object.serialized_data.clone();
        let block_size = xet_config().client.upload_reporting_block_size;

        let mut upload_reporter = StreamProgressReporter::new(n_transfer_bytes)
            .with_adaptive_concurrency_reporter(upload_permit.get_partial_completion_reporting_function());
        if let Some(cb) = progress_callback {
            upload_reporter = upload_reporter.with_progress_callback(cb);
        }

        let xorb_uploaded = {
            if !self.dry_run {
                let client = self.authenticated_http_client.clone();

                let api_tag = "cas::upload_xorb";

                let response: UploadXorbResponse = RetryWrapper::new(api_tag)
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
                hash=%serialized_cas_object.hash,
                result="not_inserted",
                "Completed upload_xorb API call",
            );
        } else {
            event!(
                INFORMATION_LOG_LEVEL,
                call_id,
                prefix,
                hash=%serialized_cas_object.hash,
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
        serialized_cas_object: SerializedCasObject,
        _progress_callback: Option<ProgressCallback>,
        _upload_permit: ConnectionPermit,
    ) -> Result<u64> {
        let key = Key {
            prefix: prefix.to_string(),
            hash: serialized_cas_object.hash,
        };

        let url = Url::parse(&format!("{}/v1/xorbs/{key}", self.endpoint))?;

        let n_upload_bytes = serialized_cas_object.serialized_data.len() as u64;

        let xorb_uploaded = self
            .authenticated_http_client
            .post(url)
            .with_extension(Api("cas::upload_xorb"))
            .body(serialized_cas_object.serialized_data)
            .send()
            .await?;

        Ok(n_upload_bytes)
    }
}

#[cfg(test)]
#[cfg(not(target_family = "wasm"))]
mod tests {
    use cas_object::CompressionScheme;
    use cas_object::test_utils::*;
    use tracing_test::traced_test;
    use xet_runtime::XetRuntime;

    use super::*;

    #[ignore = "requires a running CAS server"]
    #[traced_test]
    #[test]
    fn test_basic_put() {
        // Arrange
        let prefix = PREFIX_DEFAULT;
        let raw_xorb = build_raw_xorb(3, ChunkSize::Random(512, 10248));

        let threadpool = XetRuntime::new().unwrap();
        let client = RemoteClient::new(CAS_ENDPOINT, &None, "", false, None);

        let cas_object = build_and_verify_cas_object(raw_xorb, Some(CompressionScheme::LZ4));

        // Act
        let result = threadpool
            .external_run_async_task(async move {
                let permit = client.acquire_upload_permit().await.unwrap();
                client.upload_xorb(prefix, cas_object, None, permit).await
            })
            .unwrap();

        // Assert
        assert!(result.is_ok());
    }

    /// Integration test for V2 reconstruction against a real CAS server.
    ///
    /// Requires:
    ///   - DynamoDB local on port 8000
    ///   - MinIO on port 9000
    ///   - CAS server on port 4884 with JWT secret "xet-jwt-secret"
    ///
    /// Run with:
    ///   CAS_TEST_WRITE_TOKEN=... CAS_TEST_READ_TOKEN=... \
    ///   cargo test -p cas_client test_v2_reconstruction -- --ignored --nocapture
    #[ignore = "requires a running CAS server on port 4884"]
    #[traced_test]
    #[tokio::test]
    async fn test_v2_reconstruction() {
        use cas_types::ReconstructionResponse;
        use deduplication::{Chunk, RawXorbData};
        use mdb_shard::chunk_verification::range_hash_from_chunks;
        use mdb_shard::file_structs::{
            FileDataSequenceEntry, FileDataSequenceHeader, FileMetadataExt, FileVerificationEntry, MDBFileInfo,
        };
        use mdb_shard::shard_in_memory::MDBInMemoryShard;
        use merklehash::{compute_data_hash, file_hash_with_salt};
        use rand::rngs::SmallRng;
        use rand::{Rng, RngCore, SeedableRng};
        use utils::auth::AuthConfig;

        let endpoint = "http://localhost:4884";

        let write_token = std::env::var("CAS_TEST_WRITE_TOKEN")
            .expect("Set CAS_TEST_WRITE_TOKEN env var with a valid JWT write token");
        let read_token =
            std::env::var("CAS_TEST_READ_TOKEN").expect("Set CAS_TEST_READ_TOKEN env var with a valid JWT read token");

        let write_auth = AuthConfig::maybe_new(Some(write_token), None, None);
        let write_client = RemoteClient::new(endpoint, &write_auth, "test-v2", false, None);

        // Build xorb data: 2 xorbs, one with 5 chunks, one with 3 chunks
        let chunk_size = 512usize;
        let term_spec: &[(u64, (u64, u64))] = &[(1, (0, 5)), (2, (0, 3))];

        let mut xorb_num_chunks = std::collections::HashMap::<u64, u64>::new();
        for &(xorb_seed, (_start, end)) in term_spec {
            let c = xorb_num_chunks.entry(xorb_seed).or_default();
            *c = (*c).max(end);
        }

        let mut shard = MDBInMemoryShard::default();
        let mut xorb_data = std::collections::HashMap::<u64, RawXorbData>::new();

        for (&xorb_seed, &n_chunks) in &xorb_num_chunks {
            let mut rng = SmallRng::seed_from_u64(xorb_seed);
            let n_chunks = n_chunks as usize;
            let mut chunks = Vec::with_capacity(n_chunks);

            for _ in 0..n_chunks {
                let n = rng.random_range((chunk_size / 2 + 1)..chunk_size);
                let n_left = chunk_size - n;
                let mut rng_data = vec![0u8; n];
                rng.fill_bytes(&mut rng_data);
                let mut buf = vec![0u8; chunk_size];
                buf[..n].copy_from_slice(&rng_data[..n]);
                buf[n..].copy_from_slice(&rng_data[..n_left]);
                let hash = compute_data_hash(&buf);
                chunks.push(Chunk {
                    hash,
                    data: bytes::Bytes::from(buf),
                });
            }

            let raw_xorb = RawXorbData::from_chunks(&chunks, vec![0]);
            shard.add_cas_block(raw_xorb.cas_info.clone()).unwrap();

            let serialized_xorb = cas_object::SerializedCasObject::from_xorb(raw_xorb.clone(), None, true).unwrap();
            let upload_permit = write_client.acquire_upload_permit().await.unwrap();
            write_client
                .upload_xorb("default", serialized_xorb, None, upload_permit)
                .await
                .unwrap();

            xorb_data.insert(xorb_seed, raw_xorb);
        }

        // Build file info with verification entries
        let mut file_segments = Vec::new();
        let mut verification = Vec::new();
        let mut file_data = Vec::new();
        let mut chunk_file_hashes = Vec::new();

        for &(xorb_seed, (chunk_idx_start, chunk_idx_end)) in term_spec {
            let raw_xorb = xorb_data.get(&xorb_seed).unwrap();
            let xorb_h = raw_xorb.hash();
            let (c_lb, c_ub) = (chunk_idx_start as usize, chunk_idx_end as usize);

            let mut n_bytes = 0;
            for i in c_lb..c_ub {
                let chunk_bytes = &raw_xorb.data[i];
                file_data.extend_from_slice(chunk_bytes);
                n_bytes += chunk_bytes.len();
                chunk_file_hashes.push((raw_xorb.cas_info.chunks[i].chunk_hash, chunk_bytes.len() as u64));
            }

            file_segments.push(FileDataSequenceEntry::new(
                xorb_h,
                n_bytes,
                chunk_idx_start as usize,
                chunk_idx_end as usize,
            ));

            // Compute verification hash from chunk hashes in this range
            let chunk_hashes: Vec<_> = (c_lb..c_ub).map(|i| raw_xorb.cas_info.chunks[i].chunk_hash).collect();
            let range_hash = range_hash_from_chunks(&chunk_hashes);
            verification.push(FileVerificationEntry::new(range_hash));
        }

        let file_hash = file_hash_with_salt(&chunk_file_hashes, &[0; 32]);

        // Compute a hash of the file data for metadata_ext SHA256 field.
        // For testing purposes, we use the blake3 data hash (the server only checks the field exists).
        let file_sha256 = compute_data_hash(&file_data);

        shard
            .add_file_reconstruction_info(MDBFileInfo {
                metadata: FileDataSequenceHeader::new(
                    file_hash,
                    file_segments.len(),
                    true, // contains_verification
                    true, // contains_metadata_ext
                ),
                segments: file_segments,
                verification,
                metadata_ext: Some(FileMetadataExt::new(file_sha256)),
            })
            .unwrap();

        let shard_bytes = shard.to_bytes().unwrap();

        let upload_permit = write_client.acquire_upload_permit().await.unwrap();
        write_client.upload_shard(shard_bytes.into(), upload_permit).await.unwrap();

        info!("Upload complete. File hash: {file_hash:?}");

        // Now query V2 reconstruction with a read client.
        let read_auth = AuthConfig::maybe_new(Some(read_token), None, None);
        let read_client = RemoteClient::new(endpoint, &read_auth, "test-v2", false, None);

        let file_range = cas_types::FileRange::new(0, file_data.len() as u64);
        let response = read_client
            .get_reconstruction_v2(&file_hash, Some(file_range))
            .await
            .expect("get_reconstruction_v2 should succeed");

        match response {
            Some(ReconstructionResponse::V2(v2)) => {
                assert!(!v2.terms.is_empty(), "V2 response should have terms");
                assert!(!v2.xorbs.is_empty(), "V2 response should have xorb descriptors");

                for term in &v2.terms {
                    assert!(v2.xorbs.contains_key(&term.hash), "Term hash {:?} should be in xorbs map", term.hash);
                }

                for (hash, descriptor) in &v2.xorbs {
                    assert!(!descriptor.fetch.is_empty(), "Xorb {hash:?} should have fetch entries");
                    for fetch in &descriptor.fetch {
                        assert!(!fetch.url.is_empty(), "Fetch URL should not be empty");
                        assert!(!fetch.ranges.is_empty(), "Fetch should have ranges");
                    }
                }

                info!("V2 reconstruction verified: {} terms, {} xorbs", v2.terms.len(), v2.xorbs.len());
            },
            Some(ReconstructionResponse::V1(_)) => {
                panic!("Expected V2 response but got V1 — server may not support V2");
            },
            None => {
                panic!("Expected reconstruction response but got None");
            },
        }
    }

    /// Integration test: V2 reconstruction with real CloudFront download (read-only).
    ///
    /// Tests the full V2 flow against a CAS server with prod S3/CloudFront config:
    /// 1. get_reconstruction_v2 returns V2 response with CloudFront signed URLs
    /// 2. get_multi_range_term_data downloads xorb data and deserializes CAS objects
    ///
    /// Prerequisites:
    ///   - CAS server on port 4884 with prod S3/CloudFront config
    ///
    /// Run with:
    ///   CAS_TEST_READ_TOKEN=... CAS_TEST_FILE_ID=... \
    ///   cargo test -p cas_client test_v2_cloudfront_download -- --ignored --nocapture
    #[ignore = "requires CAS server with prod CloudFront config"]
    #[traced_test]
    #[tokio::test]
    async fn test_v2_cloudfront_download() {
        use cas_types::ReconstructionResponse;
        use utils::auth::AuthConfig;

        let endpoint = "http://localhost:4884";

        let read_token =
            std::env::var("CAS_TEST_READ_TOKEN").expect("Set CAS_TEST_READ_TOKEN env var with a valid JWT read token");
        let file_id_hex =
            std::env::var("CAS_TEST_FILE_ID").expect("Set CAS_TEST_FILE_ID env var with a file hash from prod");

        let file_hash = MerkleHash::from_hex(&file_id_hex).expect("invalid file hash hex");

        let read_auth = AuthConfig::maybe_new(Some(read_token), None, None);
        let client = RemoteClient::new(endpoint, &read_auth, "test-v2-cf", false, None);

        // Optional byte range (e.g. "1000000000-1005000000") to limit the download size.
        let file_range = std::env::var("CAS_TEST_FILE_RANGE").ok().map(|s| {
            let parts: Vec<&str> = s.split('-').collect();
            cas_types::FileRange::new(parts[0].parse().unwrap(), parts[1].parse().unwrap())
        });

        // Step 1: Get V2 reconstruction
        info!("Requesting V2 reconstruction for {file_hash:?}, range={file_range:?}");
        let response = client
            .get_reconstruction_v2(&file_hash, file_range)
            .await
            .expect("get_reconstruction_v2 should succeed");

        let v2 = match response {
            Some(ReconstructionResponse::V2(v2)) => {
                info!("Got V2 response: {} terms, {} xorbs", v2.terms.len(), v2.xorbs.len());
                v2
            },
            Some(ReconstructionResponse::V1(_)) => panic!("Expected V2 but got V1"),
            None => panic!("File not found"),
        };

        // Step 2: Download xorb data via CloudFront signed URLs
        let mut total_downloaded = 0usize;
        let mut total_parts = 0usize;

        for (xorb_hash, descriptor) in &v2.xorbs {
            for fetch_entry in &descriptor.fetch {
                let range_header = fetch_entry
                    .url
                    .find('?')
                    .and_then(|qi| {
                        fetch_entry.url[qi + 1..].split('&').find_map(|p| {
                            p.strip_prefix("X-Xet-Signed-Range=")
                                .map(|v| v.replace("%20", " ").replace("%3D", "=").replace("%2C", ","))
                        })
                    })
                    .unwrap_or_default();

                info!(
                    "Downloading xorb {xorb_hash}: {} ranges, range_header={range_header}",
                    fetch_entry.ranges.len()
                );

                let permit = client.acquire_download_permit().await.unwrap();

                let parts = client
                    .get_multi_range_term_data(&fetch_entry.url, &range_header, fetch_entry.ranges.len(), permit, None)
                    .await
                    .expect("get_multi_range_term_data should succeed");

                for (data, chunk_offsets) in &parts {
                    info!(
                        "  Part: {} bytes, {} chunks",
                        data.len(),
                        chunk_offsets.len()
                    );
                    total_downloaded += data.len();
                    assert!(!data.is_empty(), "Downloaded data should not be empty");
                    assert!(!chunk_offsets.is_empty(), "Chunk offsets should not be empty");
                }

                total_parts += parts.len();
            }
        }

        info!("V2 CloudFront download complete: {total_parts} parts, {total_downloaded} bytes total");
        assert!(total_downloaded > 0, "Should have downloaded some data");
    }
}
