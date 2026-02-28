use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use cas_object::SerializedCasObject;
use cas_types::{
    BatchQueryReconstructionResponse, FileRange, HttpRange, Key, QueryReconstructionResponse,
    QueryReconstructionResponseV2, UploadShardResponse, UploadShardResponseType, UploadXorbResponse,
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

impl RemoteClient {
    /// V1 reconstruction: returns per-range presigned URLs.
    pub async fn get_reconstruction_v1(
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
            "Starting get_reconstruction_v1 API call",
        );

        let api_tag = "cas::get_reconstruction_v1";
        let client = self.authenticated_http_client.clone();

        let result: Result<QueryReconstructionResponse> = RetryWrapper::new(api_tag)
            .run_and_extract_json(move || {
                let mut request = client.get(url.clone()).with_extension(Api(api_tag));
                if let Some(range) = bytes_range {
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
                    "Completed get_reconstruction_v1 API call"
                );
                Ok(Some(query_reconstruction_response))
            },
            Err(CasClientError::ReqwestError(ref e, _)) if e.status() == Some(StatusCode::RANGE_NOT_SATISFIABLE) => {
                Ok(None)
            },
            Err(e) => Err(e),
        }
    }

    /// V2 reconstruction: returns per-xorb multi-range fetch descriptors.
    pub async fn get_reconstruction_v2(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponseV2>> {
        let call_id = FN_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let url = Url::parse(&format!("{}/v2/reconstructions/{}", self.endpoint, file_id.hex()))?;
        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            %file_id,
            ?bytes_range,
            "Starting get_reconstruction_v2 API call",
        );

        let api_tag = "cas::get_reconstruction_v2";
        let client = self.authenticated_http_client.clone();

        let result: Result<QueryReconstructionResponseV2> = RetryWrapper::new(api_tag)
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
}

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl Client for RemoteClient {
    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponseV2>> {
        match self.get_reconstruction_v2(file_id, bytes_range).await {
            Ok(result) => Ok(result),
            Err(_) => Ok(self.get_reconstruction_v1(file_id, bytes_range).await?.map(Into::into)),
        }
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

        let (_, url_ranges) = url_info.retrieve_url().await?;
        let total_download_bytes: u64 = url_ranges.iter().map(|r| r.length()).sum();

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
                        let (url_string, url_ranges) = url_info
                            .retrieve_url()
                            .await
                            .map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?;
                        let url =
                            Url::parse(&url_string).map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?;

                        let range_header = url_ranges
                            .iter()
                            .map(|r| format!("{}-{}", r.start, r.end))
                            .collect::<Vec<_>>()
                            .join(",");

                        let response = http_client
                            .get(url)
                            .header(RANGE, format!("bytes={range_header}"))
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
                            // Multi-range response: collect the full body, parse the multipart
                            // parts, and deserialize each CAS object independently. We bypass
                            // the streaming progress wrapper since the multipart MIME overhead
                            // would inflate the byte count beyond what the progress tracker expects.
                            let body = resp
                                .bytes()
                                .await
                                .map_err(|e| RetryableReqwestError::RetryableError(CasClientError::from(e)))?;

                            let multipart_parts = crate::multipart::parse_multipart_byteranges(&content_type, body)
                                .map_err(RetryableReqwestError::FatalError)?;

                            let mut all_decompressed = Vec::with_capacity(uncompressed_size_if_known.unwrap_or(0));
                            let mut all_chunk_indices = Vec::<u32>::new();
                            let mut total_compressed_bytes = 0u64;

                            for part in multipart_parts {
                                total_compressed_bytes += part.data.len() as u64;

                                let (data, mut chunk_indices) =
                                    cas_object::deserialize_chunks(&mut std::io::Cursor::new(part.data.as_ref()))
                                        .map_err(|e| {
                                            RetryableReqwestError::RetryableError(CasClientError::CasObjectError(e))
                                        })?;

                                let base_offset = all_decompressed.len() as u32;
                                if !all_chunk_indices.is_empty() {
                                    chunk_indices = chunk_indices.iter().skip(1).map(|&o| o + base_offset).collect();
                                }
                                all_decompressed.extend_from_slice(&data);
                                all_chunk_indices.extend(chunk_indices);
                            }

                            // Report transfer progress for the actual data bytes
                            // (excluding MIME overhead).
                            transfer_reporter.report_progress(total_compressed_bytes as usize);

                            if let Some(expected) = uncompressed_size_if_known {
                                debug_assert_eq!(
                                    all_decompressed.len(),
                                    expected,
                                    "get_file_term_data: expected {} bytes, got {}",
                                    expected,
                                    all_decompressed.len()
                                );
                                if expected != all_decompressed.len() {
                                    warn!(
                                        "get_file_term_data: expected {} bytes, got {}",
                                        expected,
                                        all_decompressed.len()
                                    );
                                }
                            }
                            Ok((Bytes::from(all_decompressed), all_chunk_indices))
                        } else {
                            // Single-range response: deserialize directly from the stream.
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
                                            warn!(
                                                "get_file_term_data: expected {} bytes, got {}",
                                                expected,
                                                buffer.len()
                                            );
                                        }
                                    }
                                    Ok((Bytes::from(buffer), chunk_byte_indices))
                                },
                                Err(e) => Err(RetryableReqwestError::RetryableError(CasClientError::CasObjectError(e))),
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
}
