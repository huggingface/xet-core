use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use cas_object::SerializedCasObject;
use cas_types::{
    BatchQueryReconstructionResponse, FileRange, HttpRange, Key, QueryReconstructionResponse, UploadShardResponse,
    UploadShardResponseType, UploadXorbResponse,
};
use error_printer::ErrorPrinter;
#[cfg(not(target_family = "wasm"))]
use futures::TryStreamExt;
use http::HeaderValue;
use http::header::{CONTENT_LENGTH, RANGE};
use mdb_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use merklehash::MerkleHash;
use progress_tracking::upload_tracking::CompletionTracker;
use reqwest::{Body, Response, StatusCode, Url};
use reqwest_middleware::ClientWithMiddleware;
use tracing::{event, info, instrument};
use utils::auth::AuthConfig;

use crate::adaptive_concurrency::{AdaptiveConcurrencyController, ConnectionPermit};
use crate::error::{CasClientError, Result};
use crate::http_client::{Api, ResponseErrorLogger, RetryConfig};
#[cfg(not(target_family = "wasm"))]
use crate::interface::URLProvider;
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
    http_client_with_retry: Arc<ClientWithMiddleware>,
    http_client_no_retry: Arc<ClientWithMiddleware>,
    authenticated_http_client_with_retry: Arc<ClientWithMiddleware>,
    authenticated_http_client: Arc<ClientWithMiddleware>,
    upload_concurrency_controller: Arc<AdaptiveConcurrencyController>,
    download_concurrency_controller: Arc<AdaptiveConcurrencyController>,
}

impl RemoteClient {
    pub fn new(
        endpoint: &str,
        auth: &Option<AuthConfig>,
        session_id: &str,
        dry_run: bool,
        user_agent: &str,
    ) -> Arc<Self> {
        Arc::new(Self {
            endpoint: endpoint.to_string(),
            dry_run,
            authenticated_http_client_with_retry: Arc::new(
                http_client::build_auth_http_client(auth, RetryConfig::default(), session_id, user_agent).unwrap(),
            ),
            authenticated_http_client: Arc::new(
                http_client::build_auth_http_client_no_retry(auth, session_id, user_agent).unwrap(),
            ),
            http_client_with_retry: Arc::new(
                http_client::build_http_client(RetryConfig::default(), session_id, user_agent).unwrap(),
            ),
            http_client_no_retry: Arc::new(http_client::build_http_client_no_retry(session_id, user_agent).unwrap()),
            upload_concurrency_controller: AdaptiveConcurrencyController::new_upload("upload"),
            download_concurrency_controller: AdaptiveConcurrencyController::new_download("download"),
        })
    }

    /// Get the endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Get the authenticated HTTP client with retry logic.
    pub fn authenticated_http_client_with_retry(&self) -> Arc<ClientWithMiddleware> {
        self.authenticated_http_client_with_retry.clone()
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
            .run(move |_partial_report_fn| client.get(url.clone()).with_extension(Api(api_tag)).send())
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

#[cfg(not(target_family = "wasm"))]
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
            .run_and_extract_json(move |_partial_report_fn| client.get(url.clone()).with_extension(Api(api_tag)).send())
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
    #[cfg(not(target_family = "wasm"))]
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

        let mut request = self
            .authenticated_http_client_with_retry
            .get(url)
            .with_extension(Api("cas::get_reconstruction"));
        if let Some(range) = bytes_range {
            // convert exclusive-end to inclusive-end range
            request = request.header(RANGE, HttpRange::from(range).range_header())
        }
        let response = request.send().await.process_error("get_reconstruction");

        let Ok(response) = response else {
            let e = response.unwrap_err();

            // bytes_range not satisfiable
            if let CasClientError::ReqwestError(e, _) = &e
                && let Some(StatusCode::RANGE_NOT_SATISFIABLE) = e.status()
            {
                return Ok(None);
            }

            return Err(e);
        };

        let len = response.content_length();
        event!(INFORMATION_LOG_LEVEL, %file_id, len, "query_reconstruction");

        let query_reconstruction_response: QueryReconstructionResponse = response
            .json()
            .await
            .info_error_fn(|| format!("JSON parsing failed in get_reconstruction, call_id={}", call_id))?;

        event!(
            INFORMATION_LOG_LEVEL,
            call_id,
            %file_id,
            ?bytes_range,
            "Completed get_reconstruction API call"
        );

        Ok(Some(query_reconstruction_response))
    }

    #[cfg(not(target_family = "wasm"))]
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
            .run_and_extract_json(move |_partial_report_fn| client.get(url.clone()).with_extension(Api(api_tag)).send())
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

    #[cfg(not(target_family = "wasm"))]
    async fn get_file_term_data(
        &self,
        url_info: Box<dyn URLProvider>,
        download_permit: ConnectionPermit,
    ) -> Result<(Bytes, Vec<u32>)> {
        use std::sync::atomic::AtomicBool;

        let api_tag = "s3::get_range";
        let http_client = self.http_client_no_retry.clone();
        let url_info = Arc::new(tokio::sync::Mutex::new(url_info));
        let called_before = Arc::new(AtomicBool::new(false));

        RetryWrapper::new(api_tag)
            .with_retry_on_403()
            .with_connection_permit(download_permit, None)
            .run_and_extract_custom(
                move |_partial_report_fn| {
                    let http_client = http_client.clone();
                    let url_info = url_info.clone();
                    let called_before = called_before.clone();

                    async move {
                        let force_refresh = called_before.swap(true, Ordering::Relaxed);
                        let (url_string, url_range) = url_info
                            .lock()
                            .await
                            .retrieve_url(force_refresh)
                            .await
                            .map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?;
                        let url =
                            Url::parse(&url_string).map_err(|e| reqwest_middleware::Error::Middleware(e.into()))?;

                        http_client
                            .get(url)
                            .header(RANGE, url_range.range_header())
                            .with_extension(Api(api_tag))
                            .send()
                            .await
                    }
                },
                |resp: Response| async move {
                    let result = cas_object::deserialize_async::deserialize_chunks_from_stream(
                        resp.bytes_stream().map_err(std::io::Error::other),
                    )
                    .await;

                    match result {
                        Ok((data, chunk_byte_indices)) => Ok((Bytes::from(data), chunk_byte_indices)),
                        Err(e) => Err(RetryableReqwestError::RetryableError(CasClientError::CasObjectError(e))),
                    }
                },
            )
            .await
    }

    #[cfg(not(target_family = "wasm"))]
    async fn get_file_term_data_v1(
        &self,
        hash: MerkleHash,
        fetch_term: cas_types::CASReconstructionFetchInfo,
        chunk_cache: Option<Arc<dyn chunk_cache::ChunkCache>>,
        range_download_single_flight: crate::file_reconstruction_v1::RangeDownloadSingleFlight,
    ) -> Result<crate::file_reconstruction_v1::TermDownloadOutput> {
        crate::file_reconstruction_v1::get_file_term_data_impl(
            hash,
            fetch_term,
            self.http_client_with_retry.clone(),
            chunk_cache,
            range_download_single_flight,
        )
        .await
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
            .run_and_extract_json(move |_partial_report_fn| client.get(url.clone()).with_extension(Api(api_tag)).send())
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
            .run_and_extract_json(move |_partial_report_fn| {
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
        upload_tracker: Option<Arc<CompletionTracker>>,
        upload_permit: ConnectionPermit,
    ) -> Result<u64> {
        use xet_runtime::xet_config;

        use crate::upload_progress_stream::UploadProgressStream;

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

        let n_raw_bytes = serialized_cas_object.raw_num_bytes;
        let xorb_hash = serialized_cas_object.hash;
        let n_transfer_bytes = serialized_cas_object.serialized_data.len() as u64;

        let serialized_data = serialized_cas_object.serialized_data.clone();

        let upload_stream =
            UploadProgressStream::new(serialized_data.clone(), xet_config().client.upload_reporting_block_size);

        let xorb_uploaded = {
            if !self.dry_run {
                let client = self.authenticated_http_client.clone();

                let api_tag = "cas::upload_xorb";

                let response: UploadXorbResponse = RetryWrapper::new(api_tag)
                    .with_connection_permit(upload_permit, Some(n_transfer_bytes))
                    .run_and_extract_json(move |partial_report_fn| {
                        let partial_report_fn = partial_report_fn.clone();
                        let total_size = n_transfer_bytes;
                        let upload_tracker = upload_tracker.clone();

                        let progress_callback = move |bytes_sent_delta: u64, total_bytes: u64| {
                            if let Some(utr) = upload_tracker.as_ref() {
                                // First, recalibrate the sending, as the compressed size is different from the actual
                                // data size.
                                let adjusted_update = (bytes_sent_delta * n_raw_bytes) / n_upload_bytes;

                                utr.clone().register_xorb_upload_progress_background(xorb_hash, adjusted_update);
                            }

                            if let Some(ref partial_report_fn) = partial_report_fn
                                && total_size > 0
                            {
                                let portion = (total_bytes as f64 / total_size as f64).min(1.0);
                                partial_report_fn(portion, total_bytes);
                            }
                        };

                        let upload_stream = upload_stream.clone_with_reset_and_new_callback(progress_callback);
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
        upload_tracker: Option<Arc<CompletionTracker>>,
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

    fn use_xorb_footer(&self) -> bool {
        false
    }

    fn use_shard_footer(&self) -> bool {
        false
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
        let client = RemoteClient::new(CAS_ENDPOINT, &None, "", false, "");

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
