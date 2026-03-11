//! RemoteSimulationClient - A wrapper around RemoteClient for simulation endpoints
//!
//! This module provides a `RemoteSimulationClient` that wraps a `RemoteClient` instance,
//! allowing passthrough access via `Deref` while adding simulation-specific methods.

use std::ops::Deref;
use std::sync::Arc;

use bytes::Bytes;
use http::HeaderValue;
use http::header::CONTENT_LENGTH;
use rand::Rng;
use reqwest::{Body, Url};
use serde_json;

use super::super::adaptive_concurrency::ConnectionPermit;
use super::super::error::{CasClientError, Result};
use super::super::http_client::Api;
use super::super::interface::Client;
use super::super::progress_tracked_streams::{ProgressCallback, UploadProgressStream};
use super::super::remote_client::RemoteClient;
use super::super::retry_wrapper::RetryWrapper;
use super::local_server::ServerLatencyProfile;

/// A wrapper around `RemoteClient` that provides simulation-specific methods for controlling
/// latency profiles and uploading dummy data for benchmarking and simulation purposes.
pub struct RemoteSimulationClient {
    inner: Arc<RemoteClient>,
}

impl RemoteSimulationClient {
    /// Create a new `RemoteSimulationClient` wrapping the given `RemoteClient`.
    pub fn new(remote_client: Arc<RemoteClient>) -> Self {
        Self { inner: remote_client }
    }

    /// Set the latency profile on the simulation server.
    ///
    /// This sends a POST request to `/simulation/set_config` with the provided `ServerLatencyProfile`
    /// as JSON in the request body.
    pub async fn simulation_set_latency_profile(&self, profile: ServerLatencyProfile) -> Result<()> {
        let url = Url::parse(&format!("{}/simulation/set_config", self.inner.endpoint()))?;
        let client = self.inner.http_client();

        let json_body = serde_json::to_vec(&profile)
            .map_err(|e| CasClientError::Other(format!("Failed to serialize ServerLatencyProfile: {e}")))?;

        let response = client
            .post(url)
            .with_extension(Api("simulation::set_config"))
            .header(CONTENT_LENGTH, HeaderValue::from(json_body.len()))
            .body(json_body)
            .send()
            .await
            .map_err(|e| CasClientError::Other(format!("Failed to send set_config request: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            return Err(CasClientError::Other(format!(
                "set_config request failed with status {}: {}",
                status, error_text
            )));
        }

        Ok(())
    }

    /// Upload dummy data to the simulation server.
    ///
    /// This method uploads `n_bytes` of random data to the `/simulation/dummy_upload` endpoint.
    /// It follows the same code paths as `upload_xorb`, using `UploadProgressStream` and
    /// `RetryWrapper` with the provided `upload_permit`.
    pub async fn simulation_upload_dummy_data(&self, n_bytes: usize, upload_permit: ConnectionPermit) -> Result<u64> {
        let url = Url::parse(&format!("{}/simulation/dummy_upload", self.inner.endpoint()))?;
        let client = self.inner.http_client();

        let mut random_data = vec![0u8; n_bytes];
        rand::rng().fill(&mut random_data[..]);
        let random_bytes = Bytes::from(random_data);

        let n_upload_bytes = random_bytes.len() as u64;
        let block_size = xet_runtime::core::xet_config().client.upload_reporting_block_size;

        let api_tag = "simulation::dummy_upload";

        RetryWrapper::new(api_tag)
            .with_connection_permit(upload_permit, Some(n_upload_bytes))
            .run(move || {
                let upload_stream = UploadProgressStream::new(random_bytes.clone(), block_size);
                let url = url.clone();

                client
                    .post(url)
                    .with_extension(Api(api_tag))
                    .header(CONTENT_LENGTH, HeaderValue::from(n_upload_bytes))
                    .body(Body::wrap_stream(upload_stream))
                    .send()
            })
            .await
            .map_err(|e| CasClientError::Other(format!("Failed to upload dummy data: {e}")))?;

        Ok(n_upload_bytes)
    }
}

impl Deref for RemoteSimulationClient {
    type Target = RemoteClient;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

// Explicitly implement Client trait to allow RemoteSimulationClient to be used
// where Client trait bounds are required (e.g., Arc<dyn Client>)
#[async_trait::async_trait]
impl Client for RemoteSimulationClient {
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &xet_core_structures::merklehash::MerkleHash,
    ) -> Result<
        Option<(
            xet_core_structures::metadata_shard::file_structs::MDBFileInfo,
            Option<xet_core_structures::merklehash::MerkleHash>,
        )>,
    > {
        self.inner.get_file_reconstruction_info(file_hash).await
    }

    async fn get_reconstruction(
        &self,
        file_id: &xet_core_structures::merklehash::MerkleHash,
        bytes_range: Option<crate::cas_types::FileRange>,
    ) -> Result<Option<crate::cas_types::QueryReconstructionResponse>> {
        self.inner.get_reconstruction(file_id, bytes_range).await
    }

    async fn batch_get_reconstruction(
        &self,
        file_ids: &[xet_core_structures::merklehash::MerkleHash],
    ) -> Result<crate::cas_types::BatchQueryReconstructionResponse> {
        self.inner.batch_get_reconstruction(file_ids).await
    }

    async fn acquire_download_permit(&self) -> Result<ConnectionPermit> {
        self.inner.acquire_download_permit().await
    }

    async fn get_file_term_data(
        &self,
        url_info: Box<dyn super::super::interface::URLProvider>,
        download_permit: ConnectionPermit,
        progress_callback: Option<ProgressCallback>,
        uncompressed_size_if_known: Option<usize>,
    ) -> Result<(Bytes, Vec<u32>)> {
        self.inner
            .get_file_term_data(url_info, download_permit, progress_callback, uncompressed_size_if_known)
            .await
    }

    async fn query_for_global_dedup_shard(
        &self,
        prefix: &str,
        chunk_hash: &xet_core_structures::merklehash::MerkleHash,
    ) -> Result<Option<Bytes>> {
        self.inner.query_for_global_dedup_shard(prefix, chunk_hash).await
    }

    async fn acquire_upload_permit(&self) -> Result<ConnectionPermit> {
        self.inner.acquire_upload_permit().await
    }

    async fn upload_shard(&self, shard_data: Bytes, upload_permit: ConnectionPermit) -> Result<bool> {
        self.inner.upload_shard(shard_data, upload_permit).await
    }

    async fn upload_xorb(
        &self,
        prefix: &str,
        serialized_xorb_object: xet_core_structures::xorb_object::SerializedXorbObject,
        progress_callback: Option<ProgressCallback>,
        upload_permit: ConnectionPermit,
    ) -> Result<u64> {
        self.inner
            .upload_xorb(prefix, serialized_xorb_object, progress_callback, upload_permit)
            .await
    }
}
