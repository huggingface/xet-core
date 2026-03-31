use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http::header::HeaderMap;
use xet_core_structures::merklehash::MerkleHash;
use xet_core_structures::xorb_object::XorbObject;

use super::simulation_types::{
    FetchTermDataRequest, FetchTermDataResponse, FileShardsEntry, FileSizeResponse, XorbExistsResponse,
    XorbLengthResponse, XorbRangesRequest, XorbRangesResponse, XorbRawLengthResponse,
};
use crate::cas_client::RemoteClient;
use crate::cas_client::interface::Client;
use crate::cas_client::simulation::{DeletionControlableClient, DirectAccessClient};
use crate::cas_types::{FileRange, HexMerkleHash, QueryReconstructionResponseV2, XorbReconstructionFetchInfo};
use crate::error::{ClientError, Result};

const CONFIG_POST_MAX_ATTEMPTS: usize = 4;
const CONFIG_POST_RETRY_DELAY_MS: u64 = 40;

fn duration_to_expiration_secs_ceil(expiration: Option<Duration>) -> u64 {
    expiration.map_or(0, |d| d.as_secs().saturating_add(u64::from(d.subsec_nanos() > 0)))
}

/// A client that connects to a `LocalTestServer` via HTTP and provides access
/// to both `DirectAccessClient` and `DeletionControlableClient` operations
/// through the `/simulation/` routes.
///
/// Standard `Client` trait methods are delegated to an internal `RemoteClient`
/// that uses the regular CAS API routes.
pub struct SimulationControlClient {
    endpoint: String,
    http_client: reqwest::Client,
    remote_client: Arc<RemoteClient>,
}

impl SimulationControlClient {
    /// Creates a new client connected to the given server endpoint URL.
    pub fn new(endpoint: &str) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::USER_AGENT, http::header::HeaderValue::from_static("simulation-control-client"));
        let remote_client = RemoteClient::new(endpoint, &None, "simulation-session", false, Some(Arc::new(headers)));

        Self {
            endpoint: endpoint.to_string(),
            http_client: reqwest::Client::new(),
            remote_client,
        }
    }

    /// Constructs a full URL for a `/simulation/` endpoint path.
    fn sim_url(&self, path: &str) -> String {
        format!("{}/simulation{}", self.endpoint, path)
    }

    /// Posts a config key-value pair to the `/simulation/set_config` endpoint.
    fn post_config(&self, config: &str, value: &str) {
        let url = format!(
            "{}/simulation/set_config?config={}&value={}",
            self.endpoint,
            urlencoding::encode(config),
            urlencoding::encode(value),
        );
        let client = self.http_client.clone();
        let config_name = config.to_string();
        let config_value = value.to_string();
        tokio::spawn(async move {
            for attempt in 1..=CONFIG_POST_MAX_ATTEMPTS {
                match client.post(&url).send().await {
                    Ok(response) if response.status().is_success() => return,
                    Ok(response) => {
                        let status = response.status();
                        let body = response.text().await.unwrap_or_default();

                        if !status.is_server_error() || attempt == CONFIG_POST_MAX_ATTEMPTS {
                            tracing::warn!(
                                "simulation config apply failed: config={} value={} attempt={}/{} status={} body={}",
                                config_name,
                                config_value,
                                attempt,
                                CONFIG_POST_MAX_ATTEMPTS,
                                status,
                                body
                            );
                            return;
                        }

                        tracing::warn!(
                            "simulation config apply retrying: config={} value={} attempt={}/{} status={} body={}",
                            config_name,
                            config_value,
                            attempt,
                            CONFIG_POST_MAX_ATTEMPTS,
                            status,
                            body
                        );
                    },
                    Err(error) => {
                        if attempt == CONFIG_POST_MAX_ATTEMPTS {
                            tracing::warn!(
                                "simulation config apply failed after retries: config={} value={} attempt={}/{} error={}",
                                config_name,
                                config_value,
                                attempt,
                                CONFIG_POST_MAX_ATTEMPTS,
                                error
                            );
                            return;
                        }

                        tracing::warn!(
                            "simulation config apply retrying after transport error: config={} value={} attempt={}/{} error={}",
                            config_name,
                            config_value,
                            attempt,
                            CONFIG_POST_MAX_ATTEMPTS,
                            error
                        );
                    },
                }

                let delay_ms = CONFIG_POST_RETRY_DELAY_MS.saturating_mul(attempt as u64);
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            }
        });
    }

    /// Checks an HTTP response status, mapping errors to `ClientError`.
    async fn check_status(response: reqwest::Response) -> Result<reqwest::Response> {
        let status = response.status();
        if status.is_success() {
            Ok(response)
        } else if status == reqwest::StatusCode::NOT_IMPLEMENTED {
            Err(ClientError::Other("Deletion controls not available for this server backend".to_string()))
        } else if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
            Err(ClientError::InvalidRange)
        } else {
            let body = response.text().await.unwrap_or_default();
            Err(ClientError::Other(format!("HTTP {status}: {body}")))
        }
    }

    /// Like `check_status`, but maps 404 to `ClientError::XORBNotFound` for XORB endpoints.
    async fn check_xorb_status(response: reqwest::Response, hash: &MerkleHash) -> Result<reqwest::Response> {
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            Err(ClientError::XORBNotFound(*hash))
        } else {
            Self::check_status(response).await
        }
    }
}

#[async_trait]
impl Client for SimulationControlClient {
    /// Delegates file reconstruction info lookup to the internal `RemoteClient`.
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(xet_core_structures::metadata_shard::file_structs::MDBFileInfo, Option<MerkleHash>)>> {
        self.remote_client.get_file_reconstruction_info(file_hash).await
    }

    /// Delegates reconstruction query to the internal `RemoteClient`.
    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponseV2>> {
        self.remote_client.get_reconstruction(file_id, bytes_range).await
    }

    /// Delegates batch reconstruction query to the internal `RemoteClient`.
    async fn batch_get_reconstruction(
        &self,
        file_ids: &[MerkleHash],
    ) -> Result<crate::cas_types::BatchQueryReconstructionResponse> {
        self.remote_client.batch_get_reconstruction(file_ids).await
    }

    /// Delegates download permit acquisition to the internal `RemoteClient`.
    async fn acquire_download_permit(&self) -> Result<crate::cas_client::adaptive_concurrency::ConnectionPermit> {
        self.remote_client.acquire_download_permit().await
    }

    /// Delegates file term data download to the internal `RemoteClient`.
    async fn get_file_term_data(
        &self,
        url_info: Box<dyn crate::cas_client::interface::URLProvider>,
        download_permit: crate::cas_client::adaptive_concurrency::ConnectionPermit,
        progress_callback: Option<crate::cas_client::ProgressCallback>,
        uncompressed_size_if_known: Option<usize>,
    ) -> Result<(Bytes, Vec<u32>)> {
        self.remote_client
            .get_file_term_data(url_info, download_permit, progress_callback, uncompressed_size_if_known)
            .await
    }

    /// Delegates global dedup shard query to the internal `RemoteClient`.
    async fn query_for_global_dedup_shard(&self, prefix: &str, chunk_hash: &MerkleHash) -> Result<Option<Bytes>> {
        self.remote_client.query_for_global_dedup_shard(prefix, chunk_hash).await
    }

    /// Delegates upload permit acquisition to the internal `RemoteClient`.
    async fn acquire_upload_permit(&self) -> Result<crate::cas_client::adaptive_concurrency::ConnectionPermit> {
        self.remote_client.acquire_upload_permit().await
    }

    /// Delegates shard upload to the internal `RemoteClient`.
    async fn upload_shard(
        &self,
        shard_data: Bytes,
        upload_permit: crate::cas_client::adaptive_concurrency::ConnectionPermit,
    ) -> Result<bool> {
        self.remote_client.upload_shard(shard_data, upload_permit).await
    }

    /// Delegates XORB upload to the internal `RemoteClient`.
    async fn upload_xorb(
        &self,
        prefix: &str,
        serialized_xorb_object: xet_core_structures::xorb_object::SerializedXorbObject,
        progress_callback: Option<crate::cas_client::ProgressCallback>,
        upload_permit: crate::cas_client::adaptive_concurrency::ConnectionPermit,
    ) -> Result<u64> {
        self.remote_client
            .upload_xorb(prefix, serialized_xorb_object, progress_callback, upload_permit)
            .await
    }
}

#[async_trait]
impl DirectAccessClient for SimulationControlClient {
    fn set_global_dedup_shard_expiration(&self, expiration: Option<Duration>) {
        self.post_config("global_dedup_shard_expiration", &duration_to_expiration_secs_ceil(expiration).to_string());
    }

    fn set_fetch_term_url_expiration(&self, expiration: Duration) {
        self.post_config("url_expiration", &(expiration.as_millis() as u64).to_string());
    }

    async fn apply_api_delay(&self) {
        // No-op: delays are applied server-side via set_api_delay_range
    }

    fn set_max_ranges_per_fetch(&self, max_ranges: usize) {
        self.post_config("max_ranges_per_fetch", &max_ranges.to_string());
    }

    fn disable_v2_reconstruction(&self, status_code: u16) {
        self.post_config("disable_v2_reconstruction", &status_code.to_string());
    }

    async fn get_reconstruction_v1(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<crate::cas_types::QueryReconstructionResponse>> {
        self.remote_client.get_reconstruction_v1(file_id, bytes_range).await
    }

    async fn get_reconstruction_v2(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponseV2>> {
        self.remote_client.get_reconstruction_v2(file_id, bytes_range).await
    }

    fn set_api_delay_range(&self, delay_range: Option<Range<Duration>>) {
        match delay_range {
            Some(range) => {
                let value = format!("({}ms, {}ms)", range.start.as_millis(), range.end.as_millis());
                self.post_config("api_delay", &value);
            },
            None => {
                self.post_config("api_delay", "(0ms, 0ms)");
            },
        }
    }

    /// Lists all XORB hashes via the `/simulation/xorbs` endpoint.
    async fn list_xorbs(&self) -> Result<Vec<MerkleHash>> {
        let resp = self
            .http_client
            .get(self.sim_url("/xorbs"))
            .send()
            .await
            .map_err(|e| ClientError::Other(e.to_string()))?;
        let resp = Self::check_status(resp).await?;
        resp.json().await.map_err(|e| ClientError::Other(e.to_string()))
    }

    /// Deletes a XORB by hash via the `/simulation/xorbs/{hash}` endpoint.
    async fn delete_xorb(&self, hash: &MerkleHash) {
        let hex = HexMerkleHash::from(*hash);
        let url = self.sim_url(&format!("/xorbs/{hex}"));
        let _ = self.http_client.delete(&url).send().await;
    }

    /// Retrieves the full XORB contents by hash via the `/simulation/xorbs/{hash}` endpoint.
    async fn get_full_xorb(&self, hash: &MerkleHash) -> Result<Bytes> {
        let hex = HexMerkleHash::from(*hash);
        let url = self.sim_url(&format!("/xorbs/{hex}"));
        let resp = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| ClientError::Other(e.to_string()))?;
        let resp = Self::check_xorb_status(resp, hash).await?;
        resp.bytes().await.map_err(|e| ClientError::Other(e.to_string()))
    }

    /// Retrieves specific chunk ranges from a XORB via the `/simulation/xorbs/{hash}/ranges` endpoint.
    async fn get_xorb_ranges(&self, hash: &MerkleHash, chunk_ranges: Vec<(u32, u32)>) -> Result<Vec<Bytes>> {
        let hex = HexMerkleHash::from(*hash);
        let url = self.sim_url(&format!("/xorbs/{hex}/ranges"));
        let body = XorbRangesRequest { ranges: chunk_ranges };
        let resp = self
            .http_client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| ClientError::Other(e.to_string()))?;
        let resp = Self::check_xorb_status(resp, hash).await?;
        let result: XorbRangesResponse = resp.json().await.map_err(|e| ClientError::Other(e.to_string()))?;
        Ok(result.data.into_iter().map(Bytes::from).collect())
    }

    /// Returns the chunk count of a XORB via the `/simulation/xorbs/{hash}/length` endpoint.
    async fn xorb_length(&self, hash: &MerkleHash) -> Result<u32> {
        let hex = HexMerkleHash::from(*hash);
        let url = self.sim_url(&format!("/xorbs/{hex}/length"));
        let resp = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| ClientError::Other(e.to_string()))?;
        let resp = Self::check_xorb_status(resp, hash).await?;
        let result: XorbLengthResponse = resp.json().await.map_err(|e| ClientError::Other(e.to_string()))?;
        Ok(result.length)
    }

    /// Checks whether a XORB exists via the `/simulation/xorbs/{hash}/exists` endpoint.
    async fn xorb_exists(&self, hash: &MerkleHash) -> Result<bool> {
        let hex = HexMerkleHash::from(*hash);
        let url = self.sim_url(&format!("/xorbs/{hex}/exists"));
        let resp = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| ClientError::Other(e.to_string()))?;
        let resp = Self::check_status(resp).await?;
        let result: XorbExistsResponse = resp.json().await.map_err(|e| ClientError::Other(e.to_string()))?;
        Ok(result.exists)
    }

    /// Fetches the raw XORB bytes and deserializes the `XorbObject` footer locally.
    async fn xorb_footer(&self, hash: &MerkleHash) -> Result<XorbObject> {
        let raw_bytes = self.get_xorb_raw_bytes(hash, None).await?;
        XorbObject::deserialize(&mut std::io::Cursor::new(raw_bytes))
            .map_err(|e| ClientError::Other(format!("Failed to deserialize XorbObject footer: {e}")))
    }

    /// Returns the file size via the `/simulation/files/{hash}/size` endpoint.
    async fn get_file_size(&self, hash: &MerkleHash) -> Result<u64> {
        let hex = HexMerkleHash::from(*hash);
        let url = self.sim_url(&format!("/files/{hex}/size"));
        let resp = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| ClientError::Other(e.to_string()))?;
        let resp = Self::check_status(resp).await?;
        let result: FileSizeResponse = resp.json().await.map_err(|e| ClientError::Other(e.to_string()))?;
        Ok(result.size)
    }

    /// Retrieves file data, optionally with a byte range, via the `/simulation/files/{hash}/data` endpoint.
    async fn get_file_data(&self, hash: &MerkleHash, byte_range: Option<FileRange>) -> Result<Bytes> {
        let hex = HexMerkleHash::from(*hash);
        let url = self.sim_url(&format!("/files/{hex}/data"));
        let mut req = self.http_client.get(&url);
        if let Some(range) = byte_range {
            req = req.header(reqwest::header::RANGE, format!("bytes={}-{}", range.start, range.end.saturating_sub(1)));
        }
        let resp = req.send().await.map_err(|e| ClientError::Other(e.to_string()))?;
        let resp = Self::check_status(resp).await?;
        resp.bytes().await.map_err(|e| ClientError::Other(e.to_string()))
    }

    /// Retrieves raw XORB bytes, optionally with a byte range, via the `/simulation/xorbs/{hash}/raw` endpoint.
    async fn get_xorb_raw_bytes(&self, hash: &MerkleHash, byte_range: Option<FileRange>) -> Result<Bytes> {
        let hex = HexMerkleHash::from(*hash);
        let url = self.sim_url(&format!("/xorbs/{hex}/raw"));
        let mut req = self.http_client.get(&url);
        if let Some(range) = byte_range {
            req = req.header(reqwest::header::RANGE, format!("bytes={}-{}", range.start, range.end.saturating_sub(1)));
        }
        let resp = req.send().await.map_err(|e| ClientError::Other(e.to_string()))?;
        let resp = Self::check_xorb_status(resp, hash).await?;
        resp.bytes().await.map_err(|e| ClientError::Other(e.to_string()))
    }

    /// Returns the raw byte length of a XORB via the `/simulation/xorbs/{hash}/raw_length` endpoint.
    async fn xorb_raw_length(&self, hash: &MerkleHash) -> Result<u64> {
        let hex = HexMerkleHash::from(*hash);
        let url = self.sim_url(&format!("/xorbs/{hex}/raw_length"));
        let resp = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| ClientError::Other(e.to_string()))?;
        let resp = Self::check_xorb_status(resp, hash).await?;
        let result: XorbRawLengthResponse = resp.json().await.map_err(|e| ClientError::Other(e.to_string()))?;
        Ok(result.length)
    }

    /// Fetches reconstructed term data via the `/simulation/fetch_term_data` endpoint.
    async fn fetch_term_data(
        &self,
        hash: MerkleHash,
        fetch_term: XorbReconstructionFetchInfo,
    ) -> Result<(Bytes, Vec<u32>)> {
        let url = self.sim_url("/fetch_term_data");
        let body = FetchTermDataRequest { hash, fetch_term };
        let resp = self
            .http_client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| ClientError::Other(e.to_string()))?;
        let resp = Self::check_status(resp).await?;
        let result: FetchTermDataResponse = resp.json().await.map_err(|e| ClientError::Other(e.to_string()))?;
        Ok((Bytes::from(result.data), result.chunk_byte_indices))
    }

    async fn verify_integrity(&self) -> Result<()> {
        let resp = self
            .http_client
            .post(self.sim_url("/verify_integrity"))
            .send()
            .await
            .map_err(|e| ClientError::Other(e.to_string()))?;
        Self::check_status(resp).await?;
        Ok(())
    }

    async fn verify_all_reachable(&self) -> Result<()> {
        let resp = self
            .http_client
            .post(self.sim_url("/verify_all_reachable"))
            .send()
            .await
            .map_err(|e| ClientError::Other(e.to_string()))?;
        Self::check_status(resp).await?;
        Ok(())
    }
}

#[async_trait]
impl DeletionControlableClient for SimulationControlClient {
    /// Lists all shard entry hashes via the `/simulation/shards` endpoint.
    async fn list_shard_entries(&self) -> Result<Vec<MerkleHash>> {
        let resp = self
            .http_client
            .get(self.sim_url("/shards"))
            .send()
            .await
            .map_err(|e| ClientError::Other(e.to_string()))?;
        let resp = Self::check_status(resp).await?;
        resp.json().await.map_err(|e| ClientError::Other(e.to_string()))
    }

    /// Retrieves raw shard bytes by hash via the `/simulation/shards/{hash}` endpoint.
    async fn get_shard_bytes(&self, hash: &MerkleHash) -> Result<Bytes> {
        let hex = HexMerkleHash::from(*hash);
        let url = self.sim_url(&format!("/shards/{hex}"));
        let resp = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| ClientError::Other(e.to_string()))?;
        let resp = Self::check_status(resp).await?;
        resp.bytes().await.map_err(|e| ClientError::Other(e.to_string()))
    }

    /// Deletes a shard entry by hash via the `/simulation/shards/{hash}` endpoint.
    async fn delete_shard_entry(&self, hash: &MerkleHash) -> Result<()> {
        let hex = HexMerkleHash::from(*hash);
        let url = self.sim_url(&format!("/shards/{hex}"));
        let resp = self
            .http_client
            .delete(&url)
            .send()
            .await
            .map_err(|e| ClientError::Other(e.to_string()))?;
        Self::check_status(resp).await?;
        Ok(())
    }

    /// Lists all (file_hash, shard_hash) pairs via the `/simulation/file_entries` endpoint.
    async fn list_file_shard_entries(&self) -> Result<Vec<(MerkleHash, MerkleHash)>> {
        let resp = self
            .http_client
            .get(self.sim_url("/file_entries"))
            .send()
            .await
            .map_err(|e| ClientError::Other(e.to_string()))?;
        let resp = Self::check_status(resp).await?;
        let entries: Vec<FileShardsEntry> = resp.json().await.map_err(|e| ClientError::Other(e.to_string()))?;
        Ok(entries.into_iter().map(|e| (e.file_hash, e.shard_hash)).collect())
    }

    /// Deletes a file entry by hash via the `/simulation/file_entries/{hash}` endpoint.
    async fn delete_file_entry(&self, file_hash: &MerkleHash) -> Result<()> {
        let hex = HexMerkleHash::from(*file_hash);
        let url = self.sim_url(&format!("/file_entries/{hex}"));
        let resp = self
            .http_client
            .delete(&url)
            .send()
            .await
            .map_err(|e| ClientError::Other(e.to_string()))?;
        Self::check_status(resp).await?;
        Ok(())
    }

    /// Removes all global-dedup table entries for a shard via `/simulation/shards/{hash}/dedup_entries`.
    async fn remove_shard_dedup_entries(&self, shard_hash: &MerkleHash) -> Result<()> {
        let hex = HexMerkleHash::from(*shard_hash);
        let url = self.sim_url(&format!("/shards/{hex}/dedup_entries"));
        let resp = self
            .http_client
            .delete(&url)
            .send()
            .await
            .map_err(|e| ClientError::Other(e.to_string()))?;
        Self::check_status(resp).await?;
        Ok(())
    }
}
