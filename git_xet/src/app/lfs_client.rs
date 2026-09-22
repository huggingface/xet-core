//! LFS negotiation for standalone transfers.
//!
//! In standalone mode git-lfs delegates the metadata batch request to the agent, so downloads
//! resolve the batch action here. Large objects use parallel ranged requests against the returned
//! download URL; everything else streams it directly. The reconstruct/Xet-read path is
//! intentionally not used for downloads: the batch URL is authoritative, works anonymously for
//! public repositories, and no new server-side contract is required.
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use http::{HeaderMap, StatusCode};
use reqwest_middleware::{ClientWithMiddleware, RequestBuilder};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use xet_client::cas_client::auth::DirectRefreshRouteTokenRefresher;
use xet_client::cas_client::retry_wrapper::RetryWrapper;
use xet_client::common::http_client::build_http_client;
use xet_client::hub_client::{CredentialHelper, Operation};
use xet_runtime::core::XetContext;

use crate::auth::get_credential;
use crate::errors::{GitXetError, Result};
use crate::git_repo::GitRepo;
use crate::git_url::{GitUrl, Scheme};
use crate::lfs_agent_protocol::{GitBatchApiResponseAction, ProgressUpdater, TransferRequest};

// Large objects use parallel ranged requests against the download URL: a single connection
// cannot use the available bandwidth, while ranged requests scale with the object size.
const PARALLEL_HTTP_MIN_SIZE: u64 = 64 * 1024 * 1024;
const PARALLEL_HTTP_MAX_RANGES: u64 = 8;
const PARALLEL_HTTP_MIN_RANGE: u64 = 16 * 1024 * 1024;

pub(super) fn remote_from_lfs_url(url: &str) -> Result<GitUrl> {
    let remote: GitUrl = url
        .trim_end_matches('/')
        .strip_suffix("/info/lfs")
        .ok_or_else(|| GitXetError::config_error("--lfs-url must end in /info/lfs"))?
        .parse()?;
    if !matches!(remote.scheme(), Scheme::Http | Scheme::Https) {
        return Err(GitXetError::config_error("--lfs-url must use HTTP or HTTPS"));
    }
    Ok(remote)
}

pub(super) struct LfsClient {
    ctx: XetContext,
    repo: GitRepo,
    remote: GitUrl,
    endpoint: String,
    client: ClientWithMiddleware,
    credential: Option<Arc<dyn CredentialHelper>>,
}

#[derive(Deserialize)]
struct BatchResponse {
    transfer: Option<String>,
    objects: Vec<BatchObject>,
}

#[derive(Deserialize)]
struct BatchObject {
    oid: String,
    size: u64,
    error: Option<serde_json::Value>,
    #[serde(default)]
    actions: HashMap<String, GitBatchApiResponseAction>,
}

impl LfsClient {
    pub fn token_refresher(&self, route: &str) -> Arc<DirectRefreshRouteTokenRefresher> {
        Arc::new(DirectRefreshRouteTokenRefresher::new(
            self.ctx.clone(),
            route,
            self.client.clone(),
            self.credential.clone(),
        ))
    }

    pub fn new(ctx: &XetContext, repo: GitRepo, remote: GitUrl, endpoint: Option<String>) -> Result<Self> {
        let endpoint = endpoint.unwrap_or(remote.to_default_lfs_endpoint()?);
        let mut headers = HeaderMap::new();
        headers.insert(http::header::USER_AGENT, concat!("git-xet/", env!("CARGO_PKG_VERSION")).parse().unwrap());
        Ok(Self {
            ctx: ctx.clone(),
            repo,
            remote,
            endpoint,
            client: build_http_client(ctx, "", None, Some(Arc::new(headers)))?,
            credential: None,
        })
    }

    pub async fn batch(
        &mut self,
        req: &TransferRequest,
        operation: Operation,
    ) -> Result<Option<GitBatchApiResponseAction>> {
        let body = json!({
            "operation": operation.as_str(),
            "transfers": if matches!(operation, Operation::Upload) { vec!["xet"] } else { vec!["basic"] },
            "objects": [{"oid": req.oid, "size": req.size}],
        });
        let request = self
            .client
            .post(format!("{}/objects/batch", self.endpoint.trim_end_matches('/')))
            .header(http::header::ACCEPT, "application/vnd.git-lfs+json")
            .header(http::header::CONTENT_TYPE, "application/vnd.git-lfs+json")
            .body(body.to_string());
        let batch: BatchResponse = self.request(request, operation).await?;
        if matches!(operation, Operation::Download) && !matches!(batch.transfer.as_deref(), None | Some("basic")) {
            return Err(GitXetError::not_supported("Unexpected LFS download transfer type"));
        }
        let mut object = batch
            .objects
            .into_iter()
            .find(|o| o.oid == req.oid)
            .ok_or_else(|| GitXetError::internal("LFS response omitted the requested object"))?;
        if object.error.is_some() || object.size != req.size {
            return Err(GitXetError::internal("LFS object unavailable or size mismatch"));
        }
        let action = object.actions.remove(operation.as_str());
        if action.is_some() && matches!(operation, Operation::Upload) && batch.transfer.as_deref() != Some("xet") {
            return Err(GitXetError::not_supported("Server did not select Xet uploads"));
        }
        Ok(action)
    }

    async fn request<T: serde::de::DeserializeOwned>(
        &mut self,
        request: RequestBuilder,
        operation: Operation,
    ) -> Result<T> {
        // Public requests must not prompt. Share challenge handling and transient retries across metadata APIs.
        for attempt in 0..2 {
            let mut request = request.try_clone().expect("metadata request has a buffered body");
            if let Some(credential) = &self.credential {
                request = credential.fill_credential(request).await.map_err(GitXetError::internal)?;
            }
            let result = RetryWrapper::new(self.ctx.clone(), "lfs-metadata")
                .run_and_extract_json(move || request.try_clone().unwrap().send())
                .await;
            match result {
                Err(error) if error.status() == Some(StatusCode::UNAUTHORIZED) && attempt == 0 => {
                    self.credential = Some(get_credential(&self.repo, &self.remote, operation)?);
                },
                result => return result.map_err(|_| GitXetError::internal("LFS metadata request failed")),
            }
        }
        unreachable!()
    }

    pub async fn download<W: Write + Send + Sync + 'static>(
        &mut self,
        req: &TransferRequest,
        path: &Path,
        progress: ProgressUpdater<W>,
    ) -> Result<()> {
        let action = if req.action.href.is_empty() {
            self.batch(req, Operation::Download)
                .await?
                .ok_or_else(|| GitXetError::internal("LFS response omitted the download action"))?
        } else {
            req.action.clone()
        };

        let progress = Arc::new(progress);
        if req.size >= PARALLEL_HTTP_MIN_SIZE {
            match parallel_ranged_download(&self.client, &action, path, req.size, progress.clone()).await? {
                false => tracing::info!(oid = %req.oid, "download URL rejected range requests, using a single connection"),
                true => return verify_download(path, &req.oid, req.size),
            }
        }

        let mut request = self.client.get(&action.href);
        for (name, value) in &action.header {
            request = request.header(name, value);
        }
        let mut response = request
            .send()
            .await
            .map_err(|_| GitXetError::internal("LFS download request failed"))?
            .error_for_status()
            .map_err(http_error)?;
        let mut file = std::fs::File::create(path)?;
        let mut written = 0;
        while let Some(chunk) = response.chunk().await.map_err(http_error)? {
            written += chunk.len() as u64;
            if written > req.size {
                return Err(GitXetError::internal("LFS download exceeds expected size"));
            }
            file.write_all(&chunk)?;
            progress.update_bytes_so_far(written)?;
        }
        verify_download(path, &req.oid, req.size)
    }
}

/// Split `size` into up to `PARALLEL_HTTP_MAX_RANGES` ranges of at least
/// `PARALLEL_HTTP_MIN_RANGE` bytes. A single range when the object is small enough to be
/// served faster than the extra connections can be established.
fn plan_ranges(size: u64) -> Vec<(u64, u64)> {
    let streams = PARALLEL_HTTP_MAX_RANGES.min(size / PARALLEL_HTTP_MIN_RANGE).max(2);
    let range_size = size.div_ceil(streams);
    (0..streams)
        .map(|index| (index * range_size, size.min((index + 1) * range_size)))
        .filter(|(start, end)| start < end)
        .collect()
}

#[cfg(unix)]
fn write_range_at(file: &mut std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}

#[cfg(windows)]
fn write_range_at(file: &mut std::fs::File, buf: &[u8], offset: u64) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(buf)
}

/// Download `action.href` with parallel ranged requests. Returns `false` when the server
/// answered a range request without a partial response, so the caller falls back to one
/// connection; returns an error if a started range failed mid-stream.
async fn parallel_ranged_download<W: Write + Send + Sync + 'static>(
    client: &ClientWithMiddleware,
    action: &GitBatchApiResponseAction,
    path: &Path,
    size: u64,
    progress: Arc<ProgressUpdater<W>>,
) -> Result<bool> {
    let ranges = plan_ranges(size);
    let total = Arc::new(AtomicU64::new(0));

    let mut tasks = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        let mut request = client.get(&action.href);
        for (name, value) in &action.header {
            request = request.header(name, value);
        }
        request = request.header(http::header::RANGE, format!("bytes={start}-{}", end - 1));
        let progress = progress.clone();
        let total = total.clone();
        let path = path.to_owned();
        tasks.push(tokio::spawn(async move {
            let mut response = request
                .send()
                .await
                .map_err(|_| GitXetError::internal("ranged download request failed"))?;
            if response.status() != StatusCode::PARTIAL_CONTENT {
                return Ok(Some(false));
            }
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .map_err(GitXetError::internal)?;
            let mut offset = start;
            let mut block = Vec::with_capacity(8 * 1024 * 1024);
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|error| GitXetError::internal(http_error(error)))?
            {
                if offset + chunk.len() as u64 > end {
                    return Err(GitXetError::internal("ranged download exceeded its range"));
                }
                let written = total.fetch_add(chunk.len() as u64, Ordering::Relaxed) + chunk.len() as u64;
                block.extend_from_slice(&chunk);
                if block.len() >= 8 * 1024 * 1024 {
                    write_range_at(&mut file, &block, offset)?;
                    offset += block.len() as u64;
                    block.clear();
                    progress.update_bytes_so_far(written)?;
                }
            }
            if !block.is_empty() {
                write_range_at(&mut file, &block, offset).map_err(GitXetError::internal)?;
                file.sync_all().ok();
            }
            Ok::<Option<bool>, GitXetError>(None)
        }));
    }

    for task in tasks {
        match task.await.map_err(|error| GitXetError::internal(format!("ranged task failed: {error}")))?? {
            None | Some(true) => {},
            Some(false) => return Ok(false),
        }
    }
    Ok(true)
}

fn verify_download(path: &Path, oid: &str, size: u64) -> Result<()> {
    let mut file = std::fs::File::open(path)?;
    if file.metadata()?.len() != size {
        return Err(GitXetError::internal("Incomplete LFS download"));
    }
    let mut digest = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        digest.update(&buffer[..n]);
    }
    let hash: String = digest.finalize().iter().map(|byte| format!("{byte:02x}")).collect();
    if !hash.eq_ignore_ascii_case(oid) {
        return Err(GitXetError::internal("LFS download SHA-256 mismatch"));
    }
    Ok(())
}

fn http_error(error: reqwest::Error) -> GitXetError {
    GitXetError::internal(error.without_url())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_ranges_bounds() {
        // the smallest object that takes this path: four equal minimum-size ranges
        let m = PARALLEL_HTTP_MIN_RANGE;
        assert_eq!(
            plan_ranges(PARALLEL_HTTP_MIN_SIZE),
            vec![(0, m), (m, 2 * m), (2 * m, 3 * m), (3 * m, 4 * m)]
        );
        // large object: capped stream count, contiguous disjoint ranges covering everything
        let size = 773_082_315;
        let ranges = plan_ranges(size);
        assert!(ranges.len() > 2 && ranges.len() <= PARALLEL_HTTP_MAX_RANGES as usize);
        assert_eq!(ranges.first().map(|r| r.0), Some(0));
        assert_eq!(ranges.last().map(|r| r.1), Some(size));
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].1, pair[1].0, "ranges must be contiguous and disjoint");
        }
    }

    #[test]
    fn test_verify_download_rejects_corruption() {
        let dir = std::env::temp_dir();
        let path = dir.join("git-xet-verify-test");
        let data = b"openpilot";
        let oid = "b0e4f4a4b6e2d4d1d49a92e2d5b0b2a1e4d3c2b1a0f9e8d7c6b5a4938271605f";
        std::fs::write(&path, data).unwrap();
        // wrong size fails first
        assert!(verify_download(&path, oid, data.len() as u64 + 1).is_err());
        // correct size but wrong hash fails
        assert!(verify_download(&path, oid, data.len() as u64).is_err());
        std::fs::remove_file(&path).ok();
    }
}