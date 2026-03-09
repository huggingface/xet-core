use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use cas_client::adaptive_concurrency::ConnectionPermit;
use cas_client::{Client, ProgressCallback, URLProvider};
use cas_types::{
    BatchQueryReconstructionResponse, FileRange, HexMerkleHash, QueryReconstructionResponse, XorbReconstructionTerm,
};
use mdb_shard::file_structs::MDBFileInfo;
use merklehash::MerkleHash;
use xorb_object::SerializedXorbObject;

type Result<T> = std::result::Result<T, cas_client::CasClientError>;

const MAX_CACHE_ENTRIES: usize = 1024;
/// Presigned URLs returned by the CAS server expire after 1 hour.
/// Evict cache entries just before expiry to avoid serving stale URLs.
const CACHE_TTL: Duration = Duration::from_secs(59 * 60);

struct CacheEntry {
    response: QueryReconstructionResponse,
    inserted_at: Instant,
}

impl CacheEntry {
    fn new(response: QueryReconstructionResponse) -> Self {
        Self {
            response,
            inserted_at: Instant::now(),
        }
    }

    fn is_valid(&self, ttl: Duration) -> bool {
        self.inserted_at.elapsed() < ttl
    }
}

/// A `Client` wrapper that caches full-file reconstruction plans and derives
/// range responses on the fly, avoiding repeated CAS roundtrips.
///
/// Adapted from hf-mount's `CachedXetClient`.
pub struct CachedXetClient {
    inner: Arc<dyn Client>,
    cache: Mutex<HashMap<MerkleHash, CacheEntry>>,
    ttl: Duration,
}

impl CachedXetClient {
    pub fn new(inner: Arc<dyn Client>) -> Arc<Self> {
        Self::new_with_ttl(inner, CACHE_TTL)
    }

    fn new_with_ttl(inner: Arc<dyn Client>, ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner,
            cache: Mutex::new(HashMap::new()),
            ttl,
        })
    }
}

/// Derive a range-scoped `QueryReconstructionResponse` from a cached full-file response.
///
/// Walks terms, tracks cumulative byte offsets, keeps only terms overlapping
/// `[range.start, range.end)`. Sets `offset_into_first_range` to the byte offset
/// within the first overlapping term where the requested range starts.
fn derive_range_response(full: &QueryReconstructionResponse, range: FileRange) -> QueryReconstructionResponse {
    let mut cur_offset: u64 = 0;
    let mut result_terms: Vec<XorbReconstructionTerm> = Vec::new();
    let mut offset_into_first: u64 = 0;
    let mut needed_hashes: std::collections::HashSet<HexMerkleHash> = std::collections::HashSet::new();

    for term in &full.terms {
        let term_start = cur_offset;
        let term_end = cur_offset + term.unpacked_length as u64;

        if term_end <= range.start {
            cur_offset = term_end;
            continue;
        }
        if term_start >= range.end {
            break;
        }

        if result_terms.is_empty() {
            offset_into_first = range.start.saturating_sub(term_start);
        }
        needed_hashes.insert(term.hash);
        result_terms.push(term.clone());
        cur_offset = term_end;
    }

    let fetch_info = full
        .fetch_info
        .iter()
        .filter(|(k, _)| needed_hashes.contains(*k))
        .map(|(k, v)| (*k, v.clone()))
        .collect();

    QueryReconstructionResponse {
        offset_into_first_range: offset_into_first,
        terms: result_terms,
        fetch_info,
    }
}

#[async_trait::async_trait]
impl Client for CachedXetClient {
    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>> {
        let cached_full = {
            let mut cache = self.cache.lock().expect("cache poisoned");
            match cache.get(file_id) {
                Some(entry) if entry.is_valid(self.ttl) => Some(entry.response.clone()),
                Some(_) => {
                    cache.remove(file_id);
                    None
                },
                None => None,
            }
        };

        if let Some(full) = cached_full {
            if let Some(range) = bytes_range {
                let resp = derive_range_response(&full, range);
                if resp.terms.is_empty() && (!full.terms.is_empty() || range.start > 0) {
                    return Ok(None);
                }
                tracing::debug!("recon: DERV file={:.8} range={:?} terms={}", file_id, bytes_range, resp.terms.len());
                return Ok(Some(resp));
            } else {
                tracing::debug!("recon: HIT  file={:.8} range=None terms={}", file_id, full.terms.len());
                return Ok(Some(full));
            }
        }

        tracing::debug!("recon: CAS  file={:.8} range={:?}", file_id, bytes_range);
        let result = self.inner.get_reconstruction(file_id, bytes_range).await?;

        if bytes_range.is_none()
            && let Some(response) = &result
        {
            let mut cache = self.cache.lock().expect("cache poisoned");
            if cache.len() >= MAX_CACHE_ENTRIES {
                cache.clear();
            }
            cache.insert(*file_id, CacheEntry::new(response.clone()));
        }

        Ok(result)
    }

    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        self.inner.get_file_reconstruction_info(file_hash).await
    }

    async fn batch_get_reconstruction(&self, file_ids: &[MerkleHash]) -> Result<BatchQueryReconstructionResponse> {
        self.inner.batch_get_reconstruction(file_ids).await
    }

    async fn acquire_download_permit(&self) -> Result<ConnectionPermit> {
        self.inner.acquire_download_permit().await
    }

    async fn get_file_term_data(
        &self,
        url_info: Box<dyn URLProvider>,
        download_permit: ConnectionPermit,
        progress_callback: Option<ProgressCallback>,
        uncompressed_size_if_known: Option<usize>,
    ) -> Result<(Bytes, Vec<u32>)> {
        self.inner
            .get_file_term_data(url_info, download_permit, progress_callback, uncompressed_size_if_known)
            .await
    }

    async fn query_for_global_dedup_shard(&self, prefix: &str, chunk_hash: &MerkleHash) -> Result<Option<Bytes>> {
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
        serialized_cas_object: SerializedXorbObject,
        progress_callback: Option<ProgressCallback>,
        upload_permit: ConnectionPermit,
    ) -> Result<u64> {
        self.inner
            .upload_xorb(prefix, serialized_cas_object, progress_callback, upload_permit)
            .await
    }
}
