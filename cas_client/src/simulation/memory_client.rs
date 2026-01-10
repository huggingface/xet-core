use std::collections::HashMap;
use std::io::{BufReader, Cursor};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use cas_object::{CasObject, SerializedCasObject};
#[cfg(not(target_family = "wasm"))]
use cas_types::{
    BatchQueryReconstructionResponse, CASReconstructionFetchInfo, CASReconstructionTerm, ChunkRange, FileRange,
    HexMerkleHash, HttpRange, QueryReconstructionResponse,
};
#[cfg(target_family = "wasm")]
use cas_types::{ChunkRange, FileRange, HttpRange};
use mdb_shard::MDBShardInfo;
use mdb_shard::file_structs::MDBFileInfo;
use mdb_shard::shard_in_memory::MDBInMemoryShard;
use merklehash::MerkleHash;
#[cfg(not(target_family = "wasm"))]
use more_asserts::{assert_ge, assert_gt, debug_assert_lt};
#[cfg(not(target_family = "wasm"))]
use progress_tracking::item_tracking::SingleItemProgressUpdater;
use progress_tracking::upload_tracking::CompletionTracker;
use rand::Rng;
#[cfg(not(target_family = "wasm"))]
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};
use tracing::{error, info};

use super::direct_access_client::DirectAccessClient;
use crate::Client;
use crate::adaptive_concurrency::AdaptiveConcurrencyController;
#[cfg(not(target_family = "wasm"))]
use crate::download_utils::TermDownloadOutput;
use crate::error::{CasClientError, Result};
#[cfg(not(target_family = "wasm"))]
use crate::{SeekingOutputProvider, SequentialOutput};

lazy_static::lazy_static! {
    /// Reference instant for URL timestamps. Initialized far in the past to allow
    /// testing timestamps that are earlier in the current process lifetime.
    static ref REFERENCE_INSTANT: Instant = {
        let now = Instant::now();
        now.checked_sub(Duration::from_secs(365 * 24 * 60 * 60))
            .unwrap_or(now)
    };
}

/// Stored XORB data: the serialized data and the deserialized CasObject (header/footer).
struct XorbEntry {
    serialized_data: Bytes,
    cas_object: CasObject,
}

/// In-memory client for testing purposes. Stores all data in memory using hash tables.
pub struct MemoryClient {
    /// XORBs stored by hash
    xorbs: RwLock<HashMap<MerkleHash, XorbEntry>>,
    /// In-memory shard for file reconstruction info
    shard: RwLock<MDBInMemoryShard>,
    /// Global dedup lookup: chunk_hash -> shard bytes
    global_dedup: RwLock<HashMap<MerkleHash, Bytes>>,
    /// Upload concurrency controller
    upload_concurrency_controller: Arc<AdaptiveConcurrencyController>,
    /// URL expiration in milliseconds
    url_expiration_ms: AtomicU64,
    /// API delay range in milliseconds as (min_ms, max_ms). (0, 0) means disabled.
    random_ms_delay_window: (AtomicU64, AtomicU64),
}

impl MemoryClient {
    /// Create a new in-memory client.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            xorbs: RwLock::new(HashMap::new()),
            shard: RwLock::new(MDBInMemoryShard::default()),
            global_dedup: RwLock::new(HashMap::new()),
            upload_concurrency_controller: AdaptiveConcurrencyController::new_upload("memory_uploads"),
            url_expiration_ms: AtomicU64::new(u64::MAX),
            random_ms_delay_window: (AtomicU64::new(0), AtomicU64::new(0)),
        })
    }

    /// Read the footer info from a stored XORB.
    fn read_xorb_footer_from_entry(entry: &XorbEntry) -> &CasObject {
        &entry.cas_object
    }

    /// Applies the configured API delay if set.
    async fn apply_api_delay(&self) {
        let min_ms = self.random_ms_delay_window.0.load(Ordering::Relaxed);
        let max_ms = self.random_ms_delay_window.1.load(Ordering::Relaxed);

        if min_ms == 0 && max_ms == 0 {
            return;
        }

        let delay_ms = if min_ms == max_ms {
            min_ms
        } else {
            rand::rng().random_range(min_ms..max_ms)
        };

        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

impl Default for MemoryClient {
    fn default() -> Self {
        Self {
            xorbs: RwLock::new(HashMap::new()),
            shard: RwLock::new(MDBInMemoryShard::default()),
            global_dedup: RwLock::new(HashMap::new()),
            upload_concurrency_controller: AdaptiveConcurrencyController::new_upload("memory_uploads"),
            url_expiration_ms: AtomicU64::new(u64::MAX),
            random_ms_delay_window: (AtomicU64::new(0), AtomicU64::new(0)),
        }
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
impl DirectAccessClient for MemoryClient {
    fn set_fetch_term_url_expiration(&self, expiration: Duration) {
        self.url_expiration_ms.store(expiration.as_millis() as u64, Ordering::Relaxed);
    }

    fn set_api_delay_range(&self, delay_range: Option<Range<Duration>>) {
        match delay_range {
            Some(range) => {
                self.random_ms_delay_window
                    .0
                    .store(range.start.as_millis() as u64, Ordering::Relaxed);
                self.random_ms_delay_window
                    .1
                    .store(range.end.as_millis() as u64, Ordering::Relaxed);
            },
            None => {
                self.random_ms_delay_window.0.store(0, Ordering::Relaxed);
                self.random_ms_delay_window.1.store(0, Ordering::Relaxed);
            },
        }
    }

    async fn list_xorbs(&self) -> Result<Vec<MerkleHash>> {
        Ok(self.xorbs.read().await.keys().copied().collect())
    }

    async fn delete_xorb(&self, hash: &MerkleHash) {
        self.xorbs.write().await.remove(hash);
    }

    async fn get_full_xorb(&self, hash: &MerkleHash) -> Result<Bytes> {
        let xorbs = self.xorbs.read().await;
        let entry = xorbs.get(hash).ok_or_else(|| {
            error!("Unable to find xorb in memory CAS {:?}", hash);
            CasClientError::XORBNotFound(*hash)
        })?;

        let mut reader = BufReader::new(Cursor::new(&entry.serialized_data));
        let cas = CasObject::deserialize(&mut reader)?;
        let result = cas.get_all_bytes(&mut reader)?;
        Ok(Bytes::from(result))
    }

    async fn get_xorb_ranges(&self, hash: &MerkleHash, chunk_ranges: Vec<(u32, u32)>) -> Result<Vec<Bytes>> {
        if chunk_ranges.is_empty() {
            return Ok(vec![Bytes::new()]);
        }

        let xorbs = self.xorbs.read().await;
        let entry = xorbs.get(hash).ok_or_else(|| {
            error!("Unable to find xorb in memory CAS {:?}", hash);
            CasClientError::XORBNotFound(*hash)
        })?;

        let mut reader = BufReader::new(Cursor::new(&entry.serialized_data));
        let cas = CasObject::deserialize(&mut reader)?;

        let mut ret: Vec<Bytes> = Vec::new();
        for r in chunk_ranges {
            if r.0 >= r.1 {
                ret.push(Bytes::new());
                continue;
            }
            let data = cas.get_bytes_by_chunk_range(&mut reader, r.0, r.1)?;
            ret.push(Bytes::from(data));
        }
        Ok(ret)
    }

    async fn xorb_length(&self, hash: &MerkleHash) -> Result<u32> {
        let data = self.get_full_xorb(hash).await?;
        Ok(data.len() as u32)
    }

    async fn xorb_exists(&self, hash: &MerkleHash) -> Result<bool> {
        Ok(self.xorbs.read().await.contains_key(hash))
    }

    async fn xorb_footer(&self, hash: &MerkleHash) -> Result<CasObject> {
        let xorbs = self.xorbs.read().await;
        let entry = xorbs.get(hash).ok_or_else(|| {
            error!("Unable to find xorb in memory CAS {:?}", hash);
            CasClientError::XORBNotFound(*hash)
        })?;
        Ok(entry.cas_object.clone())
    }

    async fn get_file_size(&self, hash: &MerkleHash) -> Result<u64> {
        let shard = self.shard.read().await;
        let file_info = shard
            .get_file_reconstruction_info(hash)
            .ok_or(CasClientError::FileNotFound(*hash))?;
        Ok(file_info.file_size())
    }

    async fn get_file_data(&self, hash: &MerkleHash, byte_range: Option<FileRange>) -> Result<Bytes> {
        let file_info = {
            let shard = self.shard.read().await;
            shard
                .get_file_reconstruction_info(hash)
                .ok_or(CasClientError::FileNotFound(*hash))?
        };

        let mut file_vec = Vec::new();
        for entry in &file_info.segments {
            let entry_bytes = self
                .get_xorb_ranges(&entry.cas_hash, vec![(entry.chunk_index_start, entry.chunk_index_end)])
                .await?
                .pop()
                .unwrap();
            file_vec.extend_from_slice(&entry_bytes);
        }

        let file_size = file_vec.len();

        let start = byte_range.as_ref().map(|range| range.start as usize).unwrap_or(0);

        if byte_range.is_some() && start >= file_size {
            return Err(CasClientError::InvalidRange);
        }

        let end = byte_range
            .as_ref()
            .map(|range| range.end as usize)
            .unwrap_or(file_size)
            .min(file_size);

        Ok(Bytes::from(file_vec[start..end].to_vec()))
    }

    async fn get_xorb_raw_bytes(&self, hash: &MerkleHash, byte_range: Option<FileRange>) -> Result<Bytes> {
        let xorbs = self.xorbs.read().await;
        let entry = xorbs.get(hash).ok_or(CasClientError::XORBNotFound(*hash))?;
        let data = &entry.serialized_data;

        let start = byte_range.as_ref().map(|r| r.start as usize).unwrap_or(0);
        let end = byte_range
            .as_ref()
            .map(|r| r.end as usize)
            .unwrap_or(data.len())
            .min(data.len());

        if start >= data.len() {
            return Err(CasClientError::InvalidRange);
        }

        Ok(data.slice(start..end))
    }

    async fn xorb_raw_length(&self, hash: &MerkleHash) -> Result<u64> {
        let xorbs = self.xorbs.read().await;
        let entry = xorbs.get(hash).ok_or(CasClientError::XORBNotFound(*hash))?;
        Ok(entry.serialized_data.len() as u64)
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
impl Client for MemoryClient {
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        self.apply_api_delay().await;
        let shard = self.shard.read().await;
        Ok(shard.get_file_reconstruction_info(file_hash).map(|fi| (fi, None)))
    }

    async fn query_for_global_dedup_shard(&self, _prefix: &str, chunk_hash: &MerkleHash) -> Result<Option<Bytes>> {
        self.apply_api_delay().await;
        let dedup = self.global_dedup.read().await;
        Ok(dedup.get(chunk_hash).cloned())
    }

    async fn acquire_upload_permit(&self) -> Result<crate::adaptive_concurrency::ConnectionPermit> {
        self.apply_api_delay().await;
        self.upload_concurrency_controller.acquire_connection_permit().await
    }

    async fn upload_shard(
        &self,
        shard_data: Bytes,
        _permit: crate::adaptive_concurrency::ConnectionPermit,
    ) -> Result<bool> {
        self.apply_api_delay().await;
        // Parse the shard info from bytes
        let mut reader = Cursor::new(&shard_data);
        let shard_info = MDBShardInfo::load_from_reader(&mut reader)?;

        // Merge file info into our shard
        {
            let mut shard_lg = self.shard.write().await;
            reader.set_position(0);
            for file_info in shard_info.read_all_file_info_sections(&mut reader)? {
                shard_lg.add_file_reconstruction_info(file_info)?;
            }
            reader.set_position(0);
            for cas_info in shard_info.read_all_cas_blocks_full(&mut reader)? {
                shard_lg.add_cas_block(cas_info)?;
            }
        }

        // Update global dedup lookup
        let mut shard_reader = Cursor::new(&shard_data);
        let chunk_hashes = MDBShardInfo::filter_cas_chunks_for_global_dedup(&mut shard_reader)?;

        {
            let mut dedup_lg = self.global_dedup.write().await;
            for chunk in chunk_hashes {
                dedup_lg.insert(chunk, shard_data.clone());
            }
        }

        Ok(true)
    }

    async fn upload_xorb(
        &self,
        _prefix: &str,
        serialized_cas_object: SerializedCasObject,
        upload_tracker: Option<Arc<CompletionTracker>>,
        _permit: crate::adaptive_concurrency::ConnectionPermit,
    ) -> Result<u64> {
        self.apply_api_delay().await;
        let hash = serialized_cas_object.hash;

        // Check if already exists
        {
            let xorbs = self.xorbs.read().await;
            if xorbs.contains_key(&hash) {
                info!("object {hash:?} already exists in Memory CAS; returning.");
                return Ok(0);
            }
        }

        info!("Storing XORB {hash:?} in memory");

        // Deserialize the CasObject for later use
        let mut reader = BufReader::new(Cursor::new(&serialized_cas_object.serialized_data));
        let cas_object = CasObject::deserialize(&mut reader)?;

        let bytes_written = serialized_cas_object.serialized_data.len();

        // Store the xorb
        {
            let mut xorbs = self.xorbs.write().await;
            xorbs.insert(
                hash,
                XorbEntry {
                    serialized_data: Bytes::from(serialized_cas_object.serialized_data),
                    cas_object,
                },
            );
        }

        // Update progress tracker
        if let Some(tracker) = upload_tracker {
            tracker
                .register_xorb_upload_progress(hash, serialized_cas_object.raw_num_bytes)
                .await;
        }

        info!("XORB {hash:?} successfully stored with {bytes_written} bytes.");

        Ok(bytes_written as u64)
    }

    fn use_xorb_footer(&self) -> bool {
        true
    }

    fn use_shard_footer(&self) -> bool {
        true
    }

    #[cfg(not(target_family = "wasm"))]
    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>> {
        self.apply_api_delay().await;
        let file_info = {
            let shard = self.shard.read().await;
            match shard.get_file_reconstruction_info(file_id) {
                Some(fi) => fi,
                None => return Ok(None),
            }
        };

        let total_file_size: u64 = file_info.file_size();

        // Handle range validation and truncation
        let file_range = if let Some(range) = bytes_range {
            // If the entire range is out of bounds, return None (like RemoteClient does for 416)
            if range.start >= total_file_size {
                // For empty files (size 0), only the first query (start == 0) should return the empty reconstruction
                // All subsequent queries should return None to prevent infinite remainder loops
                if total_file_size == 0 && range.start == 0 {
                    // Empty file - return valid but empty reconstruction
                    return Ok(Some(QueryReconstructionResponse {
                        offset_into_first_range: 0,
                        terms: vec![],
                        fetch_info: HashMap::new(),
                    }));
                }
                return Ok(None);
            }
            FileRange::new(range.start, range.end.min(total_file_size))
        } else {
            // No range specified - handle empty files
            if total_file_size == 0 {
                return Ok(Some(QueryReconstructionResponse {
                    offset_into_first_range: 0,
                    terms: vec![],
                    fetch_info: HashMap::new(),
                }));
            }
            FileRange::full()
        };

        // Find the first segment that contains bytes in our range
        let mut s_idx = 0;
        let mut cumulative_bytes = 0u64;
        let mut first_chunk_byte_start;

        loop {
            if s_idx >= file_info.segments.len() {
                return Err(CasClientError::InvalidRange);
            }

            let n = file_info.segments[s_idx].unpacked_segment_bytes as u64;
            if cumulative_bytes + n > file_range.start {
                assert_ge!(file_range.start, cumulative_bytes);
                first_chunk_byte_start = cumulative_bytes;
                break;
            } else {
                cumulative_bytes += n;
                s_idx += 1;
            }
        }

        let mut terms = Vec::new();

        #[derive(Clone)]
        struct FetchInfoIntermediate {
            chunk_range: ChunkRange,
            byte_range: FileRange,
        }

        let mut fetch_info_map: HashMap<MerkleHash, Vec<FetchInfoIntermediate>> = HashMap::new();

        let xorbs = self.xorbs.read().await;

        while s_idx < file_info.segments.len() && cumulative_bytes < file_range.end {
            let mut segment = file_info.segments[s_idx].clone();
            let mut chunk_range = ChunkRange::new(segment.chunk_index_start, segment.chunk_index_end);

            let entry = xorbs.get(&segment.cas_hash).ok_or_else(|| {
                error!("Unable to find xorb in memory CAS {:?}", segment.cas_hash);
                CasClientError::XORBNotFound(segment.cas_hash)
            })?;
            let xorb_footer = Self::read_xorb_footer_from_entry(entry);

            // Prune first segment on chunk boundaries
            if cumulative_bytes < file_range.start {
                while chunk_range.start < chunk_range.end {
                    let next_chunk_size = xorb_footer.uncompressed_chunk_length(chunk_range.start)? as u64;

                    if cumulative_bytes + next_chunk_size <= file_range.start {
                        cumulative_bytes += next_chunk_size;
                        first_chunk_byte_start += next_chunk_size;
                        segment.unpacked_segment_bytes -= next_chunk_size as u32;
                        chunk_range.start += 1;
                        debug_assert_lt!(chunk_range.start, chunk_range.end);
                    } else {
                        break;
                    }
                }
            }

            // Prune last segment on chunk boundaries
            if cumulative_bytes + segment.unpacked_segment_bytes as u64 > file_range.end {
                while chunk_range.end > chunk_range.start {
                    let last_chunk_size = xorb_footer.uncompressed_chunk_length(chunk_range.end - 1)?;

                    if cumulative_bytes + (segment.unpacked_segment_bytes - last_chunk_size) as u64 >= file_range.end {
                        chunk_range.end -= 1;
                        segment.unpacked_segment_bytes -= last_chunk_size;
                        debug_assert_lt!(chunk_range.start, chunk_range.end);
                        assert_gt!(segment.unpacked_segment_bytes, 0);
                    } else {
                        break;
                    }
                }
            }

            let (byte_start, byte_end) = xorb_footer.get_byte_offset(chunk_range.start, chunk_range.end)?;
            let byte_range = FileRange::new(byte_start as u64, byte_end as u64);

            let cas_reconstruction_term = CASReconstructionTerm {
                hash: segment.cas_hash.into(),
                unpacked_length: segment.unpacked_segment_bytes,
                range: chunk_range,
            };

            terms.push(cas_reconstruction_term);

            let fetch_info_intermediate = FetchInfoIntermediate {
                chunk_range,
                byte_range,
            };

            fetch_info_map
                .entry(segment.cas_hash)
                .or_default()
                .push(fetch_info_intermediate);

            cumulative_bytes += segment.unpacked_segment_bytes as u64;
            s_idx += 1;
        }

        assert!(!terms.is_empty());

        let timestamp = Instant::now();

        // Sort and merge adjacent/overlapping ranges in each fetch_info Vec
        let mut merged_fetch_info_map: HashMap<HexMerkleHash, Vec<CASReconstructionFetchInfo>> = HashMap::new();
        for (hash, mut fi_vec) in fetch_info_map {
            fi_vec.sort_by_key(|fi| fi.chunk_range.start);

            let mut merged: Vec<CASReconstructionFetchInfo> = Vec::new();
            let mut idx = 0;

            while idx < fi_vec.len() {
                let mut new_fi = fi_vec[idx].clone();

                while idx + 1 < fi_vec.len() {
                    let next_fi = &fi_vec[idx + 1];
                    if next_fi.chunk_range.start <= new_fi.chunk_range.end {
                        new_fi.chunk_range.end = next_fi.chunk_range.end.max(new_fi.chunk_range.end);
                        new_fi.byte_range.end = next_fi.byte_range.end.max(new_fi.byte_range.end);
                        idx += 1;
                    } else {
                        break;
                    }
                }

                merged.push(CASReconstructionFetchInfo {
                    range: new_fi.chunk_range,
                    url: generate_fetch_url(&hash, &new_fi.byte_range, timestamp),
                    url_range: HttpRange::from(new_fi.byte_range),
                });

                idx += 1;
            }

            merged_fetch_info_map.insert(hash.into(), merged);
        }

        Ok(Some(QueryReconstructionResponse {
            offset_into_first_range: file_range.start - first_chunk_byte_start,
            terms,
            fetch_info: merged_fetch_info_map,
        }))
    }

    #[cfg(not(target_family = "wasm"))]
    async fn batch_get_reconstruction(&self, file_ids: &[MerkleHash]) -> Result<BatchQueryReconstructionResponse> {
        self.apply_api_delay().await;
        let mut files = HashMap::new();
        let mut fetch_info_map: HashMap<HexMerkleHash, Vec<CASReconstructionFetchInfo>> = HashMap::new();

        for file_id in file_ids {
            if let Some(response) = self.get_reconstruction(file_id, None).await? {
                let hex_hash: HexMerkleHash = (*file_id).into();
                files.insert(hex_hash, response.terms);

                for (hash, fetch_infos) in response.fetch_info {
                    fetch_info_map.entry(hash).or_default().extend(fetch_infos);
                }
            }
        }

        Ok(BatchQueryReconstructionResponse {
            files,
            fetch_info: fetch_info_map,
        })
    }

    #[cfg(not(target_family = "wasm"))]
    async fn get_file_term_data(
        &self,
        hash: MerkleHash,
        fetch_term: CASReconstructionFetchInfo,
    ) -> Result<TermDownloadOutput> {
        self.apply_api_delay().await;
        let (xorb_hash, url_byte_range, url_timestamp) = parse_fetch_url(&fetch_term.url)?;

        // Check if URL has expired
        let expiration_ms = self.url_expiration_ms.load(Ordering::Relaxed);
        let elapsed_ms = Instant::now().saturating_duration_since(url_timestamp).as_millis() as u64;
        if elapsed_ms > expiration_ms {
            return Err(CasClientError::PresignedUrlExpirationError);
        }

        // Validate byte range matches url_range
        // Note: url_byte_range is FileRange (exclusive end), url_range is HttpRange (inclusive end)
        // We convert url_range to FileRange for comparison
        let fetch_byte_range = FileRange::from(fetch_term.url_range);
        if url_byte_range.start != fetch_byte_range.start || url_byte_range.end != fetch_byte_range.end {
            return Err(CasClientError::InvalidArguments);
        }

        let xorbs = self.xorbs.read().await;
        let entry = xorbs.get(&xorb_hash).ok_or_else(|| {
            error!("Unable to find xorb in memory CAS {:?}", hash);
            CasClientError::XORBNotFound(hash)
        })?;

        let mut reader = BufReader::new(Cursor::new(&entry.serialized_data));
        let cas = CasObject::deserialize(&mut reader)?;

        let data = cas.get_bytes_by_chunk_range(&mut reader, fetch_term.range.start, fetch_term.range.end)?;

        let chunk_byte_indices = {
            let mut indices = Vec::new();
            let mut cumulative = 0u32;
            indices.push(0);
            for chunk_idx in fetch_term.range.start..fetch_term.range.end {
                let chunk_len = cas
                    .uncompressed_chunk_length(chunk_idx)
                    .map_err(|e| CasClientError::Other(format!("Failed to get chunk length: {e}")))?;
                cumulative += chunk_len;
                indices.push(cumulative);
            }
            indices
        };

        Ok(TermDownloadOutput {
            data: data.into(),
            chunk_byte_indices,
            chunk_range: fetch_term.range,
        })
    }

    #[cfg(not(target_family = "wasm"))]
    async fn get_file_with_sequential_writer(
        self: Arc<Self>,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        mut output_provider: SequentialOutput,
        _progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64> {
        self.apply_api_delay().await;
        let data = self.get_file_data(hash, byte_range).await?;
        let len = data.len() as u64;
        output_provider.write_all(&data).await?;
        Ok(len)
    }

    #[cfg(not(target_family = "wasm"))]
    async fn get_file_with_parallel_writer(
        self: Arc<Self>,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        output_provider: SeekingOutputProvider,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64> {
        self.apply_api_delay().await;
        let sequential = output_provider.try_into()?;
        self.get_file_with_sequential_writer(hash, byte_range, sequential, progress_updater)
            .await
    }
}

#[cfg(not(target_family = "wasm"))]
fn generate_fetch_url(hash: &MerkleHash, byte_range: &FileRange, timestamp: Instant) -> String {
    let timestamp_ms = timestamp.saturating_duration_since(*REFERENCE_INSTANT).as_millis() as u64;
    format!("{}:{}:{}:{}", hash.hex(), byte_range.start, byte_range.end, timestamp_ms)
}

#[cfg(not(target_family = "wasm"))]
fn parse_fetch_url(url: &str) -> Result<(MerkleHash, FileRange, Instant)> {
    let mut parts = url.rsplitn(4, ':').collect::<Vec<_>>();
    parts.reverse();

    if parts.len() != 4 {
        return Err(CasClientError::InvalidArguments);
    }

    let hash = MerkleHash::from_hex(parts[0]).map_err(|_| CasClientError::InvalidArguments)?;
    let start_pos: u64 = parts[1].parse().map_err(|_| CasClientError::InvalidArguments)?;
    let end_pos: u64 = parts[2].parse().map_err(|_| CasClientError::InvalidArguments)?;
    let timestamp_ms: u64 = parts[3].parse().map_err(|_| CasClientError::InvalidArguments)?;

    let byte_range = FileRange::new(start_pos, end_pos);
    let timestamp = *REFERENCE_INSTANT + Duration::from_millis(timestamp_ms);

    Ok((hash, byte_range, timestamp))
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use super::*;

    /// Runs the common TestingClient trait test suite for MemoryClient.
    #[tokio::test]
    async fn test_common_client_suite() {
        super::super::client_unit_testing::test_client_functionality(|| async {
            MemoryClient::new() as std::sync::Arc<dyn super::super::DirectAccessClient>
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_url_expiration() {
        super::super::client_unit_testing::test_url_expiration_functionality(|| async {
            MemoryClient::new() as std::sync::Arc<dyn super::super::DirectAccessClient>
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_api_delay() {
        super::super::client_unit_testing::test_api_delay_functionality(|| async {
            MemoryClient::new() as std::sync::Arc<dyn super::super::DirectAccessClient>
        })
        .await;
    }
}
