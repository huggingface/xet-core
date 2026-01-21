use std::collections::HashMap;
use std::io::{BufReader, Cursor};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use cas_object::{CasObject, SerializedCasObject};
use cas_types::{
    BatchQueryReconstructionResponse, CASReconstructionFetchInfo, CASReconstructionTerm, ChunkRange, FileRange,
    HexMerkleHash, HttpRange, QueryReconstructionResponse,
};
use mdb_shard::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfo};
use mdb_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use mdb_shard::shard_in_memory::MDBInMemoryShard;
use mdb_shard::streaming_shard::MDBMinimalShard;
use merklehash::MerkleHash;
use more_asserts::{assert_ge, assert_gt, debug_assert_lt};
use progress_tracking::upload_tracking::CompletionTracker;
use rand::Rng;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};
use tracing::{error, info};

use super::client_testing_utils::{FileTermReference, RandomFileContents};
use super::direct_access_client::DirectAccessClient;
use super::random_xorb::RandomXorb;
use crate::Client;
use crate::adaptive_concurrency::AdaptiveConcurrencyController;
use crate::error::{CasClientError, Result};

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
struct MaterializedXorb {
    serialized_data: Bytes,
    cas_object: CasObject,
}

/// Storage for a XORB - either fully materialized or generated on-the-fly.
enum XorbStorage {
    /// Fully materialized XORB data stored in memory.
    Materialized(MaterializedXorb),
    /// XORB data generated on-the-fly from random seeds.
    Random(RandomXorb),
}

/// In-memory client for testing purposes. Stores all data in memory using hash tables.
pub struct MemoryClient {
    /// XORBs stored by hash
    xorbs: RwLock<HashMap<MerkleHash, XorbStorage>>,
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

    /// Inserts a RandomXorb into the client's storage and registers it in the shard
    /// for CAS block lookups. Returns the xorb hash.
    ///
    /// This allows storing XORBs that generate their data on-the-fly,
    /// enabling testing with massive files without storing all data in memory.
    pub async fn insert_random_xorb(&self, xorb: RandomXorb) -> Result<MerkleHash> {
        use mdb_shard::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfo};

        let hash = xorb.xorb_hash();
        let cas = xorb.get_cas_object();

        // Create CAS block entries for each chunk
        let mut chunk_entries = Vec::with_capacity(xorb.num_chunks() as usize);
        let mut byte_offset = 0u32;

        for i in 0..xorb.num_chunks() {
            let chunk_size = xorb.chunk_size(i).unwrap();
            chunk_entries.push(CASChunkSequenceEntry {
                chunk_hash: xorb.chunk_hash(i).unwrap(),
                chunk_byte_range_start: byte_offset,
                unpacked_segment_bytes: chunk_size,
                flags: 0,
                _unused: 0,
            });
            byte_offset += chunk_size;
        }

        let cas_info = MDBCASInfo {
            metadata: CASChunkSequenceHeader::new(
                hash,
                xorb.num_chunks(),
                cas.info.unpacked_chunk_offsets.last().copied().unwrap_or(0),
            ),
            chunks: chunk_entries,
        };

        {
            let mut shard = self.shard.write().await;
            shard.add_cas_block(cas_info)?;
        }

        self.xorbs.write().await.insert(hash, XorbStorage::Random(xorb));
        Ok(hash)
    }

    /// Upload a random file using lazy generation with RandomXorb.
    ///
    /// This uses `RandomXorb` to generate XORB data on-the-fly, allowing testing
    /// with massive files without storing all data in memory.
    ///
    /// Each term is defined as `(xorb_seed, (chunk_start, chunk_end))` where:
    /// - `xorb_seed` determines the random data for that XORB
    /// - `chunk_start` and `chunk_end` define the range of chunks to include
    pub async fn insert_random_lazy_file(
        &self,
        term_spec: &[(u64, (u64, u64))],
        chunk_size: usize,
    ) -> Result<RandomFileContents> {
        use mdb_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};

        // Collect max chunk count needed for each xorb seed
        let mut xorb_num_chunks = std::collections::HashMap::<u64, u64>::new();
        for &(xorb_seed, (_, chunk_idx_end)) in term_spec {
            let c = xorb_num_chunks.entry(xorb_seed).or_default();
            *c = (*c).max(chunk_idx_end);
        }

        // Create and register RandomXorbs
        let mut random_xorbs = std::collections::HashMap::<u64, RandomXorb>::new();
        for (&xorb_seed, &n_chunks) in &xorb_num_chunks {
            let xorb = RandomXorb::from_seed(xorb_seed, n_chunks as u32, chunk_size as u32);
            self.insert_random_xorb(xorb.clone()).await?;
            random_xorbs.insert(xorb_seed, xorb);
        }

        // Build file info from terms
        let mut file_segments = Vec::new();
        let mut chunk_file_hashes = Vec::new();
        let mut term_infos = Vec::new();
        let mut file_data = Vec::new();

        for &(xorb_seed, (chunk_start, chunk_end)) in term_spec {
            let xorb = random_xorbs.get(&xorb_seed).unwrap();
            let (c_start, c_end) = (chunk_start as u32, chunk_end as u32);

            chunk_file_hashes.extend(xorb.chunk_hash_sizes(c_start, c_end));

            let term_data = xorb.get_chunk_range_data(c_start, c_end).unwrap_or_default();
            file_data.extend_from_slice(&term_data);

            file_segments.push(FileDataSequenceEntry::new(
                xorb.xorb_hash(),
                xorb.chunk_range_size(c_start, c_end) as usize,
                chunk_start as usize,
                chunk_end as usize,
            ));

            term_infos.push(FileTermReference {
                xorb_hash: xorb.xorb_hash(),
                chunk_start: c_start,
                chunk_end: c_end,
                data: term_data,
                chunk_hashes: xorb.chunk_hashes_range(c_start, c_end),
            });
        }

        let file_hash = merklehash::file_hash_with_salt(&chunk_file_hashes, &[0; 32]);

        // Add file reconstruction info to shard
        {
            let mut shard = self.shard.write().await;
            shard.add_file_reconstruction_info(MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, file_segments.len(), false, false),
                segments: file_segments,
                verification: vec![],
                metadata_ext: None,
            })?;
        }

        Ok(RandomFileContents {
            file_hash,
            data: Bytes::from(file_data),
            xorbs: std::collections::HashMap::new(),
            terms: term_infos,
        })
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
        let storage = xorbs.get(hash).ok_or_else(|| {
            error!("Unable to find xorb in memory CAS {:?}", hash);
            CasClientError::XORBNotFound(*hash)
        })?;

        match storage {
            XorbStorage::Materialized(entry) => {
                let mut reader = BufReader::new(Cursor::new(&entry.serialized_data));
                let cas = CasObject::deserialize(&mut reader)?;
                let result = cas.get_all_bytes(&mut reader)?;
                Ok(Bytes::from(result))
            },
            XorbStorage::Random(xorb) => xorb
                .get_chunk_range_data(0, xorb.num_chunks())
                .ok_or(CasClientError::XORBNotFound(*hash)),
        }
    }

    async fn get_xorb_ranges(&self, hash: &MerkleHash, chunk_ranges: Vec<(u32, u32)>) -> Result<Vec<Bytes>> {
        if chunk_ranges.is_empty() {
            return Ok(vec![Bytes::new()]);
        }

        let xorbs = self.xorbs.read().await;
        let storage = xorbs.get(hash).ok_or_else(|| {
            error!("Unable to find xorb in memory CAS {:?}", hash);
            CasClientError::XORBNotFound(*hash)
        })?;

        match storage {
            XorbStorage::Materialized(entry) => {
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
            },
            XorbStorage::Random(xorb) => {
                let mut ret: Vec<Bytes> = Vec::new();
                for r in chunk_ranges {
                    if r.0 >= r.1 {
                        ret.push(Bytes::new());
                        continue;
                    }
                    let data = xorb.get_chunk_range_data(r.0, r.1).ok_or(CasClientError::XORBNotFound(*hash))?;
                    ret.push(data);
                }
                Ok(ret)
            },
        }
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
        let storage = xorbs.get(hash).ok_or_else(|| {
            error!("Unable to find xorb in memory CAS {:?}", hash);
            CasClientError::XORBNotFound(*hash)
        })?;

        match storage {
            XorbStorage::Materialized(entry) => Ok(entry.cas_object.clone()),
            XorbStorage::Random(xorb) => Ok(xorb.get_cas_object()),
        }
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
        let storage = xorbs.get(hash).ok_or(CasClientError::XORBNotFound(*hash))?;

        match storage {
            XorbStorage::Materialized(entry) => {
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
            },
            XorbStorage::Random(xorb) => {
                let total_len = xorb.serialized_length();
                let start = byte_range.as_ref().map(|r| r.start).unwrap_or(0);
                let end = byte_range.as_ref().map(|r| r.end).unwrap_or(total_len).min(total_len);

                if start >= total_len {
                    return Err(CasClientError::InvalidRange);
                }

                Ok(xorb.get_serialized_range(start, end))
            },
        }
    }

    async fn xorb_raw_length(&self, hash: &MerkleHash) -> Result<u64> {
        let xorbs = self.xorbs.read().await;
        let storage = xorbs.get(hash).ok_or(CasClientError::XORBNotFound(*hash))?;

        match storage {
            XorbStorage::Materialized(entry) => Ok(entry.serialized_data.len() as u64),
            XorbStorage::Random(xorb) => Ok(xorb.serialized_length()),
        }
    }

    async fn fetch_term_data(
        &self,
        hash: MerkleHash,
        fetch_term: CASReconstructionFetchInfo,
    ) -> Result<(Bytes, Vec<u32>)> {
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
        let storage = xorbs.get(&xorb_hash).ok_or_else(|| {
            error!("Unable to find xorb in memory CAS {:?}", hash);
            CasClientError::XORBNotFound(hash)
        })?;

        let (data, cas) = match storage {
            XorbStorage::Materialized(entry) => {
                let mut reader = BufReader::new(Cursor::new(&entry.serialized_data));
                let cas = CasObject::deserialize(&mut reader)?;
                let data = cas.get_bytes_by_chunk_range(&mut reader, fetch_term.range.start, fetch_term.range.end)?;
                (Bytes::from(data), cas)
            },
            XorbStorage::Random(xorb) => {
                let data = xorb
                    .get_chunk_range_data(fetch_term.range.start, fetch_term.range.end)
                    .ok_or(CasClientError::XORBNotFound(hash))?;
                let cas = xorb.get_cas_object();
                (data, cas)
            },
        };

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

        Ok((data, chunk_byte_indices))
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
        // Parse the shard using the streaming parser (handles shards without footer)
        let mut reader = Cursor::new(&shard_data);
        let minimal_shard = MDBMinimalShard::from_reader(&mut reader, true, true)?;

        // Merge file info into our shard
        {
            let mut shard_lg = self.shard.write().await;

            // Convert file views to MDBFileInfo and add to shard
            for i in 0..minimal_shard.num_files() {
                let file_view = minimal_shard.file(i).unwrap();
                let segments: Vec<FileDataSequenceEntry> =
                    (0..file_view.num_entries()).map(|j| file_view.entry(j)).collect();
                let verification = if file_view.contains_verification() {
                    (0..file_view.num_entries()).map(|j| file_view.verification(j)).collect()
                } else {
                    vec![]
                };
                let file_info = MDBFileInfo {
                    metadata: FileDataSequenceHeader::new(
                        file_view.file_hash(),
                        segments.len(),
                        file_view.contains_verification(),
                        file_view.contains_metadata_ext(),
                    ),
                    segments,
                    verification,
                    metadata_ext: None,
                };
                shard_lg.add_file_reconstruction_info(file_info)?;
            }

            // Convert CAS views to MDBCASInfo and add to shard
            for i in 0..minimal_shard.num_cas() {
                let cas_view = minimal_shard.cas(i).unwrap();
                let chunks: Vec<CASChunkSequenceEntry> =
                    (0..cas_view.num_entries()).map(|j| cas_view.chunk(j)).collect();
                let total_bytes = chunks
                    .last()
                    .map(|c| c.chunk_byte_range_start + c.unpacked_segment_bytes)
                    .unwrap_or(0);
                let cas_info = MDBCASInfo {
                    metadata: CASChunkSequenceHeader::new(cas_view.cas_hash(), chunks.len() as u32, total_bytes),
                    chunks,
                };
                shard_lg.add_cas_block(cas_info)?;
            }
        }

        // Update global dedup lookup using the minimal shard's method
        let chunk_hashes = minimal_shard.global_dedup_eligible_chunks();

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
        let raw_num_bytes = serialized_cas_object.raw_num_bytes;
        let footer_start = serialized_cas_object.footer_start;
        let serialized_data = serialized_cas_object.serialized_data;

        // Check if already exists
        {
            let xorbs = self.xorbs.read().await;
            if xorbs.contains_key(&hash) {
                info!("object {hash:?} already exists in Memory CAS; returning.");
                return Ok(0);
            }
        }

        info!("Storing XORB {hash:?} in memory");

        // Reconstruct CasObject from chunk data if no footer, or deserialize if footer present
        let (cas_object, serialized_data) = if footer_start.is_some() {
            // Data has footer - deserialize directly
            let mut reader = Cursor::new(&serialized_data);
            let cas_object = CasObject::deserialize(&mut reader)?;
            (cas_object, serialized_data)
        } else {
            // No footer - reconstruct CasObject from chunk data and append footer
            let mut data_with_footer = Vec::new();
            let (cas_object, computed_hash) = cas_object::reconstruct_xorb_with_footer(
                &mut data_with_footer,
                &serialized_data,
            )?;
            if computed_hash != hash {
                return Err(CasClientError::Other(format!(
                    "XORB hash mismatch: expected {}, got {}",
                    hash.hex(),
                    computed_hash.hex(),
                )));
            }
            (cas_object, data_with_footer)
        };

        let bytes_written = serialized_data.len();

        // Store the xorb
        {
            let mut xorbs = self.xorbs.write().await;
            xorbs.insert(
                hash,
                XorbStorage::Materialized(MaterializedXorb {
                    serialized_data: Bytes::from(serialized_data),
                    cas_object,
                }),
            );
        }

        // Update progress tracker
        if let Some(tracker) = upload_tracker {
            tracker.register_xorb_upload_progress(hash, raw_num_bytes).await;
        }

        info!("XORB {hash:?} successfully stored with {bytes_written} bytes.");

        Ok(bytes_written as u64)
    }

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

            let storage = xorbs.get(&segment.cas_hash).ok_or_else(|| {
                error!("Unable to find xorb in memory CAS {:?}", segment.cas_hash);
                CasClientError::XORBNotFound(segment.cas_hash)
            })?;
            let xorb_footer = match storage {
                XorbStorage::Materialized(entry) => entry.cas_object.clone(),
                XorbStorage::Random(xorb) => xorb.get_cas_object(),
            };

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

    async fn acquire_download_permit(&self) -> Result<crate::adaptive_concurrency::ConnectionPermit> {
        self.apply_api_delay().await;
        self.upload_concurrency_controller.acquire_connection_permit().await
    }

    async fn get_file_term_data(
        &self,
        url_info: Box<dyn crate::URLProvider>,
        _download_permit: crate::adaptive_concurrency::ConnectionPermit,
    ) -> Result<(Bytes, Vec<u32>)> {
        self.apply_api_delay().await;
        let (url, range) = url_info.retrieve_url().await?;
        let (xorb_hash, _url_byte_range, url_timestamp) = parse_fetch_url(&url)?;

        // Check if URL has expired
        let expiration_ms = self.url_expiration_ms.load(Ordering::Relaxed);
        let elapsed_ms = Instant::now().saturating_duration_since(url_timestamp).as_millis() as u64;
        if elapsed_ms > expiration_ms {
            return Err(CasClientError::PresignedUrlExpirationError);
        }

        let xorbs = self.xorbs.read().await;
        let storage = xorbs.get(&xorb_hash).ok_or(CasClientError::XORBNotFound(xorb_hash))?;

        // Extract the byte range from the serialized data and deserialize
        let start = range.start as usize;
        let end = range.end as usize + 1; // HttpRange is inclusive end

        match storage {
            XorbStorage::Materialized(entry) => {
                let range_data = &entry.serialized_data[start..end];
                let (decompressed_data, chunk_byte_indices) =
                    cas_object::deserialize_chunks(&mut Cursor::new(range_data))?;
                Ok((Bytes::from(decompressed_data), chunk_byte_indices))
            },
            XorbStorage::Random(xorb) => {
                let range_data = xorb.get_serialized_range(start as u64, end as u64);
                let (decompressed_data, chunk_byte_indices) =
                    cas_object::deserialize_chunks(&mut Cursor::new(range_data.as_ref()))?;
                Ok((Bytes::from(decompressed_data), chunk_byte_indices))
            },
        }
    }
}

fn generate_fetch_url(hash: &MerkleHash, byte_range: &FileRange, timestamp: Instant) -> String {
    let timestamp_ms = timestamp.saturating_duration_since(*REFERENCE_INSTANT).as_millis() as u64;
    format!("{}:{}:{}:{}", hash.hex(), byte_range.start, byte_range.end, timestamp_ms)
}

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
    use super::super::client_testing_utils::ClientTestingUtils;
    use super::*;

    fn new_client() -> Arc<dyn super::super::DirectAccessClient> {
        MemoryClient::new()
    }

    #[tokio::test]
    async fn test_common_client_suite() {
        super::super::client_unit_testing::test_client_functionality(|| async { new_client() }).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_url_expiration() {
        super::super::client_unit_testing::test_url_expiration_functionality(|| async { new_client() }).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_api_delay() {
        super::super::client_unit_testing::test_api_delay_functionality(|| async { new_client() }).await;
    }

    /// Comprehensive test for RandomXorb insertion and data access.
    #[tokio::test]
    async fn test_random_xorb() {
        let client = MemoryClient::new();

        // Basic insertion and existence
        let xorb = RandomXorb::from_seed(42, 5, 1024);
        let xorb_hash = xorb.xorb_hash();
        let returned_hash = client.insert_random_xorb(xorb.clone()).await.unwrap();
        assert_eq!(xorb_hash, returned_hash);
        assert!(client.xorb_exists(&xorb_hash).await.unwrap());
        assert_eq!(client.list_xorbs().await.unwrap(), vec![xorb_hash]);

        // Full and partial data retrieval
        let full_data = client.get_full_xorb(&xorb_hash).await.unwrap();
        assert_eq!(full_data, xorb.get_chunk_range_data(0, 5).unwrap());

        let range_data = client.get_xorb_ranges(&xorb_hash, vec![(1, 3)]).await.unwrap();
        assert_eq!(range_data[0], xorb.get_chunk_range_data(1, 3).unwrap());

        // Footer/CasObject correctness
        let footer = client.xorb_footer(&xorb_hash).await.unwrap();
        let expected_footer = xorb.get_cas_object();
        assert_eq!(footer.info.num_chunks, expected_footer.info.num_chunks);
        assert_eq!(footer.info.cashash, expected_footer.info.cashash);
        assert_eq!(footer.info.chunk_hashes, expected_footer.info.chunk_hashes);

        // Raw serialized bytes
        let raw_len = client.xorb_raw_length(&xorb_hash).await.unwrap();
        assert_eq!(raw_len, xorb.serialized_length());
        assert_eq!(client.get_xorb_raw_bytes(&xorb_hash, None).await.unwrap(), xorb.get_full_serialized());

        let partial = client
            .get_xorb_raw_bytes(&xorb_hash, Some(FileRange::new(10, 50)))
            .await
            .unwrap();
        assert_eq!(partial, xorb.get_serialized_range(10, 50));

        // Deletion
        client.delete_xorb(&xorb_hash).await;
        assert!(!client.xorb_exists(&xorb_hash).await.unwrap());
    }

    /// Test RandomXorb with large chunk count and scattered range access.
    #[tokio::test]
    async fn test_random_xorb_large() {
        let client = MemoryClient::new();
        let xorb = RandomXorb::from_seed(12345, 100, 4096);
        let xorb_hash = client.insert_random_xorb(xorb.clone()).await.unwrap();

        let ranges = vec![(0, 10), (50, 60), (90, 100)];
        let results = client.get_xorb_ranges(&xorb_hash, ranges.clone()).await.unwrap();

        for (i, (start, end)) in ranges.iter().enumerate() {
            assert_eq!(results[i], xorb.get_chunk_range_data(*start, *end).unwrap());
        }
    }

    /// Comprehensive test for lazy file insertion with on-the-fly xorb generation.
    #[tokio::test]
    async fn test_lazy_file() {
        let client = MemoryClient::new();

        // Single-term file
        let file = client.insert_random_lazy_file(&[(1, (0, 3))], 256).await.unwrap();
        assert_eq!(client.get_file_size(&file.file_hash).await.unwrap(), file.data.len() as u64);
        assert_eq!(client.get_file_data(&file.file_hash, None).await.unwrap(), file.data);

        // Multi-term file with reused xorb
        let file2 = client
            .insert_random_lazy_file(&[(1, (0, 2)), (2, (0, 3)), (1, (2, 4))], 512)
            .await
            .unwrap();
        assert_eq!(file2.terms.len(), 3);
        for term in &file2.terms {
            let xorb_data = client
                .get_xorb_ranges(&term.xorb_hash, vec![(term.chunk_start, term.chunk_end)])
                .await
                .unwrap();
            assert_eq!(xorb_data[0], term.data);
        }
        assert_eq!(client.get_file_data(&file2.file_hash, None).await.unwrap(), file2.data);

        // Range access
        let (start, end) = (100u64, 500u64);
        let range_data = client
            .get_file_data(&file2.file_hash, Some(FileRange::new(start, end)))
            .await
            .unwrap();
        assert_eq!(range_data.as_ref(), &file2.data[start as usize..end as usize]);

        // Reconstruction workflow
        let recon = client.get_reconstruction(&file2.file_hash, None).await.unwrap().unwrap();
        for term in &recon.terms {
            let xorb_hash: MerkleHash = term.hash.into();
            for fetch_info in recon.fetch_info.get(&term.hash).unwrap() {
                let (data, _chunk_indices) = client.fetch_term_data(xorb_hash, fetch_info.clone()).await.unwrap();
                assert!(!data.is_empty());
            }
        }
    }

    /// Same term_spec on two clients should produce identical file hashes and data.
    #[tokio::test]
    async fn test_lazy_file_deterministic() {
        let term_spec = &[(999, (0, 4))];
        let file1 = MemoryClient::new().insert_random_lazy_file(term_spec, 512).await.unwrap();
        let file2 = MemoryClient::new().insert_random_lazy_file(term_spec, 512).await.unwrap();
        assert_eq!(file1.file_hash, file2.file_hash);
        assert_eq!(file1.data, file2.data);
    }

    /// Verify materialized and random xorbs coexist correctly.
    #[tokio::test]
    async fn test_mixed_xorb_types() {
        let client = MemoryClient::new();

        let random_xorb = RandomXorb::from_seed(111, 3, 256);
        let random_hash = client.insert_random_xorb(random_xorb).await.unwrap();

        let file = client.upload_random_file(&[(222, (0, 3))], 256).await.unwrap();
        let materialized_hash = file.terms[0].xorb_hash;

        assert!(client.xorb_exists(&random_hash).await.unwrap());
        assert!(client.xorb_exists(&materialized_hash).await.unwrap());
        assert_eq!(client.list_xorbs().await.unwrap().len(), 2);
        assert!(!client.get_full_xorb(&random_hash).await.unwrap().is_empty());
        assert!(!client.get_full_xorb(&materialized_hash).await.unwrap().is_empty());
    }
}
