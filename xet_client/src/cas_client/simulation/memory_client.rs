use std::collections::HashMap;
use std::io::{BufReader, Cursor};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicU64, AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use rand::RngExt;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};
use tracing::{error, info};
use xet_core_structures::MerkleHashMap;
use xet_core_structures::merklehash::MerkleHash;
#[cfg(not(target_family = "wasm"))]
use xet_core_structures::merklehash::compute_data_hash;
use xet_core_structures::metadata_shard::file_structs::MDBFileInfo;
use xet_core_structures::metadata_shard::shard_in_memory::MDBInMemoryShard;
use xet_core_structures::metadata_shard::streaming_shard::MDBMinimalShard;
use xet_core_structures::metadata_shard::xorb_structs::MDBXorbInfo;
use xet_core_structures::xorb_object::{SerializedXorbObject, XorbObject};
use xet_runtime::core::XetContext;

use super::super::Client;
use super::super::adaptive_concurrency::AdaptiveConcurrencyController;
use super::super::progress_tracked_streams::ProgressCallback;
use super::client_testing_utils::{FileTermReference, RandomFileContents};
#[cfg(not(target_family = "wasm"))]
use super::deletion_controls::ObjectTag;
use super::direct_access_client::DirectAccessClient;
use super::random_xorb::RandomXorb;
use super::xorb_utils::{self, REFERENCE_INSTANT, duration_to_expiration_secs_ceil};
use crate::cas_client::chunk_window_builder::build_file_chunk_hashes_response;
use crate::cas_types::{
    BatchQueryReconstructionResponse, FileChunkHashesResponse, FileRange, HexMerkleHash, HttpRange,
    QueryReconstructionResponse, QueryReconstructionResponseV2, XorbMultiRangeFetch, XorbRangeDescriptor,
    XorbReconstructionFetchInfo,
};
use crate::error::{ClientError, Result};

/// Stored XORB data: the serialized data and the deserialized XorbObject (header/footer).
struct MaterializedXorb {
    serialized_data: Bytes,
    xorb_object: XorbObject,
}

/// Storage for a XORB - either fully materialized or generated on-the-fly.
/// Each variant carries a monotonic `generation` counter that is bumped on
/// every insert, so the tag changes even when content is identical (matching
/// production ETag semantics).
enum XorbStorage {
    Materialized { entry: MaterializedXorb, generation: u64 },
    Random { xorb: RandomXorb, generation: u64 },
}

/// In-memory client for testing purposes. Stores all data in memory using hash tables.
pub struct MemoryClient {
    /// XORBs stored by hash
    xorbs: RwLock<MerkleHashMap<XorbStorage>>,
    /// In-memory shard for file reconstruction info
    shard: RwLock<MDBInMemoryShard>,
    /// Global dedup lookup: chunk_hash -> shard bytes
    global_dedup: RwLock<MerkleHashMap<Bytes>>,
    /// Upload concurrency controller
    upload_concurrency_controller: Arc<AdaptiveConcurrencyController>,
    /// Monotonic counter for xorb upload generations (tag freshness).
    xorb_generation: AtomicU64,
    /// URL expiration in milliseconds
    url_expiration_ms: AtomicU64,
    /// Global dedup shard expiration in seconds (0 = disabled).
    global_dedup_expiration_secs: AtomicU64,
    /// API delay range in milliseconds as (min_ms, max_ms). (0, 0) means disabled.
    random_ms_delay_window: (AtomicU64, AtomicU64),
    /// Max ranges per XorbMultiRangeFetch entry. usize::MAX means no splitting.
    max_ranges_per_fetch: AtomicUsize,
    /// HTTP status code to return when V2 is disabled (0 = enabled).
    v2_disabled_status: AtomicU16,
}

impl MemoryClient {
    /// Create a new in-memory client.
    pub fn new(ctx: XetContext) -> Arc<Self> {
        Arc::new(Self {
            xorbs: RwLock::new(MerkleHashMap::new()),
            shard: RwLock::new(MDBInMemoryShard::default()),
            global_dedup: RwLock::new(MerkleHashMap::new()),
            upload_concurrency_controller: AdaptiveConcurrencyController::new_upload(ctx, "memory_uploads"),
            xorb_generation: AtomicU64::new(0),
            url_expiration_ms: AtomicU64::new(u64::MAX),
            global_dedup_expiration_secs: AtomicU64::new(0),
            random_ms_delay_window: (AtomicU64::new(0), AtomicU64::new(0)),
            max_ranges_per_fetch: AtomicUsize::new(usize::MAX),
            v2_disabled_status: AtomicU16::new(0),
        })
    }

    /// Inserts a RandomXorb into the client's storage and registers it in the shard
    /// for XORB block lookups. Returns the xorb hash.
    ///
    /// This allows storing XORBs that generate their data on-the-fly,
    /// enabling testing with massive files without storing all data in memory.
    pub async fn insert_random_xorb(&self, xorb: RandomXorb) -> Result<MerkleHash> {
        use xet_core_structures::metadata_shard::xorb_structs::{
            MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader,
        };

        let hash = xorb.xorb_hash();
        let xorb_obj = xorb.get_xorb_object();

        // Create XORB block entries for each chunk
        let mut chunk_entries = Vec::with_capacity(xorb.num_chunks() as usize);
        let mut byte_offset = 0u32;

        for i in 0..xorb.num_chunks() {
            let chunk_size = xorb.chunk_size(i).unwrap();
            chunk_entries.push(XorbChunkSequenceEntry {
                chunk_hash: xorb.chunk_hash(i).unwrap(),
                chunk_byte_range_start: byte_offset,
                unpacked_segment_bytes: chunk_size,
                flags: 0,
                _unused: 0,
            });
            byte_offset += chunk_size;
        }

        let cas_info = MDBXorbInfo {
            metadata: XorbChunkSequenceHeader::new(
                hash,
                xorb.num_chunks(),
                xorb_obj.info.unpacked_chunk_offsets.last().copied().unwrap_or(0),
            ),
            chunks: chunk_entries,
        };

        {
            let mut shard = self.shard.write().await;
            shard.add_xorb_block(cas_info)?;
        }

        let generation = self.xorb_generation.fetch_add(1, Ordering::Relaxed);
        self.xorbs.write().await.insert(hash, XorbStorage::Random { xorb, generation });
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
        use xet_core_structures::metadata_shard::file_structs::{
            FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo,
        };

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

        let file_hash = xet_core_structures::merklehash::file_hash_with_salt(&chunk_file_hashes, &[0; 32]);

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
            xorbs: MerkleHashMap::new(),
            terms: term_infos,
        })
    }

    #[cfg(not(target_family = "wasm"))]
    fn current_shard_hash_and_bytes(shard: &MDBInMemoryShard) -> Result<Option<(MerkleHash, Bytes)>> {
        if shard.is_empty() {
            return Ok(None);
        }
        let bytes = Bytes::from(shard.to_bytes()?);
        let hash = compute_data_hash(bytes.as_ref());
        Ok(Some((hash, bytes)))
    }

    #[cfg(not(target_family = "wasm"))]
    fn object_tag_from_key_and_payload(prefix: &[u8], key: &MerkleHash, payload: &[u8]) -> ObjectTag {
        let key_bytes: [u8; 32] = (*key).into();
        let payload_hash: [u8; 32] = compute_data_hash(payload).into();
        let mut entropy = Vec::with_capacity(prefix.len() + key_bytes.len() + payload_hash.len());
        entropy.extend_from_slice(prefix);
        entropy.extend_from_slice(&key_bytes);
        entropy.extend_from_slice(&payload_hash);
        compute_data_hash(&entropy).into()
    }

    #[cfg(not(target_family = "wasm"))]
    fn xorb_tag(hash: &MerkleHash, storage: &XorbStorage) -> ObjectTag {
        match storage {
            XorbStorage::Materialized { entry, generation } => {
                let mut payload = Vec::from(entry.serialized_data.as_ref());
                payload.extend_from_slice(&generation.to_le_bytes());
                Self::object_tag_from_key_and_payload(b"xorb", hash, &payload)
            },
            XorbStorage::Random { xorb, generation } => {
                let mut entropy = Vec::with_capacity(16);
                entropy.extend_from_slice(&xorb.num_chunks().to_le_bytes());
                entropy.extend_from_slice(&generation.to_le_bytes());
                Self::object_tag_from_key_and_payload(b"xorb", hash, &entropy)
            },
        }
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
impl DirectAccessClient for MemoryClient {
    fn set_fetch_term_url_expiration(&self, expiration: Duration) {
        self.url_expiration_ms.store(expiration.as_millis() as u64, Ordering::Relaxed);
    }

    fn set_global_dedup_shard_expiration(&self, expiration: Option<Duration>) {
        self.global_dedup_expiration_secs
            .store(duration_to_expiration_secs_ceil(expiration), Ordering::Relaxed);
    }

    fn set_max_ranges_per_fetch(&self, max_ranges: usize) {
        self.max_ranges_per_fetch.store(max_ranges, Ordering::Relaxed);
    }

    fn disable_v2_reconstruction(&self, status_code: u16) {
        self.v2_disabled_status.store(status_code, Ordering::Relaxed);
    }

    fn v2_disabled_status_code(&self) -> u16 {
        self.v2_disabled_status.load(Ordering::Relaxed)
    }

    async fn get_reconstruction_v1(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>> {
        MemoryClient::get_reconstruction_v1(self, file_id, bytes_range).await
    }

    async fn get_reconstruction_v2(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponseV2>> {
        MemoryClient::get_reconstruction_v2(self, file_id, bytes_range).await
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

    async fn list_xorbs(&self) -> Result<Vec<MerkleHash>> {
        Ok(self.xorbs.read().await.keys().copied().collect())
    }

    async fn get_full_xorb(&self, hash: &MerkleHash) -> Result<Bytes> {
        let xorbs = self.xorbs.read().await;
        let storage = xorbs.get(hash).ok_or_else(|| {
            error!("Unable to find xorb in memory CAS {:?}", hash);
            ClientError::XORBNotFound(*hash)
        })?;

        match storage {
            XorbStorage::Materialized { entry, .. } => {
                let mut reader = BufReader::new(Cursor::new(&entry.serialized_data));
                let xorb_obj = XorbObject::deserialize(&mut reader)?;
                let result = xorb_obj.get_all_bytes(&mut reader)?;
                Ok(Bytes::from(result))
            },
            XorbStorage::Random { xorb, .. } => xorb
                .get_chunk_range_data(0, xorb.num_chunks())
                .ok_or(ClientError::XORBNotFound(*hash)),
        }
    }

    async fn get_xorb_ranges(&self, hash: &MerkleHash, chunk_ranges: Vec<(u32, u32)>) -> Result<Vec<Bytes>> {
        if chunk_ranges.is_empty() {
            return Ok(vec![Bytes::new()]);
        }

        let xorbs = self.xorbs.read().await;
        let storage = xorbs.get(hash).ok_or_else(|| {
            error!("Unable to find xorb in memory CAS {:?}", hash);
            ClientError::XORBNotFound(*hash)
        })?;

        match storage {
            XorbStorage::Materialized { entry, .. } => {
                let mut reader = BufReader::new(Cursor::new(&entry.serialized_data));
                let xorb_obj = XorbObject::deserialize(&mut reader)?;

                let mut ret: Vec<Bytes> = Vec::new();
                for r in chunk_ranges {
                    if r.0 >= r.1 {
                        ret.push(Bytes::new());
                        continue;
                    }
                    let data = xorb_obj.get_bytes_by_chunk_range(&mut reader, r.0, r.1)?;
                    ret.push(Bytes::from(data));
                }
                Ok(ret)
            },
            XorbStorage::Random { xorb, .. } => {
                let mut ret: Vec<Bytes> = Vec::new();
                for r in chunk_ranges {
                    if r.0 >= r.1 {
                        ret.push(Bytes::new());
                        continue;
                    }
                    let data = xorb.get_chunk_range_data(r.0, r.1).ok_or(ClientError::XORBNotFound(*hash))?;
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

    async fn xorb_footer(&self, hash: &MerkleHash) -> Result<XorbObject> {
        let xorbs = self.xorbs.read().await;
        let storage = xorbs.get(hash).ok_or_else(|| {
            error!("Unable to find xorb in memory CAS {:?}", hash);
            ClientError::XORBNotFound(*hash)
        })?;

        match storage {
            XorbStorage::Materialized { entry, .. } => Ok(entry.xorb_object.clone()),
            XorbStorage::Random { xorb, .. } => Ok(xorb.get_xorb_object()),
        }
    }

    async fn get_file_size(&self, hash: &MerkleHash) -> Result<u64> {
        let shard = self.shard.read().await;
        let file_info = shard
            .get_file_reconstruction_info(hash)
            .ok_or(ClientError::FileNotFound(*hash))?;
        Ok(file_info.file_size())
    }

    async fn get_file_data(&self, hash: &MerkleHash, byte_range: Option<FileRange>) -> Result<Bytes> {
        let file_info = {
            let shard = self.shard.read().await;
            shard
                .get_file_reconstruction_info(hash)
                .ok_or(ClientError::FileNotFound(*hash))?
        };

        let mut file_vec = Vec::new();
        for entry in &file_info.segments {
            let entry_bytes = self
                .get_xorb_ranges(&entry.xorb_hash, vec![(entry.chunk_index_start, entry.chunk_index_end)])
                .await?
                .pop()
                .unwrap();
            file_vec.extend_from_slice(&entry_bytes);
        }

        let file_size = file_vec.len();

        let start = byte_range.as_ref().map(|range| range.start as usize).unwrap_or(0);

        if byte_range.is_some() && start >= file_size {
            return Err(ClientError::InvalidRange);
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
        let storage = xorbs.get(hash).ok_or(ClientError::XORBNotFound(*hash))?;

        match storage {
            XorbStorage::Materialized { entry, .. } => {
                let data = &entry.serialized_data;

                let start = byte_range.as_ref().map(|r| r.start as usize).unwrap_or(0);
                let end = byte_range
                    .as_ref()
                    .map(|r| r.end as usize)
                    .unwrap_or(data.len())
                    .min(data.len());

                if start >= data.len() {
                    return Err(ClientError::InvalidRange);
                }

                Ok(data.slice(start..end))
            },
            XorbStorage::Random { xorb, .. } => {
                let total_len = xorb.serialized_length();
                let start = byte_range.as_ref().map(|r| r.start).unwrap_or(0);
                let end = byte_range.as_ref().map(|r| r.end).unwrap_or(total_len).min(total_len);

                if start >= total_len {
                    return Err(ClientError::InvalidRange);
                }

                Ok(xorb.get_serialized_range(start, end))
            },
        }
    }

    async fn xorb_raw_length(&self, hash: &MerkleHash) -> Result<u64> {
        let xorbs = self.xorbs.read().await;
        let storage = xorbs.get(hash).ok_or(ClientError::XORBNotFound(*hash))?;

        match storage {
            XorbStorage::Materialized { entry, .. } => Ok(entry.serialized_data.len() as u64),
            XorbStorage::Random { xorb, .. } => Ok(xorb.serialized_length()),
        }
    }

    async fn fetch_term_data(
        &self,
        hash: MerkleHash,
        fetch_term: XorbReconstructionFetchInfo,
    ) -> Result<(Bytes, Vec<u32>)> {
        self.apply_api_delay().await;
        let (xorb_hash, url_byte_range, url_timestamp) = parse_fetch_url(&fetch_term.url)?;

        // Check if URL has expired
        let expiration_ms = self.url_expiration_ms.load(Ordering::Relaxed);
        let elapsed_ms = Instant::now().saturating_duration_since(url_timestamp).as_millis() as u64;
        if elapsed_ms > expiration_ms {
            return Err(ClientError::PresignedUrlExpirationError);
        }

        // Validate byte range matches url_range
        // Note: url_byte_range is FileRange (exclusive end), url_range is HttpRange (inclusive end)
        // We convert url_range to FileRange for comparison
        let fetch_byte_range = FileRange::from(fetch_term.url_range);
        if url_byte_range.start != fetch_byte_range.start || url_byte_range.end != fetch_byte_range.end {
            return Err(ClientError::InvalidArguments);
        }

        let xorbs = self.xorbs.read().await;
        let storage = xorbs.get(&xorb_hash).ok_or_else(|| {
            error!("Unable to find xorb in memory CAS {:?}", hash);
            ClientError::XORBNotFound(hash)
        })?;

        let (data, xorb_obj) = match storage {
            XorbStorage::Materialized { entry, .. } => {
                let mut reader = BufReader::new(Cursor::new(&entry.serialized_data));
                let xorb_obj = XorbObject::deserialize(&mut reader)?;
                let data =
                    xorb_obj.get_bytes_by_chunk_range(&mut reader, fetch_term.range.start, fetch_term.range.end)?;
                (Bytes::from(data), xorb_obj)
            },
            XorbStorage::Random { xorb, .. } => {
                let data = xorb
                    .get_chunk_range_data(fetch_term.range.start, fetch_term.range.end)
                    .ok_or(ClientError::XORBNotFound(hash))?;
                let xorb_obj = xorb.get_xorb_object();
                (data, xorb_obj)
            },
        };

        let chunk_byte_indices = {
            let mut indices = Vec::new();
            let mut cumulative = 0u32;
            indices.push(0);
            for chunk_idx in fetch_term.range.start..fetch_term.range.end {
                let chunk_len = xorb_obj
                    .uncompressed_chunk_length(chunk_idx)
                    .map_err(|e| ClientError::Other(format!("Failed to get chunk length: {e}")))?;
                cumulative += chunk_len;
                indices.push(cumulative);
            }
            indices
        };

        Ok((data, chunk_byte_indices))
    }
}

impl MemoryClient {
    async fn compute_reconstruction_ranges(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<xorb_utils::ReconstructionRangesResult> {
        let file_info = {
            let shard = self.shard.read().await;
            match shard.get_file_reconstruction_info(file_id) {
                Some(fi) => fi,
                None => return Ok(None),
            }
        };

        let xorbs = self.xorbs.read().await;
        xorb_utils::compute_reconstruction_ranges(&file_info, bytes_range, &mut |hash| {
            let storage = xorbs.get(hash).ok_or_else(|| {
                error!("Unable to find xorb in memory CAS {:?}", hash);
                ClientError::XORBNotFound(*hash)
            })?;
            Ok(match storage {
                XorbStorage::Materialized { entry, .. } => entry.xorb_object.clone(),
                XorbStorage::Random { xorb, .. } => xorb.get_xorb_object(),
            })
        })
    }

    /// V1 reconstruction: returns per-range presigned URLs.
    pub async fn get_reconstruction_v1(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>> {
        self.apply_api_delay().await;

        let result = self.compute_reconstruction_ranges(file_id, bytes_range).await?;
        let Some((offset_into_first_range, terms, merged_ranges)) = result else {
            return Ok(None);
        };

        if terms.is_empty() {
            return Ok(Some(QueryReconstructionResponse {
                offset_into_first_range,
                terms,
                fetch_info: HashMap::new(),
            }));
        }

        let timestamp = Instant::now();
        let mut fetch_info: HashMap<HexMerkleHash, Vec<XorbReconstructionFetchInfo>> = HashMap::new();
        for (hash, ranges) in merged_ranges {
            let entries = ranges
                .into_iter()
                .map(|r| XorbReconstructionFetchInfo {
                    range: r.chunk_range,
                    url: generate_fetch_url(&hash, &r.byte_range, timestamp),
                    url_range: HttpRange::from(r.byte_range),
                })
                .collect();
            fetch_info.insert(hash.into(), entries);
        }

        Ok(Some(QueryReconstructionResponse {
            offset_into_first_range,
            terms,
            fetch_info,
        }))
    }

    /// V2 reconstruction: returns per-xorb multi-range fetch descriptors.
    pub async fn get_reconstruction_v2(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponseV2>> {
        self.apply_api_delay().await;

        let result = self.compute_reconstruction_ranges(file_id, bytes_range).await?;
        let Some((offset_into_first_range, terms, merged_ranges)) = result else {
            return Ok(None);
        };

        if terms.is_empty() {
            return Ok(Some(QueryReconstructionResponseV2 {
                offset_into_first_range,
                terms,
                xorbs: HashMap::new(),
            }));
        }

        let timestamp = Instant::now();
        let max_ranges = self.max_ranges_per_fetch.load(Ordering::Relaxed);

        let mut xorbs: HashMap<HexMerkleHash, Vec<XorbMultiRangeFetch>> = HashMap::new();
        for (hash, ranges) in merged_ranges {
            let mut fetch_entries = Vec::new();

            for chunk in ranges.chunks(max_ranges) {
                let range_descriptors: Vec<XorbRangeDescriptor> = chunk
                    .iter()
                    .map(|r| XorbRangeDescriptor {
                        chunks: r.chunk_range,
                        bytes: HttpRange::from(r.byte_range),
                    })
                    .collect();

                let url = generate_v2_fetch_url(&hash, &range_descriptors, timestamp);
                fetch_entries.push(XorbMultiRangeFetch {
                    url,
                    ranges: range_descriptors,
                });
            }

            xorbs.insert(hash.into(), fetch_entries);
        }

        Ok(Some(QueryReconstructionResponseV2 {
            offset_into_first_range,
            terms,
            xorbs,
        }))
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
        let shard_bytes = {
            let dedup = self.global_dedup.read().await;
            let Some(shard_bytes) = dedup.get(chunk_hash) else {
                return Ok(None);
            };
            shard_bytes.clone()
        };

        let expiration_secs = self.global_dedup_expiration_secs.load(Ordering::Relaxed);
        if expiration_secs == 0 {
            return Ok(Some(shard_bytes));
        }

        let expiry = std::time::SystemTime::now() + Duration::from_secs(expiration_secs);

        let mut reader = Cursor::new(shard_bytes.as_ref());
        let minimal_shard = MDBMinimalShard::from_reader(&mut reader, true, true)?;

        let mut out = Vec::new();
        minimal_shard.serialize_xorb_subset_with_expiry(&mut out, Some(expiry), |_| true)?;
        Ok(Some(out.into()))
    }

    async fn acquire_upload_permit(&self) -> Result<super::super::adaptive_concurrency::ConnectionPermit> {
        self.apply_api_delay().await;
        self.upload_concurrency_controller.acquire_connection_permit().await
    }

    async fn upload_shard(
        &self,
        shard_data: Bytes,
        _permit: super::super::adaptive_concurrency::ConnectionPermit,
    ) -> Result<bool> {
        self.apply_api_delay().await;
        // Parse the shard using the streaming parser (handles shards without footer)
        let mut reader = Cursor::new(&shard_data);
        let minimal_shard = MDBMinimalShard::from_reader(&mut reader, true, true)?;

        // Merge file info into our shard
        {
            let mut shard_lg = self.shard.write().await;

            // Add file info from the views
            for i in 0..minimal_shard.num_files() {
                let file_view = minimal_shard.file(i).unwrap();
                shard_lg.add_file_reconstruction_info(MDBFileInfo::from(file_view))?;
            }

            // Add XORB info from the views
            for i in 0..minimal_shard.num_xorb() {
                let xorb_view = minimal_shard.xorb(i).unwrap();
                shard_lg.add_xorb_block(MDBXorbInfo::from(xorb_view))?;
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
        serialized_xorb_object: SerializedXorbObject,
        progress_callback: Option<ProgressCallback>,
        _permit: super::super::adaptive_concurrency::ConnectionPermit,
    ) -> Result<u64> {
        self.apply_api_delay().await;
        let hash = serialized_xorb_object.hash;
        let footer_start = serialized_xorb_object.footer_start;
        let serialized_data = serialized_xorb_object.serialized_data;

        // Always overwrite: even if the xorb already exists, we must store it
        // with a fresh generation so its tag changes, matching production ETag
        // semantics and ensuring delete_xorb_if_tag_matches is safe under
        // concurrent uploads.

        info!("Storing XORB {hash:?} in memory");

        // Reconstruct XorbObject from chunk data if no footer, or deserialize if footer present
        let (xorb_obj, serialized_data) = if footer_start.is_some() {
            let mut reader = Cursor::new(&serialized_data);
            let xorb_obj = XorbObject::deserialize(&mut reader)?;
            (xorb_obj, serialized_data)
        } else {
            let mut data_with_footer = Vec::new();
            let (xorb_obj, computed_hash) = xet_core_structures::xorb_object::reconstruct_xorb_with_footer(
                &mut data_with_footer,
                &serialized_data,
            )?;
            if computed_hash != hash {
                return Err(ClientError::Other(format!(
                    "XORB hash mismatch: expected {}, got {}",
                    hash.hex(),
                    computed_hash.hex(),
                )));
            }
            (xorb_obj, data_with_footer)
        };

        let bytes_written = serialized_data.len();
        let generation = self.xorb_generation.fetch_add(1, Ordering::Relaxed);

        {
            let mut xorbs = self.xorbs.write().await;
            xorbs.insert(
                hash,
                XorbStorage::Materialized {
                    entry: MaterializedXorb {
                        serialized_data: Bytes::from(serialized_data),
                        xorb_object: xorb_obj,
                    },
                    generation,
                },
            );
        }

        if let Some(ref cb) = progress_callback {
            let n = bytes_written as u64;
            cb(n, n, n);
        }

        info!("XORB {hash:?} successfully stored with {bytes_written} bytes.");

        Ok(bytes_written as u64)
    }

    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponseV2>> {
        self.get_reconstruction_v2(file_id, bytes_range).await
    }

    async fn batch_get_reconstruction(&self, file_ids: &[MerkleHash]) -> Result<BatchQueryReconstructionResponse> {
        self.apply_api_delay().await;
        let mut files = HashMap::new();
        let mut fetch_info_map: HashMap<HexMerkleHash, Vec<XorbReconstructionFetchInfo>> = HashMap::new();

        for file_id in file_ids {
            if let Some(response) = self.get_reconstruction_v1(file_id, None).await? {
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

    async fn acquire_download_permit(&self) -> Result<super::super::adaptive_concurrency::ConnectionPermit> {
        self.apply_api_delay().await;
        self.upload_concurrency_controller.acquire_connection_permit().await
    }

    async fn get_file_term_data(
        &self,
        url_info: Box<dyn super::super::interface::URLProvider>,
        _download_permit: super::super::adaptive_concurrency::ConnectionPermit,
        progress_callback: Option<ProgressCallback>,
        uncompressed_size_if_known: Option<usize>,
    ) -> Result<(Bytes, Vec<u32>)> {
        self.apply_api_delay().await;
        let (url, http_ranges) = url_info.retrieve_url().await?;
        let (xorb_hash, url_timestamp) = parse_any_fetch_url(&url)?;

        // Check if URL has expired
        let expiration_ms = self.url_expiration_ms.load(Ordering::Relaxed);
        let elapsed_ms = Instant::now().saturating_duration_since(url_timestamp).as_millis() as u64;
        if elapsed_ms > expiration_ms {
            return Err(ClientError::PresignedUrlExpirationError);
        }

        let xorbs = self.xorbs.read().await;
        let storage = xorbs.get(&xorb_hash).ok_or(ClientError::XORBNotFound(xorb_hash))?;

        // Extract each byte range from the serialized data and deserialize
        let mut all_decompressed = Vec::new();
        let mut all_chunk_indices = Vec::<u32>::new();
        let mut total_transfer = 0u64;

        for http_range in &http_ranges {
            let start = http_range.start as usize;
            let end = http_range.end as usize + 1;
            total_transfer += http_range.length();

            let (data, chunk_indices) = match storage {
                XorbStorage::Materialized { entry, .. } => {
                    let range_data = &entry.serialized_data[start..end];
                    xet_core_structures::xorb_object::deserialize_chunks(&mut Cursor::new(range_data))?
                },
                XorbStorage::Random { xorb, .. } => {
                    let range_data = xorb.get_serialized_range(start as u64, end as u64);
                    xet_core_structures::xorb_object::deserialize_chunks(&mut Cursor::new(range_data.as_ref()))?
                },
            };

            xet_core_structures::xorb_object::append_chunk_segment(
                &mut all_decompressed,
                &mut all_chunk_indices,
                &data,
                &chunk_indices,
            );
        }

        if let Some(expected) = uncompressed_size_if_known {
            debug_assert_eq!(
                all_decompressed.len(),
                expected,
                "get_file_term_data: expected {} bytes, got {}",
                expected,
                all_decompressed.len()
            );
        }

        if let Some(ref cb) = progress_callback {
            cb(total_transfer, total_transfer, total_transfer);
        }
        Ok((Bytes::from(all_decompressed), all_chunk_indices))
    }

    async fn get_file_chunk_hashes(
        &self,
        file_id: &MerkleHash,
        dirty_ranges: Vec<FileRange>,
    ) -> Result<FileChunkHashesResponse> {
        self.apply_api_delay().await;

        let file_info = {
            let shard = self.shard.read().await;
            shard
                .get_file_reconstruction_info(file_id)
                .ok_or(ClientError::FileNotFound(*file_id))?
        };

        let xorbs = self.xorbs.read().await;
        let mut chunks: Vec<(MerkleHash, u64)> = Vec::new();
        for segment in &file_info.segments {
            let storage = xorbs
                .get(&segment.xorb_hash)
                .ok_or(ClientError::XORBNotFound(segment.xorb_hash))?;
            let xorb_obj = match storage {
                XorbStorage::Materialized { entry, .. } => std::borrow::Cow::Borrowed(&entry.xorb_object),
                XorbStorage::Random { xorb, .. } => std::borrow::Cow::Owned(xorb.get_xorb_object()),
            };
            chunks.extend(
                xorb_obj
                    .chunk_hash_sizes(segment.chunk_index_start, segment.chunk_index_end)
                    .map_err(|err| ClientError::Other(format!("chunk_hash_sizes error: {err}")))?,
            );
        }

        build_file_chunk_hashes_response(file_info.file_size(), dirty_ranges, chunks)
    }
}

#[cfg(not(target_family = "wasm"))]
#[async_trait]
impl super::DeletionControlableClient for MemoryClient {
    async fn list_shard_entries(&self) -> Result<Vec<MerkleHash>> {
        let shard = self.shard.read().await;
        Ok(Self::current_shard_hash_and_bytes(&shard)?
            .map(|(h, _)| vec![h])
            .unwrap_or_default())
    }

    async fn get_shard_bytes(&self, hash: &MerkleHash) -> Result<Bytes> {
        let shard = self.shard.read().await;
        let Some((current_hash, bytes)) = Self::current_shard_hash_and_bytes(&shard)? else {
            return Err(ClientError::Other(format!("Shard not found: {}", hash.hex())));
        };
        if &current_hash != hash {
            return Err(ClientError::Other(format!("Shard not found: {}", hash.hex())));
        }
        Ok(bytes)
    }

    async fn delete_shard_entry(&self, hash: &MerkleHash) -> Result<()> {
        let mut shard = self.shard.write().await;
        let Some((current_hash, _)) = Self::current_shard_hash_and_bytes(&shard)? else {
            return Err(ClientError::Other(format!("Shard not found: {}", hash.hex())));
        };
        if &current_hash != hash {
            return Err(ClientError::Other(format!("Shard not found: {}", hash.hex())));
        }
        *shard = MDBInMemoryShard::default();
        Ok(())
    }

    async fn list_file_shard_entries(&self) -> Result<Vec<(MerkleHash, MerkleHash)>> {
        let shard = self.shard.read().await;
        let Some((shard_hash, _)) = Self::current_shard_hash_and_bytes(&shard)? else {
            return Ok(Vec::new());
        };
        Ok(shard
            .file_content
            .keys()
            .copied()
            .map(|file_hash| (file_hash, shard_hash))
            .collect())
    }

    async fn delete_file_entry(&self, file_hash: &MerkleHash) -> Result<()> {
        let mut shard = self.shard.write().await;
        if shard.file_content.remove(file_hash).is_some() {
            shard.recalculate_shard_size();
        }
        Ok(())
    }

    async fn remove_shard_dedup_entries(&self, shard_hash: &MerkleHash) -> Result<()> {
        let shard = self.shard.read().await;
        let Some((current_hash, _)) = Self::current_shard_hash_and_bytes(&shard)? else {
            return Ok(());
        };
        if &current_hash != shard_hash {
            return Ok(());
        }
        drop(shard);

        self.global_dedup.write().await.clear();
        Ok(())
    }

    async fn delete_xorb(&self, hash: &MerkleHash) {
        self.xorbs.write().await.remove(hash);
    }

    async fn list_xorbs_and_tags(&self) -> Result<Vec<(MerkleHash, ObjectTag)>> {
        let xorbs = self.xorbs.read().await;
        Ok(xorbs
            .iter()
            .map(|(hash, storage)| (*hash, Self::xorb_tag(hash, storage)))
            .collect())
    }

    async fn delete_xorb_if_tag_matches(&self, hash: &MerkleHash, tag: &ObjectTag) -> Result<bool> {
        let mut xorbs = self.xorbs.write().await;
        let Some(storage) = xorbs.get(hash) else {
            return Err(ClientError::XORBNotFound(*hash));
        };
        if &Self::xorb_tag(hash, storage) != tag {
            return Ok(false);
        }
        xorbs.remove(hash);
        Ok(true)
    }

    async fn list_shards_with_tags(&self) -> Result<Vec<(MerkleHash, ObjectTag)>> {
        let shard = self.shard.read().await;
        let Some((shard_hash, shard_bytes)) = Self::current_shard_hash_and_bytes(&shard)? else {
            return Ok(Vec::new());
        };
        let tag = Self::object_tag_from_key_and_payload(b"shard", &shard_hash, shard_bytes.as_ref());
        Ok(vec![(shard_hash, tag)])
    }

    async fn delete_shard_if_tag_matches(&self, hash: &MerkleHash, tag: &ObjectTag) -> Result<bool> {
        let mut shard = self.shard.write().await;
        let Some((current_hash, shard_bytes)) = Self::current_shard_hash_and_bytes(&shard)? else {
            return Err(ClientError::Other(format!("Shard not found: {}", hash.hex())));
        };
        if &current_hash != hash {
            return Err(ClientError::Other(format!("Shard not found: {}", hash.hex())));
        }
        let current_tag = Self::object_tag_from_key_and_payload(b"shard", &current_hash, shard_bytes.as_ref());
        if &current_tag != tag {
            return Ok(false);
        }
        *shard = MDBInMemoryShard::default();
        Ok(true)
    }

    async fn verify_integrity(&self) -> Result<()> {
        let xorbs = self.xorbs.read().await;
        let shard = self.shard.read().await;
        for file_info in shard.file_content.values() {
            for segment in &file_info.segments {
                if !xorbs.contains_key(&segment.xorb_hash) {
                    return Err(ClientError::XORBNotFound(segment.xorb_hash));
                }
            }
        }
        Ok(())
    }

    async fn verify_all_reachable(&self) -> Result<()> {
        self.verify_integrity().await
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
        return Err(ClientError::InvalidArguments);
    }

    let hash = MerkleHash::from_hex(parts[0]).map_err(|_| ClientError::InvalidArguments)?;
    let start_pos: u64 = parts[1].parse().map_err(|_| ClientError::InvalidArguments)?;
    let end_pos: u64 = parts[2].parse().map_err(|_| ClientError::InvalidArguments)?;
    let timestamp_ms: u64 = parts[3].parse().map_err(|_| ClientError::InvalidArguments)?;

    let byte_range = FileRange::new(start_pos, end_pos);
    let timestamp = *REFERENCE_INSTANT + Duration::from_millis(timestamp_ms);

    Ok((hash, byte_range, timestamp))
}

fn generate_v2_fetch_url(hash: &MerkleHash, ranges: &[XorbRangeDescriptor], timestamp: Instant) -> String {
    xorb_utils::generate_v2_fetch_url(hash, ranges, timestamp)
}

/// Parse either a V1 or V2 fetch URL, returning (hash, timestamp).
fn parse_any_fetch_url(url: &str) -> Result<(MerkleHash, Instant)> {
    if let Ok((hash, _, ts)) = parse_fetch_url(url) {
        return Ok((hash, ts));
    }
    let (hash, ts, _) = xorb_utils::parse_v2_fetch_url(url)?;
    Ok((hash, ts))
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use xet_runtime::config::XetConfig;
    use xet_runtime::core::XetContext;

    use super::super::client_testing_utils::ClientTestingUtils;
    use super::super::deletion_controls::DeletionControlableClient;
    use super::*;

    fn test_ctx() -> XetContext {
        let config = XetConfig::new();
        XetContext::from_external(tokio::runtime::Handle::current(), config)
    }

    fn new_client() -> Arc<dyn super::super::DirectAccessClient> {
        MemoryClient::new(test_ctx())
    }

    fn new_deletion_client() -> Arc<MemoryClient> {
        MemoryClient::new(test_ctx())
    }

    #[tokio::test]
    async fn test_common_client_suite() {
        super::super::client_unit_testing::test_client_functionality(|| async { new_client() }).await;
    }

    #[tokio::test]
    async fn test_memory_deletion_controls_basic() {
        let client = new_deletion_client();
        let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();

        let xorbs_and_tags = client.list_xorbs_and_tags().await.unwrap();
        assert!(!xorbs_and_tags.is_empty());
        let (xorb_hash, tag) = xorbs_and_tags[0];

        let wrong_tag = [0xABu8; 32];
        assert!(!client.delete_xorb_if_tag_matches(&xorb_hash, &wrong_tag).await.unwrap());
        assert!(client.xorb_exists(&xorb_hash).await.unwrap());

        assert!(client.delete_xorb_if_tag_matches(&xorb_hash, &tag).await.unwrap());
        assert!(!client.xorb_exists(&xorb_hash).await.unwrap());

        // file deletion is idempotent for parity with the disk-backed behavior.
        client.delete_file_entry(&file.file_hash).await.unwrap();
        client.delete_file_entry(&file.file_hash).await.unwrap();

        let shards_and_tags = client.list_shards_with_tags().await.unwrap();
        if !shards_and_tags.is_empty() {
            let (shard_hash, shard_tag) = shards_and_tags[0];
            assert!(!client.delete_shard_if_tag_matches(&shard_hash, &wrong_tag).await.unwrap());
            assert!(client.delete_shard_if_tag_matches(&shard_hash, &shard_tag).await.unwrap());
            assert!(client.list_shard_entries().await.unwrap().is_empty());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_url_expiration() {
        super::super::client_unit_testing::test_url_expiration_functionality(|| async { new_client() }).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_api_delay() {
        super::super::client_unit_testing::test_api_delay_functionality(|| async { new_client() }).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_global_dedup_shard_expiration() {
        super::super::client_unit_testing::test_global_dedup_shard_expiration_functionality(|| async { new_client() })
            .await;
    }

    #[tokio::test]
    #[cfg_attr(feature = "smoke-test", ignore)]
    async fn test_global_dedup_shard_expiration_stress() {
        super::super::client_unit_testing::test_global_dedup_shard_expiration_stress(|| async { new_client() }).await;
    }

    /// Comprehensive test for RandomXorb insertion and data access.
    #[tokio::test]
    async fn test_random_xorb() {
        let client = MemoryClient::new(test_ctx());

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

        // Footer/XorbObject correctness
        let footer = client.xorb_footer(&xorb_hash).await.unwrap();
        let expected_footer = xorb.get_xorb_object();
        assert_eq!(footer.info.num_chunks, expected_footer.info.num_chunks);
        assert_eq!(footer.info.xorb_hash, expected_footer.info.xorb_hash);
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
    }

    /// Test RandomXorb with large chunk count and scattered range access.
    #[tokio::test]
    async fn test_random_xorb_large() {
        let client = MemoryClient::new(test_ctx());
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
        let client = MemoryClient::new(test_ctx());

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
        let recon = client.get_reconstruction_v1(&file2.file_hash, None).await.unwrap().unwrap();
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
        let file1 = MemoryClient::new(test_ctx())
            .insert_random_lazy_file(term_spec, 512)
            .await
            .unwrap();
        let file2 = MemoryClient::new(test_ctx())
            .insert_random_lazy_file(term_spec, 512)
            .await
            .unwrap();
        assert_eq!(file1.file_hash, file2.file_hash);
        assert_eq!(file1.data, file2.data);
    }

    /// Verify materialized and random xorbs coexist correctly.
    #[tokio::test]
    async fn test_mixed_xorb_types() {
        let client = MemoryClient::new(test_ctx());

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
