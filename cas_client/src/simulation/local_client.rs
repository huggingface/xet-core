use std::collections::HashMap;
use std::fs::{File, metadata};
use std::io::{BufReader, Cursor, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use cas_object::{CasObject, SerializedCasObject};
use cas_types::{
    BatchQueryReconstructionResponse, CASReconstructionFetchInfo, CASReconstructionTerm, ChunkRange, FileRange,
    HexMerkleHash, HttpRange, QueryReconstructionResponse,
};
use file_utils::SafeFileCreator;
use heed::types::*;
use lazy_static::lazy_static;
use mdb_shard::cas_structs::MDBCASInfo;
use mdb_shard::file_structs::MDBFileInfo;
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use mdb_shard::shard_in_memory::MDBInMemoryShard;
use mdb_shard::streaming_shard::MDBMinimalShard;
use mdb_shard::utils::shard_file_name;
use mdb_shard::{MDBShardFile, ShardFileManager};
use merklehash::MerkleHash;
use more_asserts::*;
use rand::Rng;
use tempfile::TempDir;
use tokio::time::{Duration, Instant};
use tracing::{error, info, warn};
use utils::serialization_utils::read_u32;

use super::direct_access_client::DirectAccessClient;
use crate::Client;
use crate::adaptive_concurrency::AdaptiveConcurrencyController;
use crate::error::{CasClientError, Result};
use crate::progress_tracked_streams::ProgressCallback;

lazy_static! {
    /// Reference instant for URL timestamps. Initialized far in the past to allow
    /// testing timestamps that are earlier in the current process lifetime.
    static ref REFERENCE_INSTANT: Instant = {
        let now = Instant::now();
        now.checked_sub(Duration::from_secs(365 * 24 * 60 * 60))
            .unwrap_or(now)
    };
}

pub struct LocalClient {
    // Note: Field order matters for Drop! heed::Env must be dropped before _tmp_dir
    // because heed holds file handles that need to be closed before the directory is deleted.
    // We use Option<heed::Env> so we can take() it in Drop to properly close via prepare_for_closing.
    global_dedup_db_env: Option<heed::Env>,
    global_dedup_table: heed::Database<OwnedType<MerkleHash>, OwnedType<MerkleHash>>,
    shard_manager: Arc<ShardFileManager>,
    xorb_dir: PathBuf,
    shard_dir: PathBuf,
    upload_concurrency_controller: Arc<AdaptiveConcurrencyController>,
    url_expiration_ms: AtomicU64,
    /// API delay range in milliseconds as (min_ms, max_ms). (0, 0) means disabled.
    random_ms_delay_window: (AtomicU64, AtomicU64),
    _tmp_dir: Option<TempDir>, // Must be last - dropped after heed env is closed
}

impl LocalClient {
    /// Create a local client hosted in a temporary directory for testing.
    /// This is an async function to allow use with current-thread tokio runtime.
    pub async fn temporary() -> Result<Arc<Self>> {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().to_owned();
        let s = Self::new_internal(path, Some(tmp_dir)).await?;
        Ok(Arc::new(s))
    }

    /// Create a local client hosted in a directory.  Effectively, this directory
    /// is the CAS endpoint and persists across instances of LocalClient.  
    pub async fn new(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let path = path.as_ref().to_owned();
        Ok(Arc::new(Self::new_internal(path, None).await?))
    }

    async fn new_internal(path: impl AsRef<Path>, tmp_dir: Option<TempDir>) -> Result<Self> {
        let base_dir = std::path::absolute(path)?;
        if !base_dir.exists() {
            std::fs::create_dir_all(&base_dir)?;
        }

        let shard_dir = base_dir.join("shards");
        if !shard_dir.exists() {
            std::fs::create_dir_all(&shard_dir)?;
        }

        let xorb_dir = base_dir.join("xorbs");
        if !xorb_dir.exists() {
            std::fs::create_dir_all(&xorb_dir)?;
        }

        let global_dedup_dir = base_dir.join("global_dedup_lookup.db");
        if !global_dedup_dir.exists() {
            std::fs::create_dir_all(&global_dedup_dir)?;
        }

        // Open / set up the global dedup lookup.
        let global_dedup_db_env = heed::EnvOpenOptions::new()
            .open(&global_dedup_dir)
            .map_err(|e| CasClientError::Other(format!("Error opening db at {global_dedup_dir:?}: {e}")))?;

        let global_dedup_table = global_dedup_db_env
            .create_database(None)
            .map_err(|e| CasClientError::Other(format!("Error opening heed table: {e}")))?;

        // Open / set up the shard lookup
        let shard_manager = ShardFileManager::new_in_session_directory(shard_dir.clone(), true).await?;

        Ok(Self {
            global_dedup_db_env: Some(global_dedup_db_env),
            global_dedup_table,
            shard_manager,
            xorb_dir,
            shard_dir,
            upload_concurrency_controller: AdaptiveConcurrencyController::new_upload("local_uploads"),
            url_expiration_ms: AtomicU64::new(u64::MAX),
            random_ms_delay_window: (AtomicU64::new(0), AtomicU64::new(0)),
            _tmp_dir: tmp_dir, // Must be last - dropped after heed env is closed
        })
    }

    /// Internal function to get the path for a given hash entry
    fn get_path_for_entry(&self, hash: &MerkleHash) -> PathBuf {
        self.xorb_dir.join(format!("default.{hash:?}"))
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

impl Drop for LocalClient {
    fn drop(&mut self) {
        // Properly close the heed environment by calling prepare_for_closing.
        // This removes the environment from heed's global OPENED_ENV cache,
        // allowing the file handles to be released. Without this, the cached
        // environment reference prevents the file descriptors from being closed.
        if let Some(env) = self.global_dedup_db_env.take() {
            let closing_event = env.prepare_for_closing();
            closing_event.wait();
        }
    }
}

#[async_trait]
impl DirectAccessClient for LocalClient {
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
        let mut ret = Vec::new();
        self.xorb_dir
            .read_dir()
            .map_err(CasClientError::internal)?
            .filter_map(|x| x.ok())
            .filter_map(|x| x.file_name().into_string().ok())
            .for_each(|x| {
                if let Some(pos) = x.rfind('.') {
                    let hash = &x[(pos + 1)..];
                    if let Ok(hash) = MerkleHash::from_hex(hash) {
                        ret.push(hash);
                    }
                }
            });
        Ok(ret)
    }

    async fn delete_xorb(&self, hash: &MerkleHash) {
        let file_path = self.get_path_for_entry(hash);

        // unset read-only for Windows to delete
        #[cfg(windows)]
        {
            if let Ok(metadata) = std::fs::metadata(&file_path) {
                let mut permissions = metadata.permissions();
                #[allow(clippy::permissions_set_readonly_false)]
                permissions.set_readonly(false);
                let _ = std::fs::set_permissions(&file_path, permissions);
            }
        }

        let _ = std::fs::remove_file(file_path);
    }

    async fn get_full_xorb(&self, hash: &MerkleHash) -> Result<Bytes> {
        let file_path = self.get_path_for_entry(hash);
        let file = File::open(&file_path).map_err(|_| {
            error!("Unable to find file in local CAS {:?}", file_path);
            CasClientError::XORBNotFound(*hash)
        })?;

        let mut reader = BufReader::new(file);
        let cas = CasObject::deserialize(&mut reader)?;
        let result = cas.get_all_bytes(&mut reader)?;
        Ok(Bytes::from(result))
    }

    async fn get_xorb_ranges(&self, hash: &MerkleHash, chunk_ranges: Vec<(u32, u32)>) -> Result<Vec<Bytes>> {
        if chunk_ranges.is_empty() {
            return Ok(vec![Bytes::new()]);
        }

        let file_path = self.get_path_for_entry(hash);
        let file = File::open(&file_path).map_err(|_| {
            error!("Unable to find file in local CAS {:?}", file_path);
            CasClientError::XORBNotFound(*hash)
        })?;

        let mut reader = BufReader::new(file);
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
        let file_path = self.get_path_for_entry(hash);
        match File::open(file_path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);
                let cas = CasObject::deserialize(&mut reader)?;
                let length = cas.get_all_bytes(&mut reader)?.len();
                Ok(length as u32)
            },
            Err(_) => Err(CasClientError::XORBNotFound(*hash)),
        }
    }

    async fn xorb_exists(&self, hash: &MerkleHash) -> Result<bool> {
        let file_path = self.get_path_for_entry(hash);

        let Ok(md) = metadata(&file_path) else {
            return Ok(false);
        };

        if !md.is_file() {
            return Err(CasClientError::internal(format!(
                "Attempting to write to {file_path:?}, but it is not a file"
            )));
        }

        let Ok(file) = File::open(&file_path) else {
            return Err(CasClientError::XORBNotFound(*hash));
        };

        let mut reader = BufReader::new(file);
        CasObject::deserialize(&mut reader)?;
        Ok(true)
    }

    async fn xorb_footer(&self, hash: &MerkleHash) -> Result<CasObject> {
        let file_path = self.get_path_for_entry(hash);
        let mut file = File::open(&file_path).map_err(|_| {
            error!("Unable to find xorb in local CAS {:?}", file_path);
            CasClientError::XORBNotFound(*hash)
        })?;

        file.seek(SeekFrom::End(-(size_of::<u32>() as i64)))?;
        let info_length = read_u32(&mut file)?;

        file.seek(SeekFrom::End(-(info_length as i64)))?;

        let mut reader = BufReader::new(file);
        let cas_object = CasObject::deserialize(&mut reader)?;
        Ok(cas_object)
    }

    async fn get_file_size(&self, hash: &MerkleHash) -> Result<u64> {
        let file_info = self.shard_manager.get_file_reconstruction_info(hash).await?;
        Ok(file_info.unwrap().0.file_size())
    }

    async fn get_file_data(&self, hash: &MerkleHash, byte_range: Option<FileRange>) -> Result<Bytes> {
        let Some((file_info, _)) = self
            .shard_manager
            .get_file_reconstruction_info(hash)
            .await
            .map_err(|e| anyhow!("{e}"))?
        else {
            return Err(CasClientError::FileNotFound(*hash));
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
        let file_path = self.get_path_for_entry(hash);
        let data = std::fs::read(&file_path).map_err(|_| CasClientError::XORBNotFound(*hash))?;

        let start = byte_range.as_ref().map(|r| r.start as usize).unwrap_or(0);
        let end = byte_range
            .as_ref()
            .map(|r| r.end as usize)
            .unwrap_or(data.len())
            .min(data.len());

        if start >= data.len() {
            return Err(CasClientError::InvalidRange);
        }

        Ok(Bytes::from(data[start..end].to_vec()))
    }

    async fn xorb_raw_length(&self, hash: &MerkleHash) -> Result<u64> {
        let file_path = self.get_path_for_entry(hash);
        let metadata = std::fs::metadata(&file_path).map_err(|_| CasClientError::XORBNotFound(*hash))?;
        Ok(metadata.len())
    }

    async fn fetch_term_data(
        &self,
        hash: MerkleHash,
        fetch_term: CASReconstructionFetchInfo,
    ) -> Result<(Bytes, Vec<u32>)> {
        self.apply_api_delay().await;
        let (file_path, url_byte_range, url_timestamp) = parse_fetch_url(&fetch_term.url)?;

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
        let file = File::open(&file_path).map_err(|_| {
            error!("Unable to find xorb in local CAS {:?}", file_path);
            CasClientError::XORBNotFound(hash)
        })?;

        let mut reader = BufReader::new(file);
        let cas = CasObject::deserialize(&mut reader)?;

        let data = cas.get_bytes_by_chunk_range(&mut reader, fetch_term.range.start, fetch_term.range.end)?;

        let chunk_byte_indices = {
            let mut indices = Vec::new();
            let mut cumulative = 0u32;
            // Start with 0, matching the format from deserialize_chunks_from_stream
            indices.push(0);
            // ChunkRange is exclusive-end, so we iterate from start to end (exclusive)
            for chunk_idx in fetch_term.range.start..fetch_term.range.end {
                let chunk_len = cas
                    .uncompressed_chunk_length(chunk_idx)
                    .map_err(|e| CasClientError::Other(format!("Failed to get chunk length: {e}")))?;
                cumulative += chunk_len;
                indices.push(cumulative);
            }
            indices
        };

        Ok((data.into(), chunk_byte_indices))
    }
}

/// LocalClient is responsible for writing/reading Xorbs on the local disk.
#[async_trait]
impl Client for LocalClient {
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        self.apply_api_delay().await;
        Ok(self.shard_manager.get_file_reconstruction_info(file_hash).await?)
    }

    async fn query_for_global_dedup_shard(&self, _prefix: &str, chunk_hash: &MerkleHash) -> Result<Option<Bytes>> {
        self.apply_api_delay().await;
        let env = self
            .global_dedup_db_env
            .as_ref()
            .ok_or_else(|| CasClientError::Other("LocalClient has been closed".to_string()))?;
        let read_txn = env.read_txn().map_err(map_heed_db_error)?;

        if let Some(shard) = self.global_dedup_table.get(&read_txn, chunk_hash).map_err(map_heed_db_error)? {
            let filename = self.shard_dir.join(shard_file_name(&shard));
            return Ok(Some(std::fs::read(filename)?.into()));
        }
        Ok(None)
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

        // Rebuild a full in-memory shard to rebuild the new shard.  Quick and convenient.
        let mut in_memory_shard = MDBInMemoryShard::default();

        // Add file info from the views
        for i in 0..minimal_shard.num_files() {
            let file_view = minimal_shard.file(i).unwrap();
            in_memory_shard.add_file_reconstruction_info(MDBFileInfo::from(file_view))?;
        }

        // Add CAS info from the views
        for i in 0..minimal_shard.num_cas() {
            let cas_view = minimal_shard.cas(i).unwrap();
            in_memory_shard.add_cas_block(MDBCASInfo::from(cas_view))?;
        }

        // Write the rebuilt shard to disk (creates proper lookup tables)
        let shard_path = in_memory_shard.write_to_directory(&self.shard_dir, None)?;
        let shard = MDBShardFile::load_from_file(&shard_path)?;
        let shard_hash = shard.shard_hash;

        self.shard_manager.register_shards(&[shard]).await?;

        // Get global dedup chunks from the minimal shard
        let chunk_hashes = minimal_shard.global_dedup_eligible_chunks();

        let env = self
            .global_dedup_db_env
            .as_ref()
            .ok_or_else(|| CasClientError::Other("LocalClient has been closed".to_string()))?;
        let mut write_txn = env.write_txn().map_err(map_heed_db_error)?;

        for chunk in chunk_hashes {
            self.global_dedup_table
                .put(&mut write_txn, &chunk, &shard_hash)
                .map_err(map_heed_db_error)?;
        }

        write_txn.commit().map_err(map_heed_db_error)?;

        Ok(true)
    }

    async fn upload_xorb(
        &self,
        _prefix: &str,
        serialized_cas_object: SerializedCasObject,
        progress_callback: Option<ProgressCallback>,
        _permit: crate::adaptive_concurrency::ConnectionPermit,
    ) -> Result<u64> {
        self.apply_api_delay().await;
        let hash = serialized_cas_object.hash;
        let footer_start = serialized_cas_object.footer_start;
        let serialized_data = serialized_cas_object.serialized_data;
        if self.xorb_exists(&hash).await? {
            info!("object {hash:?} already exists in Local CAS; returning.");
            return Ok(0);
        }

        // Reconstruct footer if not present
        let data_to_write = if footer_start.is_some() {
            serialized_data
        } else {
            let mut data_with_footer = Vec::new();
            let (_, computed_hash) = cas_object::reconstruct_xorb_with_footer(&mut data_with_footer, &serialized_data)?;
            if computed_hash != hash {
                return Err(CasClientError::Other(format!(
                    "XORB hash mismatch: expected {}, got {}",
                    hash.hex(),
                    computed_hash.hex(),
                )));
            }
            data_with_footer
        };

        let file_path = self.get_path_for_entry(&hash);
        info!("Writing XORB {hash:?} to local path {file_path:?}");

        let total = data_to_write.len() as u64;
        let mut file = SafeFileCreator::new(&file_path)?;

        for i in 0..10 {
            let start = (i * data_to_write.len()) / 10;
            let end = ((i + 1) * data_to_write.len()) / 10;
            let chunk_len = end - start;

            file.write_all(&data_to_write[start..end])?;

            if let Some(ref cb) = progress_callback {
                let completed = end as u64;
                let delta = chunk_len as u64;
                cb(delta, completed, total);
            }
        }

        let bytes_written = data_to_write.len();
        file.close()?;

        #[cfg(unix)]
        if let Ok(metadata) = metadata(&file_path) {
            let mut permissions = metadata.permissions();
            permissions.set_readonly(true);
            let _ = std::fs::set_permissions(&file_path, permissions);
        }

        info!("{file_path:?} successfully written with {bytes_written} bytes.");

        Ok(bytes_written as u64)
    }

    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>> {
        self.apply_api_delay().await;
        let Some((file_info, _)) = self.shard_manager.get_file_reconstruction_info(file_id).await? else {
            return Ok(None);
        };

        // Calculate total file size from segments
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
            // Truncate end if it extends beyond file size
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

        // First skip file segments until we find the first one that starts before the file range start
        let mut s_idx = 0;
        let mut cumulative_bytes = 0u64;
        let mut first_chunk_byte_start;

        loop {
            if s_idx >= file_info.segments.len() {
                // We have here that the requested file range is out of bounds,
                // so return a range error.
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

        // Now, prepare the response by iterating over the segments and
        // adding the terms and fetch info to the response.
        let mut terms = Vec::new();

        #[derive(Clone)]
        struct FetchInfoIntermediate {
            chunk_range: ChunkRange,
            byte_range: FileRange,
        }

        let mut fetch_info_map: HashMap<MerkleHash, Vec<FetchInfoIntermediate>> = HashMap::new();

        while s_idx < file_info.segments.len() && cumulative_bytes < file_range.end {
            let mut segment = file_info.segments[s_idx].clone();
            let mut chunk_range = ChunkRange::new(segment.chunk_index_start, segment.chunk_index_end);

            // Now get the URL for this segment, which involves reading the actual byte range there.
            let xorb_footer = self.xorb_footer(&segment.cas_hash).await?;

            // Do we need to prune the first segment on chunk boundaries to align with the range given?
            if cumulative_bytes < file_range.start {
                while chunk_range.start < chunk_range.end {
                    let next_chunk_size = xorb_footer.uncompressed_chunk_length(chunk_range.start)? as u64;

                    if cumulative_bytes + next_chunk_size <= file_range.start {
                        cumulative_bytes += next_chunk_size;
                        first_chunk_byte_start += next_chunk_size;
                        segment.unpacked_segment_bytes -= next_chunk_size as u32;

                        chunk_range.start += 1;

                        // Should find it somewhere in here.
                        debug_assert_lt!(chunk_range.start, chunk_range.end);
                    } else {
                        break;
                    }
                }
            }

            // Do we need to prune the last segment on chunk boundaries to align with the range given?
            if cumulative_bytes + segment.unpacked_segment_bytes as u64 > file_range.end {
                while chunk_range.end > chunk_range.start {
                    let last_chunk_size = xorb_footer.uncompressed_chunk_length(chunk_range.end - 1)?;

                    if cumulative_bytes + (segment.unpacked_segment_bytes - last_chunk_size) as u64 >= file_range.end {
                        // We can cut the last chunk off and still contain the requested range.
                        chunk_range.end -= 1;
                        segment.unpacked_segment_bytes -= last_chunk_size;
                        debug_assert_lt!(chunk_range.start, chunk_range.end);
                        debug_assert_gt!(segment.unpacked_segment_bytes, 0);
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

            let fetch_info_intemediate = FetchInfoIntermediate {
                chunk_range,
                byte_range,
            };

            fetch_info_map.entry(segment.cas_hash).or_default().push(fetch_info_intemediate);

            cumulative_bytes += segment.unpacked_segment_bytes as u64;
            s_idx += 1;
        }

        assert!(!terms.is_empty());

        let timestamp = Instant::now();

        // Sort and merge adjacent/overlapping ranges in each fetch_info Vec
        let mut merged_fetch_info_map: HashMap<HexMerkleHash, Vec<CASReconstructionFetchInfo>> = HashMap::new();
        for (hash, mut fi_vec) in fetch_info_map {
            // Sort by url_range.start
            fi_vec.sort_by_key(|fi| fi.chunk_range.start);
            let file_path = self.get_path_for_entry(&hash);

            // Merge adjacent or overlapping ranges
            let mut merged: Vec<CASReconstructionFetchInfo> = Vec::new();
            let mut idx = 0;

            while idx < fi_vec.len() {
                // Go through and merge adjascent or overlapping ranges,
                // then form the full CASReconstructionFetchInfo structs.
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
                    url: generate_fetch_url(&file_path, &new_fi.byte_range, timestamp),
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
        progress_callback: Option<ProgressCallback>,
        uncompressed_size_if_known: Option<usize>,
    ) -> Result<(Bytes, Vec<u32>)> {
        // Retry loop: try to fetch, and if URL expired, refresh and retry once.
        for attempt in 0..2 {
            self.apply_api_delay().await;
            let (url, range) = url_info.retrieve_url().await?;
            let (file_path, _url_byte_range, url_timestamp) = parse_fetch_url(&url)?;

            // Check if URL has expired
            let expiration_ms = self.url_expiration_ms.load(Ordering::Relaxed);
            let elapsed_ms = Instant::now().saturating_duration_since(url_timestamp).as_millis() as u64;
            if elapsed_ms > expiration_ms {
                if attempt == 0 {
                    // First attempt failed due to expiration - refresh URL and retry.
                    url_info.refresh_url().await?;
                    continue;
                }
                return Err(CasClientError::PresignedUrlExpirationError);
            }

            // Read the byte range from the file and deserialize
            let mut file = File::open(&file_path).map_err(|_| CasClientError::XORBNotFound(MerkleHash::default()))?;
            let start = range.start;
            let end = range.end + 1; // HttpRange is inclusive end
            file.seek(SeekFrom::Start(start))?;
            let len = (end - start) as usize;
            let mut data = vec![0u8; len];
            std::io::Read::read_exact(&mut file, &mut data)?;

            // Deserialize the chunks from the raw CAS data
            let (decompressed_data, chunk_byte_indices) = cas_object::deserialize_chunks(&mut Cursor::new(&data))?;

            if let Some(expected) = uncompressed_size_if_known {
                debug_assert_eq!(
                    decompressed_data.len(),
                    expected,
                    "get_file_term_data: expected {} bytes, got {}",
                    expected,
                    decompressed_data.len()
                );
            }

            let transfer_len = len as u64;
            if let Some(ref cb) = progress_callback {
                cb(transfer_len, transfer_len, transfer_len);
            }
            return Ok((Bytes::from(decompressed_data), chunk_byte_indices));
        }

        // Should not reach here, but return error if we do.
        Err(CasClientError::PresignedUrlExpirationError)
    }
}

fn map_heed_db_error(e: heed::Error) -> CasClientError {
    let msg = format!("Global shard dedup database error: {e:?}");
    warn!("{msg}");
    CasClientError::Other(msg)
}

fn generate_fetch_url(file_path: &Path, byte_range: &FileRange, timestamp: Instant) -> String {
    let timestamp_ms = timestamp.saturating_duration_since(*REFERENCE_INSTANT).as_millis() as u64;
    format!("{:?}:{}:{}:{}", file_path, byte_range.start, byte_range.end, timestamp_ms)
}

fn parse_fetch_url(url: &str) -> Result<(PathBuf, FileRange, Instant)> {
    let mut parts = url.rsplitn(4, ':').collect::<Vec<_>>();
    parts.reverse();

    if parts.len() != 4 {
        return Err(CasClientError::InvalidArguments);
    }

    let file_path_str = parts[0];
    let start_pos: u64 = parts[1].parse().map_err(|_| CasClientError::InvalidArguments)?;
    let end_pos: u64 = parts[2].parse().map_err(|_| CasClientError::InvalidArguments)?;
    let timestamp_ms: u64 = parts[3].parse().map_err(|_| CasClientError::InvalidArguments)?;

    let file_path: PathBuf = file_path_str.trim_matches('"').into();
    let byte_range = FileRange::new(start_pos, end_pos);
    let timestamp = *REFERENCE_INSTANT + Duration::from_millis(timestamp_ms);

    Ok((file_path, byte_range, timestamp))
}
#[cfg(test)]
mod tests {
    use cas_object::test_utils::*;
    use cas_types::CASReconstructionFetchInfo;

    use super::*;

    /// Runs the common TestingClient trait test suite for LocalClient.
    #[tokio::test]
    async fn test_common_client_suite() {
        super::super::client_unit_testing::test_client_functionality(|| async {
            LocalClient::temporary().await.unwrap() as std::sync::Arc<dyn super::super::DirectAccessClient>
        })
        .await;
    }

    #[tokio::test]
    async fn test_download_fetch_term_data_validation() {
        // Setup: Create a client and upload a xorb
        let xorb = build_raw_xorb(3, ChunkSize::Fixed(2048));
        let cas_object = build_and_verify_cas_object(xorb, None);
        let hash = cas_object.hash;

        let client = LocalClient::temporary().await.unwrap();
        let permit = client.acquire_upload_permit().await.unwrap();
        client.upload_xorb("default", cas_object, None, permit).await.unwrap();

        // Get the actual byte offsets for a chunk range
        let file_path = client.get_path_for_entry(&hash);
        let file = File::open(&file_path).unwrap();
        let mut reader = BufReader::new(file);
        let cas = CasObject::deserialize(&mut reader).unwrap();
        let (fetch_byte_start, fetch_byte_end) = cas.get_byte_offset(0, 1).unwrap();

        let timestamp = Instant::now();
        let byte_range = FileRange::new(fetch_byte_start as u64, fetch_byte_end as u64);
        let valid_url = generate_fetch_url(&file_path, &byte_range, timestamp);
        // HttpRange uses inclusive end, FileRange uses exclusive end
        let valid_url_range = HttpRange::from(byte_range);

        // Test 1: Valid URL and fetch_term should succeed
        let valid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: valid_url.clone(),
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, valid_fetch_term).await;
        assert!(result.is_ok(), "Valid fetch_term should succeed");

        // Test 2: Invalid URL format - too few parts (3 instead of 4)
        let too_few_parts = "filename:123:456";
        let invalid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: too_few_parts.to_string(),
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "URL with too few parts should fail");
        assert!(matches!(result.unwrap_err(), CasClientError::InvalidArguments));

        // Test 3: Invalid start_pos - doesn't match url_range.start
        let wrong_byte_range = FileRange::new(fetch_byte_start as u64 + 1, fetch_byte_end as u64);
        let wrong_start_pos = generate_fetch_url(&file_path, &wrong_byte_range, timestamp);
        let invalid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: wrong_start_pos,
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "Wrong start_pos should fail");
        assert!(matches!(result.unwrap_err(), CasClientError::InvalidArguments));

        // Test 4: Invalid end_pos - doesn't match url_range.end
        let wrong_byte_range = FileRange::new(fetch_byte_start as u64, fetch_byte_end as u64 + 1);
        let wrong_end_pos = generate_fetch_url(&file_path, &wrong_byte_range, timestamp);
        let invalid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: wrong_end_pos,
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "Wrong end_pos should fail");
        assert!(matches!(result.unwrap_err(), CasClientError::InvalidArguments));

        // Test 5: Invalid start_pos - non-numeric
        let timestamp_ms = timestamp.saturating_duration_since(*REFERENCE_INSTANT).as_millis() as u64;
        let non_numeric_start = format!("{:?}:not_a_number:{}:{}", file_path, fetch_byte_end, timestamp_ms);
        let invalid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: non_numeric_start,
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "Non-numeric start_pos should fail");
        assert!(matches!(result.unwrap_err(), CasClientError::InvalidArguments));

        // Test 6: Invalid end_pos - non-numeric
        let non_numeric_end = format!("{:?}:{}:not_a_number:{}", file_path, fetch_byte_start, timestamp_ms);
        let invalid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: non_numeric_end,
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "Non-numeric end_pos should fail");
        assert!(matches!(result.unwrap_err(), CasClientError::InvalidArguments));

        // Test 7: Empty URL
        let invalid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: String::new(),
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "Empty URL should fail");
        assert!(matches!(result.unwrap_err(), CasClientError::InvalidArguments));

        // Test 8: Invalid timestamp - non-numeric
        let non_numeric_timestamp = format!("{:?}:{}:{}:not_a_number", file_path, fetch_byte_start, fetch_byte_end);
        let invalid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: non_numeric_timestamp,
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "Non-numeric timestamp should fail");
        assert!(matches!(result.unwrap_err(), CasClientError::InvalidArguments));

        // Test 9: Non-existent file path
        let non_existent_path = PathBuf::from("/nonexistent/path/file.xorb");
        let non_existent_url = generate_fetch_url(&non_existent_path, &byte_range, timestamp);
        let invalid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: non_existent_url,
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "Non-existent file should fail");
    }

    #[tokio::test(start_paused = true)]
    async fn test_url_expiration() {
        super::super::client_unit_testing::test_url_expiration_functionality(|| async {
            LocalClient::temporary().await.unwrap() as std::sync::Arc<dyn super::super::DirectAccessClient>
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_api_delay() {
        super::super::client_unit_testing::test_api_delay_functionality(|| async {
            LocalClient::temporary().await.unwrap() as std::sync::Arc<dyn super::super::DirectAccessClient>
        })
        .await;
    }
}
