use std::collections::HashMap;
use std::fs::{File, metadata};
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use cas_object::{CasObject, SerializedCasObject};
use cas_types::{
    BatchQueryReconstructionResponse, CASReconstructionFetchInfo, CASReconstructionTerm, ChunkRange, FileRange,
    HexMerkleHash, HttpRange, Key, QueryReconstructionResponse,
};
use file_utils::SafeFileCreator;
use heed::types::*;
use lazy_static::lazy_static;
use mdb_shard::file_structs::MDBFileInfo;
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use mdb_shard::utils::shard_file_name;
use mdb_shard::{MDBShardFile, MDBShardInfo, ShardFileManager};
use merklehash::MerkleHash;
use more_asserts::*;
use progress_tracking::upload_tracking::CompletionTracker;
use tempfile::TempDir;
use tokio::runtime::Handle;
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info, warn};
use utils::serialization_utils::read_u32;

use crate::Client;
use crate::adaptive_concurrency::{AdaptiveConcurrencyController, ConnectionPermit};
use crate::error::{CasClientError, Result};
use crate::file_reconstruction_v1::TermDownloadOutput;
use crate::interface::URLProvider;

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
    download_concurrency_controller: Arc<AdaptiveConcurrencyController>,
    url_expiration_ms: AtomicU64,
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
    pub fn new(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let path = path.as_ref().to_owned();
        let s = tokio::task::block_in_place(|| {
            Handle::current().block_on(async move { Self::new_internal(path, None).await })
        })?;
        Ok(Arc::new(s))
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
        // Use minimal settings to reduce file handle usage:
        // - max_dbs(1): We only use one database
        // - max_readers(4): Sufficient for local client usage, reduces reader table file size
        let global_dedup_db_env = heed::EnvOpenOptions::new()
            .max_dbs(1)
            .max_readers(4)
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
            download_concurrency_controller: AdaptiveConcurrencyController::new_download("local_downloads"),
            url_expiration_ms: AtomicU64::new(u64::MAX),
            _tmp_dir: tmp_dir, // Must be last - dropped after heed env is closed
        })
    }

    /// Internal function to get the path for a given hash entry
    fn get_path_for_entry(&self, hash: &MerkleHash) -> PathBuf {
        self.xorb_dir.join(format!("default.{hash:?}"))
    }

    /// Sets the expiration duration for fetch term URLs.
    /// URLs generated after this call will expire after the specified duration.
    pub fn set_fetch_term_url_expiration(&self, expiration: Duration) {
        self.url_expiration_ms.store(expiration.as_millis() as u64, Ordering::Relaxed);
    }

    /// Returns all entries in the local client
    pub fn get_all_entries(&self) -> Result<Vec<Key>> {
        let mut ret: Vec<_> = Vec::new();

        // loop through the directory
        self.xorb_dir
            .read_dir()
            .map_err(CasClientError::internal)?
            // take only entries which are ok
            .filter_map(|x| x.ok())
            // take only entries whose filenames convert into strings
            .filter_map(|x| x.file_name().into_string().ok())
            .for_each(|x| {
                let mut is_okay = false;

                // try to split the string with the path format [prefix].[hash]
                if let Some(pos) = x.rfind('.') {
                    let prefix = &x[..pos];
                    let hash = &x[(pos + 1)..];

                    if let Ok(hash) = MerkleHash::from_hex(hash) {
                        ret.push(Key {
                            prefix: prefix.into(),
                            hash,
                        });
                        is_okay = true;
                    }
                }
                if !is_okay {
                    debug!("File '{x:?}' in staging area not in valid format, ignoring.");
                }
            });
        Ok(ret)
    }

    /// Deletes an entry
    pub fn delete(&self, hash: &MerkleHash) {
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

    pub fn get(&self, hash: &MerkleHash) -> Result<Vec<u8>> {
        let file_path = self.get_path_for_entry(hash);
        let file = File::open(&file_path).map_err(|_| {
            error!("Unable to find file in local CAS {:?}", file_path);
            CasClientError::XORBNotFound(*hash)
        })?;

        let mut reader = BufReader::new(file);
        let cas = CasObject::deserialize(&mut reader)?;
        let result = cas.get_all_bytes(&mut reader)?;
        Ok(result)
    }

    /// Get uncompressed bytes from a CAS object within chunk ranges.
    /// Each tuple in chunk_ranges represents a chunk index range [a, b)
    fn get_object_range(&self, hash: &MerkleHash, chunk_ranges: Vec<(u32, u32)>) -> Result<Vec<Vec<u8>>> {
        // Handle the case where we aren't asked for any real data.
        if chunk_ranges.is_empty() {
            return Ok(vec![vec![]]);
        }

        let file_path = self.get_path_for_entry(hash);
        let file = File::open(&file_path).map_err(|_| {
            error!("Unable to find file in local CAS {:?}", file_path);
            CasClientError::XORBNotFound(*hash)
        })?;

        let mut reader = BufReader::new(file);
        let cas = CasObject::deserialize(&mut reader)?;

        let mut ret: Vec<Vec<u8>> = Vec::new();
        for r in chunk_ranges {
            if r.0 >= r.1 {
                ret.push(vec![]);
                continue;
            }

            let data = cas.get_bytes_by_chunk_range(&mut reader, r.0, r.1)?;
            ret.push(data);
        }
        Ok(ret)
    }

    #[cfg(test)]
    fn get_length(&self, hash: &MerkleHash) -> Result<u32> {
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

    async fn xorb_exists(&self, _prefix: &str, hash: &MerkleHash) -> Result<bool> {
        let file_path = self.get_path_for_entry(hash);

        let Ok(md) = metadata(&file_path) else {
            return Ok(false);
        };

        if !md.is_file() {
            return Err(CasClientError::internal(format!(
                "Attempting to write to {file_path:?}, but it is not a file"
            )));
        }

        let Ok(file) = File::open(file_path) else {
            return Err(CasClientError::XORBNotFound(*hash));
        };

        let mut reader = BufReader::new(file);
        CasObject::deserialize(&mut reader)?;
        Ok(true)
    }

    fn read_xorb_footer(&self, hash: &MerkleHash) -> Result<CasObject> {
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

    /// Get file term data for the v1 API (used by FileReconstructorV1).
    pub fn get_file_term_data_v1(
        &self,
        hash: MerkleHash,
        fetch_term: CASReconstructionFetchInfo,
    ) -> Result<TermDownloadOutput> {
        let (file_path, url_byte_range, url_timestamp) = parse_fetch_url(&fetch_term.url)?;

        // Check if URL has expired
        let expiration_ms = self.url_expiration_ms.load(Ordering::Relaxed);
        let elapsed_ms = Instant::now().saturating_duration_since(url_timestamp).as_millis() as u64;
        if elapsed_ms > expiration_ms {
            return Err(CasClientError::PresignedUrlExpirationError);
        }

        // Validate byte range matches url_range
        // url_byte_range is FileRange (exclusive-end), url_range is HttpRange (inclusive-end)
        let url_http_range = HttpRange::from(url_byte_range);
        if url_http_range.start != fetch_term.url_range.start || url_http_range.end != fetch_term.url_range.end {
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

        Ok(TermDownloadOutput {
            data,
            chunk_byte_indices,
            chunk_range: fetch_term.range,
        })
    }

    async fn read_term_data_from_file(&self, file_path: &Path, byte_range: FileRange) -> Result<(Bytes, Vec<u32>)> {
        let mut file = File::open(file_path).map_err(|_| {
            error!("Unable to find xorb in local CAS {:?}", file_path);
            CasClientError::InvalidArguments
        })?;

        file.seek(SeekFrom::Start(byte_range.start))?;

        let bytes_to_read = (byte_range.end - byte_range.start) as usize;
        let mut buffer = vec![0u8; bytes_to_read];
        file.read_exact(&mut buffer)?;

        let mut cursor = Cursor::new(buffer);
        let (data, chunk_byte_indices) = cas_object::deserialize_chunks(&mut cursor)?;

        Ok((Bytes::from(data), chunk_byte_indices))
    }

    pub async fn get_file_size(&self, hash: &MerkleHash) -> Result<u64> {
        let file_info = self.shard_manager.get_file_reconstruction_info(hash).await?;
        Ok(file_info.unwrap().0.file_size())
    }

    pub async fn get_file_data(&self, hash: &MerkleHash, byte_range: Option<FileRange>) -> Result<Vec<u8>> {
        let Some((file_info, _)) = self
            .shard_manager
            .get_file_reconstruction_info(hash)
            .await
            .map_err(|e| anyhow!("{e}"))?
        else {
            return Err(CasClientError::FileNotFound(*hash));
        };

        // This is just used for testing, so inefficient is fine.
        let mut file_vec = Vec::new();
        for entry in &file_info.segments {
            let mut entry_bytes = self
                .get_object_range(&entry.cas_hash, vec![(entry.chunk_index_start, entry.chunk_index_end)])?
                .pop()
                .unwrap();
            file_vec.append(&mut entry_bytes);
        }

        let file_size = file_vec.len();

        // Handle range validation and truncation
        let start = byte_range.as_ref().map(|range| range.start as usize).unwrap_or(0);

        // If the entire range is out of bounds, return InvalidRange error
        if byte_range.is_some() && start >= file_size {
            return Err(CasClientError::InvalidRange);
        }

        // Truncate end if it extends beyond file size
        let end = byte_range
            .as_ref()
            .map(|range| range.end as usize)
            .unwrap_or(file_size)
            .min(file_size);

        Ok(file_vec[start..end].to_vec())
    }
}

impl Drop for LocalClient {
    fn drop(&mut self) {
        // Properly close the heed environment by calling prepare_for_closing.
        // This removes the environment from heed's global OPENED_ENV cache,
        // allowing the file handles to be released. Without this, the cached
        // environment reference prevents the file descriptors from being closed.
        if let Some(env) = self.global_dedup_db_env.take() {
            let _closing_event = env.prepare_for_closing();
        }
    }
}

/// LocalClient is responsible for writing/reading Xorbs on the local disk.
#[async_trait]
impl Client for LocalClient {
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        Ok(self.shard_manager.get_file_reconstruction_info(file_hash).await?)
    }

    async fn query_for_global_dedup_shard(&self, _prefix: &str, chunk_hash: &MerkleHash) -> Result<Option<Bytes>> {
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

    async fn acquire_upload_permit(&self) -> Result<ConnectionPermit> {
        self.upload_concurrency_controller.acquire_connection_permit().await
    }

    async fn acquire_download_permit(&self) -> Result<ConnectionPermit> {
        self.download_concurrency_controller.acquire_connection_permit().await
    }

    async fn upload_shard(
        &self,
        shard_data: Bytes,
        _permit: crate::adaptive_concurrency::ConnectionPermit,
    ) -> Result<bool> {
        let shard = MDBShardFile::write_out_from_reader(&self.shard_dir, &mut Cursor::new(&shard_data))?;
        let shard_hash = shard.shard_hash;

        self.shard_manager.register_shards(&[shard]).await?;

        let mut shard_reader = Cursor::new(shard_data);
        let chunk_hashes = MDBShardInfo::filter_cas_chunks_for_global_dedup(&mut shard_reader)?;

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
        upload_tracker: Option<Arc<CompletionTracker>>,
        _permit: crate::adaptive_concurrency::ConnectionPermit,
    ) -> Result<u64> {
        let hash = &serialized_cas_object.hash;
        if self.xorb_exists("", hash).await? {
            info!("object {hash:?} already exists in Local CAS; returning.");
            return Ok(0);
        }

        let file_path = self.get_path_for_entry(hash);
        info!("Writing XORB {hash:?} to local path {file_path:?}");

        let mut file = SafeFileCreator::new(&file_path)?;

        for i in 0..10 {
            let start = (i * serialized_cas_object.serialized_data.len()) / 10;
            let end = ((i + 1) * serialized_cas_object.serialized_data.len()) / 10;

            file.write_all(&serialized_cas_object.serialized_data[start..end])?;

            if let Some(upload_tracker) = &upload_tracker {
                let adjusted_byte_start = (i * serialized_cas_object.raw_num_bytes as usize) / 10;
                let adjusted_byte_end = ((i + 1) * serialized_cas_object.raw_num_bytes as usize) / 10;
                let adjusted_progress = adjusted_byte_end - adjusted_byte_start;

                upload_tracker
                    .register_xorb_upload_progress(*hash, adjusted_progress as u64)
                    .await;
            }
        }

        let bytes_written = serialized_cas_object.serialized_data.len();
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

    fn use_xorb_footer(&self) -> bool {
        true
    }

    fn use_shard_footer(&self) -> bool {
        true
    }
    async fn get_reconstruction(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponse>> {
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
            let xorb_footer = self.read_xorb_footer(&segment.cas_hash)?;

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

    async fn get_file_term_data(
        &self,
        url_info: Box<dyn URLProvider>,
        _download_permit: ConnectionPermit,
    ) -> Result<(Bytes, Vec<u32>)> {
        const MAX_RETRIES: u32 = 3;

        let mut force_refresh = false;

        for attempt in 0..MAX_RETRIES {
            let (url, url_range) = url_info.retrieve_url(force_refresh).await?;
            let (file_path, file_fetch_range, url_timestamp) = parse_fetch_url(&url)?;

            // Check if URL has expired
            let expiration_ms = self.url_expiration_ms.load(Ordering::Relaxed);
            let elapsed_ms = Instant::now().saturating_duration_since(url_timestamp).as_millis() as u64;
            if elapsed_ms > expiration_ms {
                if attempt + 1 < MAX_RETRIES {
                    force_refresh = true;
                    continue;
                }
                return Err(CasClientError::PresignedUrlExpirationError);
            }

            // Validate byte range matches url_range
            // Note: url_range is an HttpRange (inclusive-end) while url_byte_range is FileRange (exclusive-end).
            // HttpRange::from(FileRange) subtracts 1 from end, so we need to account for this.
            let expected_range = FileRange::from(url_range);
            if file_fetch_range.start != expected_range.start || file_fetch_range.end != expected_range.end {
                return Err(CasClientError::InvalidArguments);
            }

            return self.read_term_data_from_file(&file_path, file_fetch_range).await;
        }

        Err(CasClientError::PresignedUrlExpirationError)
    }

    async fn get_file_term_data_v1(
        &self,
        hash: MerkleHash,
        fetch_term: CASReconstructionFetchInfo,
        _chunk_cache: Option<Arc<dyn chunk_cache::ChunkCache>>,
        _range_download_single_flight: crate::file_reconstruction_v1::RangeDownloadSingleFlight,
    ) -> Result<TermDownloadOutput> {
        // LocalClient reads from disk directly, caching is not needed
        LocalClient::get_file_term_data_v1(self, hash, fetch_term)
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
    use cas_object::CompressionScheme::LZ4;
    use cas_object::test_utils::*;
    use cas_types::CASReconstructionFetchInfo;
    use deduplication::test_utils::raw_xorb_to_vec;
    use mdb_shard::utils::parse_shard_filename;

    use super::*;
    use crate::client_testing_utils::ClientTestingUtils;

    #[tokio::test]
    async fn test_basic_put_get() {
        let xorb = build_raw_xorb(1, ChunkSize::Fixed(2048));
        let data = raw_xorb_to_vec(&xorb);

        let cas_object = build_and_verify_cas_object(xorb, None);
        let hash = cas_object.hash;

        // Act & Assert
        let client = LocalClient::temporary().await.unwrap();
        let permit = client.acquire_upload_permit().await.unwrap();
        assert!(client.upload_xorb("key", cas_object, None, permit).await.is_ok());

        let returned_data = client.get(&hash).unwrap();
        assert_eq!(data, returned_data);
    }

    #[tokio::test]
    async fn test_basic_put_get_random_medium() {
        let xorb = build_raw_xorb(44, ChunkSize::Random(512, 15633));
        let data = raw_xorb_to_vec(&xorb);

        let cas_object = build_and_verify_cas_object(xorb, Some(LZ4));
        let hash = cas_object.hash;

        // Act & Assert
        let client = LocalClient::temporary().await.unwrap();
        let permit = client.acquire_upload_permit().await.unwrap();
        assert!(client.upload_xorb("", cas_object, None, permit).await.is_ok());

        let returned_data = client.get(&hash).unwrap();
        assert_eq!(data, returned_data);
    }

    #[tokio::test]
    async fn test_basic_put_get_range_random_small() {
        let xorb = build_raw_xorb(3, ChunkSize::Random(512, 15633));
        let data = raw_xorb_to_vec(&xorb);
        let chunk_and_boundaries = xorb.cas_info.chunks_and_boundaries();

        let cas_object = build_and_verify_cas_object(xorb, Some(LZ4));
        let hash = cas_object.hash;

        // Act & Assert
        let client = LocalClient::temporary().await.unwrap();
        let permit = client.acquire_upload_permit().await.unwrap();
        assert!(client.upload_xorb("", cas_object, None, permit).await.is_ok());

        let ranges: Vec<(u32, u32)> = vec![(0, 1), (2, 3)];
        let returned_ranges = client.get_object_range(&hash, ranges).unwrap();

        let expected = [
            data[0..chunk_and_boundaries[0].1 as usize].to_vec(),
            data[chunk_and_boundaries[1].1 as usize..chunk_and_boundaries[2].1 as usize].to_vec(),
        ];

        for idx in 0..returned_ranges.len() {
            assert_eq!(expected[idx], returned_ranges[idx]);
        }
    }

    #[tokio::test]
    async fn test_basic_length() {
        let xorb = build_raw_xorb(1, ChunkSize::Fixed(2048));
        let data = raw_xorb_to_vec(&xorb);

        let cas_object = build_and_verify_cas_object(xorb, Some(LZ4));
        let hash = cas_object.hash;

        let gen_length = data.len();

        // Act
        let client = LocalClient::temporary().await.unwrap();
        let permit = client.acquire_upload_permit().await.unwrap();
        assert!(client.upload_xorb("", cas_object, None, permit).await.is_ok());
        let len = client.get_length(&hash).unwrap();

        // Assert
        assert_eq!(len as usize, gen_length);
    }

    #[tokio::test]
    async fn test_missing_xorb() {
        // Arrange
        let hash = MerkleHash::from_hex("d760aaf4beb07581956e24c847c47f1abd2e419166aa68259035bc412232e9da").unwrap();

        // Act & Assert
        let client = LocalClient::temporary().await.unwrap();
        let result = client.get(&hash);
        assert!(matches!(result, Err(CasClientError::XORBNotFound(_))));
    }

    #[tokio::test]
    async fn test_failures() {
        let hello = "hello world".as_bytes().to_vec();

        let hello_hash = merklehash::compute_data_hash(&hello[..]);

        let cas_object = serialized_cas_object_from_components(
            &hello_hash,
            hello.clone(),
            vec![(hello_hash, hello.len() as u32)],
            None,
        )
        .unwrap();

        // write "hello world"
        let client = LocalClient::temporary().await.unwrap();
        let permit = client.acquire_upload_permit().await.unwrap();
        client.upload_xorb("default", cas_object, None, permit).await.unwrap();

        let cas_object = serialized_cas_object_from_components(
            &hello_hash,
            hello.clone(),
            vec![(hello_hash, hello.len() as u32)],
            None,
        )
        .unwrap();

        // put the same value a second time. This should be ok.
        let permit2 = client.acquire_upload_permit().await.unwrap();
        client.upload_xorb("default", cas_object, None, permit2).await.unwrap();

        // we can list all entries
        let r = client.get_all_entries().unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(
            r,
            vec![Key {
                prefix: "default".into(),
                hash: hello_hash
            }]
        );

        // compute a hash of something we do not have in the store
        let world = "world".as_bytes().to_vec();
        let world_hash = merklehash::compute_data_hash(&world[..]);

        // get length of non-existent object should fail with XORBNotFound
        assert_eq!(CasClientError::XORBNotFound(world_hash), client.get_length(&world_hash).unwrap_err());

        // read of non-existent object should fail with XORBNotFound
        assert!(client.get(&world_hash).is_err());
        // read range of non-existent object should fail with XORBNotFound
        assert!(client.get_object_range(&world_hash, vec![(0, 5)]).is_err());

        // we can delete non-existent things
        client.delete(&world_hash);

        // delete the entry we inserted
        client.delete(&hello_hash);
        let r = client.get_all_entries().unwrap();
        assert_eq!(r.len(), 0);

        // now every read of that key should fail
        assert_eq!(CasClientError::XORBNotFound(hello_hash), client.get_length(&hello_hash).unwrap_err());
        assert_eq!(CasClientError::XORBNotFound(hello_hash), client.get(&hello_hash).unwrap_err());
    }

    #[tokio::test]
    async fn test_hashing() {
        // hand construct a tree of 2 chunks
        let hello = "hello".as_bytes().to_vec();
        let world = "world".as_bytes().to_vec();
        let hello_hash = merklehash::compute_data_hash(&hello[..]);
        let world_hash = merklehash::compute_data_hash(&world[..]);

        let final_hash = merklehash::xorb_hash(&[(hello_hash, 5), (world_hash, 5)]);

        // insert should succeed
        let client = LocalClient::temporary().await.unwrap();
        let permit = client.acquire_upload_permit().await.unwrap();
        client
            .upload_xorb(
                "key",
                serialized_cas_object_from_components(
                    &final_hash,
                    "helloworld".as_bytes().to_vec(),
                    vec![(hello_hash, 5), (world_hash, 10)],
                    None,
                )
                .unwrap(),
                None,
                permit,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_global_dedup() {
        let tmp_dir = TempDir::new().unwrap();
        let shard_dir_1 = tmp_dir.path().join("shard_1");
        std::fs::create_dir_all(&shard_dir_1).unwrap();
        let shard_dir_2 = tmp_dir.path().join("shard_2");
        std::fs::create_dir_all(&shard_dir_2).unwrap();

        let shard_in = mdb_shard::shard_format::test_routines::gen_random_shard_with_cas_references(
            0, &[16; 8], &[2; 20], true, true,
        )
        .unwrap();

        let new_shard_path = shard_in.write_to_directory(&shard_dir_1, None).unwrap();

        let shard_hash = parse_shard_filename(&new_shard_path).unwrap();

        let client = LocalClient::temporary().await.unwrap();

        let permit = client.acquire_upload_permit().await.unwrap();
        client
            .upload_shard(std::fs::read(&new_shard_path).unwrap().into(), permit)
            .await
            .unwrap();

        let dedup_hashes =
            MDBShardInfo::filter_cas_chunks_for_global_dedup(&mut File::open(&new_shard_path).unwrap()).unwrap();

        assert_ne!(dedup_hashes.len(), 0);

        // Now do the query...
        let new_shard = client
            .query_for_global_dedup_shard("default", &dedup_hashes[0])
            .await
            .unwrap()
            .unwrap();

        let sf = MDBShardFile::write_out_from_reader(shard_dir_2.clone(), &mut Cursor::new(new_shard)).unwrap();

        assert_eq!(sf.path, shard_dir_2.join(shard_file_name(&shard_hash)));
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
        let valid_url_range = HttpRange::from(byte_range);

        // Test 1: Valid URL and fetch_term should succeed
        let valid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: valid_url.clone(),
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data_v1(hash, valid_fetch_term);
        assert!(result.is_ok(), "Valid fetch_term should succeed");

        // Test 2: Invalid URL format - too few parts (3 instead of 4)
        let too_few_parts = "filename:123:456";
        let invalid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: too_few_parts.to_string(),
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data_v1(hash, invalid_fetch_term);
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
        let result = client.get_file_term_data_v1(hash, invalid_fetch_term);
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
        let result = client.get_file_term_data_v1(hash, invalid_fetch_term);
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
        let result = client.get_file_term_data_v1(hash, invalid_fetch_term);
        assert!(result.is_err(), "Non-numeric start_pos should fail");
        assert!(matches!(result.unwrap_err(), CasClientError::InvalidArguments));

        // Test 6: Invalid end_pos - non-numeric
        let non_numeric_end = format!("{:?}:{}:not_a_number:{}", file_path, fetch_byte_start, timestamp_ms);
        let invalid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: non_numeric_end,
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data_v1(hash, invalid_fetch_term);
        assert!(result.is_err(), "Non-numeric end_pos should fail");
        assert!(matches!(result.unwrap_err(), CasClientError::InvalidArguments));

        // Test 7: Empty URL
        let invalid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: String::new(),
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data_v1(hash, invalid_fetch_term);
        assert!(result.is_err(), "Empty URL should fail");
        assert!(matches!(result.unwrap_err(), CasClientError::InvalidArguments));

        // Test 8: Invalid timestamp - non-numeric
        let non_numeric_timestamp = format!("{:?}:{}:{}:not_a_number", file_path, fetch_byte_start, fetch_byte_end);
        let invalid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: non_numeric_timestamp,
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data_v1(hash, invalid_fetch_term);
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
        let result = client.get_file_term_data_v1(hash, invalid_fetch_term);
        assert!(result.is_err(), "Non-existent file should fail");
    }

    #[tokio::test(start_paused = true)]
    async fn test_url_expiration_within_window() {
        let xorb = build_raw_xorb(3, ChunkSize::Fixed(2048));
        let cas_object = build_and_verify_cas_object(xorb, None);
        let hash = cas_object.hash;

        let client = LocalClient::temporary().await.unwrap();
        client.set_fetch_term_url_expiration(Duration::from_secs(60));

        let permit = client.acquire_upload_permit().await.unwrap();
        client.upload_xorb("default", cas_object, None, permit).await.unwrap();

        let file_path = client.get_path_for_entry(&hash);
        let file = File::open(&file_path).unwrap();
        let mut reader = BufReader::new(file);
        let cas = CasObject::deserialize(&mut reader).unwrap();
        let (fetch_byte_start, fetch_byte_end) = cas.get_byte_offset(0, 1).unwrap();

        // Create URL at current time
        let timestamp = Instant::now();
        let byte_range = FileRange::new(fetch_byte_start as u64, fetch_byte_end as u64);
        let valid_url = generate_fetch_url(&file_path, &byte_range, timestamp);
        let valid_url_range = HttpRange::from(byte_range);

        // Advance time by 30 seconds (still within the 60 second window)
        tokio::time::advance(Duration::from_secs(30)).await;

        let fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: valid_url,
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data_v1(hash, fetch_term);
        assert!(result.is_ok(), "URL should be valid within expiration window");
    }

    #[tokio::test(start_paused = true)]
    async fn test_url_expiration_after_window() {
        let xorb = build_raw_xorb(3, ChunkSize::Fixed(2048));
        let cas_object = build_and_verify_cas_object(xorb, None);
        let hash = cas_object.hash;

        let client = LocalClient::temporary().await.unwrap();
        client.set_fetch_term_url_expiration(Duration::from_secs(60));

        let permit = client.acquire_upload_permit().await.unwrap();
        client.upload_xorb("default", cas_object, None, permit).await.unwrap();

        let file_path = client.get_path_for_entry(&hash);
        let file = File::open(&file_path).unwrap();
        let mut reader = BufReader::new(file);
        let cas = CasObject::deserialize(&mut reader).unwrap();
        let (fetch_byte_start, fetch_byte_end) = cas.get_byte_offset(0, 1).unwrap();

        // Create URL at current time
        let timestamp = Instant::now();
        let byte_range = FileRange::new(fetch_byte_start as u64, fetch_byte_end as u64);
        let expired_url = generate_fetch_url(&file_path, &byte_range, timestamp);
        let valid_url_range = HttpRange::from(byte_range);

        // Advance time by 61 seconds (past the 60 second window)
        tokio::time::advance(Duration::from_secs(61)).await;

        let fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: expired_url,
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data_v1(hash, fetch_term);
        assert!(result.is_err(), "URL should be expired after expiration window");
        assert!(matches!(result.unwrap_err(), CasClientError::PresignedUrlExpirationError));
    }

    #[tokio::test(start_paused = true)]
    async fn test_url_expiration_default_infinite() {
        let xorb = build_raw_xorb(3, ChunkSize::Fixed(2048));
        let cas_object = build_and_verify_cas_object(xorb, None);
        let hash = cas_object.hash;

        // Don't set expiration - default should be effectively infinite (u64::MAX ms)
        let client = LocalClient::temporary().await.unwrap();

        let permit = client.acquire_upload_permit().await.unwrap();
        client.upload_xorb("default", cas_object, None, permit).await.unwrap();

        let file_path = client.get_path_for_entry(&hash);
        let file = File::open(&file_path).unwrap();
        let mut reader = BufReader::new(file);
        let cas = CasObject::deserialize(&mut reader).unwrap();
        let (fetch_byte_start, fetch_byte_end) = cas.get_byte_offset(0, 1).unwrap();

        // Create URL at current time
        let timestamp = Instant::now();
        let byte_range = FileRange::new(fetch_byte_start as u64, fetch_byte_end as u64);
        let url = generate_fetch_url(&file_path, &byte_range, timestamp);
        let valid_url_range = HttpRange::from(byte_range);

        // Advance time by 1 year - should still work with default infinite expiration
        tokio::time::advance(Duration::from_secs(365 * 24 * 60 * 60)).await;

        let fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url,
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data_v1(hash, fetch_term);
        assert!(result.is_ok(), "URL should not expire with default infinite expiration");
    }

    #[tokio::test(start_paused = true)]
    async fn test_url_expiration_exact_boundary() {
        let xorb = build_raw_xorb(3, ChunkSize::Fixed(2048));
        let cas_object = build_and_verify_cas_object(xorb, None);
        let hash = cas_object.hash;

        let client = LocalClient::temporary().await.unwrap();
        client.set_fetch_term_url_expiration(Duration::from_secs(60));

        let permit = client.acquire_upload_permit().await.unwrap();
        client.upload_xorb("default", cas_object, None, permit).await.unwrap();

        let file_path = client.get_path_for_entry(&hash);
        let file = File::open(&file_path).unwrap();
        let mut reader = BufReader::new(file);
        let cas = CasObject::deserialize(&mut reader).unwrap();
        let (fetch_byte_start, fetch_byte_end) = cas.get_byte_offset(0, 1).unwrap();

        let byte_range = FileRange::new(fetch_byte_start as u64, fetch_byte_end as u64);
        let valid_url_range = HttpRange::from(byte_range);

        // Create URL at current time
        let timestamp = Instant::now();
        let url = generate_fetch_url(&file_path, &byte_range, timestamp);

        // Test inside boundary (59 seconds elapsed, within 60 second window) - should be valid
        tokio::time::advance(Duration::from_secs(59)).await;

        let fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: url.clone(),
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data_v1(hash, fetch_term);
        assert!(result.is_ok(), "URL should be valid inside expiration boundary");

        // Advance 2 more seconds (now 61 seconds total, past 60 second window) - should be expired
        tokio::time::advance(Duration::from_secs(2)).await;

        let fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url,
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data_v1(hash, fetch_term);
        assert!(result.is_err(), "URL should be expired past boundary");
        assert!(matches!(result.unwrap_err(), CasClientError::PresignedUrlExpirationError));
    }

    #[tokio::test]
    async fn test_get_reconstruction_merges_adjacent_ranges() {
        let client = LocalClient::temporary().await.unwrap();

        // Create segments: xorb 1 chunks 0-2, then chunks 2-4 (adjacent)
        let term_spec = &[(1, (0, 2)), (1, (2, 4))];
        let file = client.upload_random_file(term_spec, 2048).await.unwrap();

        // Verify reconstruction merges adjacent ranges
        let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
        assert_eq!(reconstruction.terms.len(), 2);
        assert_eq!(reconstruction.fetch_info.len(), 1);

        let xorb_hash_hex = reconstruction.terms[0].hash;
        let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
        assert_eq!(fetch_infos.len(), 1);
        assert_eq!(fetch_infos[0].range.start, 0);
        assert_eq!(fetch_infos[0].range.end, 4);

        // Verify file retrieval
        assert_eq!(client.get_file_data(&file.file_hash, None).await.unwrap(), file.data);
    }

    #[tokio::test]
    async fn test_get_reconstruction_with_multiple_xorbs() {
        let client = LocalClient::temporary().await.unwrap();

        // Create file with segments from different xorbs
        let term_spec = &[(1, (0, 3)), (2, (0, 2)), (1, (3, 5))];
        let file = client.upload_random_file(term_spec, 2048).await.unwrap();

        // Verify reconstruction
        let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
        assert_eq!(reconstruction.terms.len(), 3);
        assert_eq!(reconstruction.fetch_info.len(), 2);

        // Verify file retrieval
        assert_eq!(client.get_file_data(&file.file_hash, None).await.unwrap(), file.data);
    }

    /// Tests that overlapping chunk ranges within the same xorb are correctly merged
    /// into a single fetch_info with the union of the ranges.
    #[tokio::test]
    async fn test_get_reconstruction_overlapping_range_merging() {
        let client = LocalClient::temporary().await.unwrap();
        let chunk_size = 2048usize;

        // Test 1: Simple overlapping ranges [0,3) and [1,4) -> merged to [0,4)
        {
            let term_spec = &[(1, (0, 3)), (1, (1, 4))];
            let file = client.upload_random_file(term_spec, chunk_size).await.unwrap();

            let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 2);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            let xorb_hash_hex = reconstruction.terms[0].hash;
            let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
            assert_eq!(fetch_infos.len(), 1);
            assert_eq!(fetch_infos[0].range.start, 0);
            assert_eq!(fetch_infos[0].range.end, 4);

            assert_eq!(client.get_file_data(&file.file_hash, None).await.unwrap(), file.data);
        }

        // Test 2: Subset range - second range is fully contained in first [0,5) and [1,3) -> [0,5)
        {
            let term_spec = &[(1, (0, 5)), (1, (1, 3))];
            let file = client.upload_random_file(term_spec, chunk_size).await.unwrap();

            let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 2);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            let xorb_hash_hex = reconstruction.terms[0].hash;
            let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
            assert_eq!(fetch_infos.len(), 1);
            assert_eq!(fetch_infos[0].range.start, 0);
            assert_eq!(fetch_infos[0].range.end, 5);

            assert_eq!(client.get_file_data(&file.file_hash, None).await.unwrap(), file.data);
        }

        // Test 6: Non-contiguous ranges should NOT be merged [0,2) and [4,6) -> two separate ranges
        {
            let term_spec = &[(1, (0, 2)), (1, (4, 6))];
            let file = client.upload_random_file(term_spec, chunk_size).await.unwrap();

            let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 2);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            let xorb_hash_hex = reconstruction.terms[0].hash;
            let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
            assert_eq!(fetch_infos.len(), 2);
            assert_eq!(fetch_infos[0].range.start, 0);
            assert_eq!(fetch_infos[0].range.end, 2);
            assert_eq!(fetch_infos[1].range.start, 4);
            assert_eq!(fetch_infos[1].range.end, 6);

            assert_eq!(client.get_file_data(&file.file_hash, None).await.unwrap(), file.data);
        }

        // Test 7: Touch at boundary (adjacent) [0,3) and [3,5) -> [0,5)
        {
            let term_spec = &[(1, (0, 3)), (1, (3, 5))];
            let file = client.upload_random_file(term_spec, chunk_size).await.unwrap();

            let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 2);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            let xorb_hash_hex = reconstruction.terms[0].hash;
            let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
            assert_eq!(fetch_infos.len(), 1);
            assert_eq!(fetch_infos[0].range.start, 0);
            assert_eq!(fetch_infos[0].range.end, 5);

            assert_eq!(client.get_file_data(&file.file_hash, None).await.unwrap(), file.data);
        }
    }

    #[tokio::test]
    async fn test_range_requests() {
        let client = LocalClient::temporary().await.unwrap();
        let term_spec = &[(1, (0, 5))];
        let file = client.upload_random_file(term_spec, 2048).await.unwrap();
        let total_file_size = file.data.len() as u64;

        // Test get_reconstruction range behaviors
        {
            // Partial out-of-range truncates
            let response = client
                .get_reconstruction(&file.file_hash, Some(FileRange::new(total_file_size / 2, total_file_size + 1000)))
                .await
                .unwrap()
                .unwrap();
            assert!(!response.terms.is_empty());
            assert!(response.offset_into_first_range > 0);

            // Entire range out of bounds returns error
            // Range completely beyond file size returns Ok(None) (like RemoteClient's 416 handling)
            let result = client
                .get_reconstruction(
                    &file.file_hash,
                    Some(FileRange::new(total_file_size + 100, total_file_size + 1000)),
                )
                .await;
            assert!(result.unwrap().is_none());

            // Start equals file size returns Ok(None)
            let result = client
                .get_reconstruction(&file.file_hash, Some(FileRange::new(total_file_size, total_file_size + 100)))
                .await;
            assert!(result.unwrap().is_none());

            // Valid range within bounds succeeds
            let response = client
                .get_reconstruction(&file.file_hash, Some(FileRange::new(0, total_file_size / 2)))
                .await
                .unwrap()
                .unwrap();
            assert!(!response.terms.is_empty());
            assert_eq!(response.offset_into_first_range, 0);

            // End exactly at file size succeeds
            let response = client
                .get_reconstruction(&file.file_hash, Some(FileRange::new(0, total_file_size)))
                .await
                .unwrap()
                .unwrap();
            let total_unpacked: u64 = response.terms.iter().map(|t| t.unpacked_length as u64).sum();
            assert_eq!(total_unpacked, total_file_size);
        }

        // Test get_file_data range behaviors
        {
            // Partial out-of-range truncates
            let partial_start = total_file_size / 2;
            let data = client
                .get_file_data(&file.file_hash, Some(FileRange::new(partial_start, total_file_size + 1000)))
                .await
                .unwrap();
            assert_eq!(data, &file.data[partial_start as usize..]);

            // Entire range out of bounds returns error
            let result = client
                .get_file_data(&file.file_hash, Some(FileRange::new(total_file_size + 100, total_file_size + 1000)))
                .await;
            assert!(matches!(result.unwrap_err(), CasClientError::InvalidRange));

            // Start equals file size returns error
            let result = client
                .get_file_data(&file.file_hash, Some(FileRange::new(total_file_size, total_file_size + 100)))
                .await;
            assert!(matches!(result.unwrap_err(), CasClientError::InvalidRange));

            // Valid range within bounds
            let valid_end = total_file_size / 2;
            let data = client
                .get_file_data(&file.file_hash, Some(FileRange::new(0, valid_end)))
                .await
                .unwrap();
            assert_eq!(data, &file.data[..valid_end as usize]);

            // End exactly at file size
            let data = client
                .get_file_data(&file.file_hash, Some(FileRange::new(0, total_file_size)))
                .await
                .unwrap();
            assert_eq!(data, file.data);
        }
    }

    #[tokio::test]
    async fn test_upload_random_file_configurations() {
        let client = LocalClient::temporary().await.unwrap();

        // Test 1: Single segment with 3 chunks
        {
            let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
            assert_eq!(client.get_file_data(&file.file_hash, None).await.unwrap(), file.data);
        }

        // Test 2: Multiple segments from the same xorb
        {
            let term_spec = &[(1, (0, 2)), (1, (2, 4)), (1, (4, 6))];
            let file = client.upload_random_file(term_spec, 2048).await.unwrap();

            let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 3);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            assert_eq!(client.get_file_data(&file.file_hash, None).await.unwrap(), file.data);
        }

        // Test 3: Segments from different xorbs
        {
            let term_spec = &[(1, (0, 3)), (2, (0, 2)), (3, (0, 4))];
            let file = client.upload_random_file(term_spec, 2048).await.unwrap();

            let reconstruction = client.get_reconstruction(&file.file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 3);
            assert_eq!(reconstruction.fetch_info.len(), 3);

            assert_eq!(client.get_file_data(&file.file_hash, None).await.unwrap(), file.data);
        }

        // Test 4: Partial range retrieval
        {
            let term_spec = &[(1, (0, 5)), (2, (0, 5))];
            let file = client.upload_random_file(term_spec, 2048).await.unwrap();
            let half = file.data.len() as u64 / 2;

            // First half
            let first_half = client
                .get_file_data(&file.file_hash, Some(FileRange::new(0, half)))
                .await
                .unwrap();
            assert_eq!(first_half, &file.data[..half as usize]);

            // Second half
            let second_half = client
                .get_file_data(&file.file_hash, Some(FileRange::new(half, file.data.len() as u64)))
                .await
                .unwrap();
            assert_eq!(second_half, &file.data[half as usize..]);
        }
    }

    /// Tests that get_reconstruction correctly shrinks chunk ranges to only include
    /// chunks that contain at least part of the requested byte range.
    #[tokio::test]
    async fn test_get_reconstruction_chunk_boundary_shrinking() {
        let client = LocalClient::temporary().await.unwrap();

        // Create a file with 5 chunks of 2048 bytes each = 10240 total bytes
        let chunk_size: usize = 2048;
        let term_spec = &[(1, (0, 5))];
        let file = client.upload_random_file(term_spec, chunk_size).await.unwrap();

        let total_file_size = file.data.len() as u64;
        assert_eq!(total_file_size, (5 * chunk_size) as u64);

        let query_file_size = client.get_file_size(&file.file_hash).await.unwrap();
        assert_eq!(query_file_size, total_file_size);

        // Test 1: Range starting in the middle of chunk 1 should skip chunk 0
        {
            let start = chunk_size as u64 + 500;
            let end = total_file_size;
            let response = client
                .get_reconstruction(&file.file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 1);
            assert_eq!(response.terms[0].range.start, 1);
            assert_eq!(response.terms[0].range.end, 5);
            assert_eq!(response.offset_into_first_range, 500);
        }

        // Test 2: Range starting exactly at a chunk boundary
        {
            let start = (chunk_size * 2) as u64;
            let end = total_file_size;
            let response = client
                .get_reconstruction(&file.file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 1);
            assert_eq!(response.terms[0].range.start, 2);
            assert_eq!(response.terms[0].range.end, 5);
            assert_eq!(response.offset_into_first_range, 0);
        }

        // Test 3: Range ending in the middle of a chunk
        {
            let start = 0u64;
            let end = (chunk_size * 2) as u64 + 500;
            let response = client
                .get_reconstruction(&file.file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 1);
            assert_eq!(response.terms[0].range.start, 0);
            assert_eq!(response.terms[0].range.end, 3);
            assert_eq!(response.offset_into_first_range, 0);
        }
    }
}
