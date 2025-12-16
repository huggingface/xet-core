use std::collections::HashMap;
use std::fs::{File, metadata};
use std::io::{BufReader, Cursor, Seek, SeekFrom, Write};
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
use progress_tracking::item_tracking::SingleItemProgressUpdater;
use progress_tracking::upload_tracking::CompletionTracker;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Handle;
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info, warn};
use utils::serialization_utils::read_u32;

use crate::adaptive_concurrency::AdaptiveConcurrencyController;
use crate::download_utils::TermDownloadOutput;
use crate::error::{CasClientError, Result};
use crate::{Client, SeekingOutputProvider, SequentialOutput};

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
    _tmp_dir: Option<TempDir>, // To hold directory to use for local testing
    xorb_dir: PathBuf,
    shard_dir: PathBuf,
    shard_manager: Arc<ShardFileManager>,
    global_dedup_db_env: heed::Env,
    global_dedup_table: heed::Database<OwnedType<MerkleHash>, OwnedType<MerkleHash>>,
    upload_concurrency_controller: Arc<AdaptiveConcurrencyController>,
    url_expiration_ms: AtomicU64,
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

        // Open / set up the global dedup lookup
        let global_dedup_db_env = heed::EnvOpenOptions::new()
            .max_dbs(32)
            .max_readers(32)
            .open(&global_dedup_dir)
            .map_err(|e| CasClientError::Other(format!("Error opening db at {global_dedup_dir:?}: {e}")))?;

        let global_dedup_table = global_dedup_db_env
            .create_database(None)
            .map_err(|e| CasClientError::Other(format!("Error opening heed table: {e}")))?;

        // Open / set up the shard lookup
        let shard_manager = ShardFileManager::new_in_session_directory(shard_dir.clone(), true).await?;

        Ok(Self {
            _tmp_dir: tmp_dir,
            shard_dir,
            xorb_dir,
            shard_manager,
            global_dedup_db_env,
            global_dedup_table,
            upload_concurrency_controller: AdaptiveConcurrencyController::new_upload("local_uploads"),
            url_expiration_ms: AtomicU64::new(u64::MAX),
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
        let read_txn = self.global_dedup_db_env.read_txn().map_err(map_heed_db_error)?;

        if let Some(shard) = self.global_dedup_table.get(&read_txn, chunk_hash).map_err(map_heed_db_error)? {
            let filename = self.shard_dir.join(shard_file_name(&shard));
            return Ok(Some(std::fs::read(filename)?.into()));
        }
        Ok(None)
    }

    async fn acquire_upload_permit(&self) -> Result<crate::adaptive_concurrency::ConnectionPermit> {
        self.upload_concurrency_controller.acquire_connection_permit().await
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

        let mut write_txn = self.global_dedup_db_env.write_txn().map_err(map_heed_db_error)?;

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
            // If the entire range is out of bounds, return InvalidRange error
            if range.start >= total_file_size {
                return Err(CasClientError::InvalidRange);
            }
            // Truncate end if it extends beyond file size
            FileRange::new(range.start, range.end.min(total_file_size))
        } else {
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
        if url_byte_range.start != fetch_term.url_range.start || url_byte_range.end != fetch_term.url_range.end {
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

    async fn get_file_with_sequential_writer(
        self: Arc<Self>,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        mut output_provider: SequentialOutput,
        _progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64> {
        let data = self.get_file_data(hash, byte_range).await?;
        let len = data.len() as u64;
        output_provider.write_all(&data).await?;
        Ok(len)
    }

    async fn get_file_with_parallel_writer(
        self: Arc<Self>,
        hash: &MerkleHash,
        byte_range: Option<FileRange>,
        output_provider: SeekingOutputProvider,
        progress_updater: Option<Arc<SingleItemProgressUpdater>>,
    ) -> Result<u64> {
        let sequential = output_provider.try_into()?;
        self.get_file_with_sequential_writer(hash, byte_range, sequential, progress_updater)
            .await
    }
}

impl LocalClient {
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
        let valid_url_range = HttpRange::new(fetch_byte_start as u64, fetch_byte_end as u64);

        // Test 1: Valid URL and fetch_term should succeed
        let valid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: valid_url.clone(),
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data(hash, valid_fetch_term).await;
        assert!(result.is_ok(), "Valid fetch_term should succeed");

        // Test 2: Invalid URL format - too few parts (3 instead of 4)
        let too_few_parts = "filename:123:456";
        let invalid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: too_few_parts.to_string(),
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data(hash, invalid_fetch_term).await;
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
        let result = client.get_file_term_data(hash, invalid_fetch_term).await;
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
        let result = client.get_file_term_data(hash, invalid_fetch_term).await;
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
        let result = client.get_file_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "Non-numeric start_pos should fail");
        assert!(matches!(result.unwrap_err(), CasClientError::InvalidArguments));

        // Test 6: Invalid end_pos - non-numeric
        let non_numeric_end = format!("{:?}:{}:not_a_number:{}", file_path, fetch_byte_start, timestamp_ms);
        let invalid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: non_numeric_end,
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "Non-numeric end_pos should fail");
        assert!(matches!(result.unwrap_err(), CasClientError::InvalidArguments));

        // Test 7: Empty URL
        let invalid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: String::new(),
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "Empty URL should fail");
        assert!(matches!(result.unwrap_err(), CasClientError::InvalidArguments));

        // Test 8: Invalid timestamp - non-numeric
        let non_numeric_timestamp = format!("{:?}:{}:{}:not_a_number", file_path, fetch_byte_start, fetch_byte_end);
        let invalid_fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: non_numeric_timestamp,
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data(hash, invalid_fetch_term).await;
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
        let result = client.get_file_term_data(hash, invalid_fetch_term).await;
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
        let valid_url_range = HttpRange::new(fetch_byte_start as u64, fetch_byte_end as u64);

        // Advance time by 30 seconds (still within the 60 second window)
        tokio::time::advance(Duration::from_secs(30)).await;

        let fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: valid_url,
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data(hash, fetch_term).await;
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
        let valid_url_range = HttpRange::new(fetch_byte_start as u64, fetch_byte_end as u64);

        // Advance time by 61 seconds (past the 60 second window)
        tokio::time::advance(Duration::from_secs(61)).await;

        let fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: expired_url,
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data(hash, fetch_term).await;
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
        let valid_url_range = HttpRange::new(fetch_byte_start as u64, fetch_byte_end as u64);

        // Advance time by 1 year - should still work with default infinite expiration
        tokio::time::advance(Duration::from_secs(365 * 24 * 60 * 60)).await;

        let fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url,
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data(hash, fetch_term).await;
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
        let valid_url_range = HttpRange::new(fetch_byte_start as u64, fetch_byte_end as u64);

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
        let result = client.get_file_term_data(hash, fetch_term).await;
        assert!(result.is_ok(), "URL should be valid inside expiration boundary");

        // Advance 2 more seconds (now 61 seconds total, past 60 second window) - should be expired
        tokio::time::advance(Duration::from_secs(2)).await;

        let fetch_term = CASReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url,
            url_range: valid_url_range,
        };
        let result = client.get_file_term_data(hash, fetch_term).await;
        assert!(result.is_err(), "URL should be expired past boundary");
        assert!(matches!(result.unwrap_err(), CasClientError::PresignedUrlExpirationError));
    }

    #[tokio::test]
    async fn test_get_reconstruction_merges_adjacent_ranges() {
        let client = LocalClient::temporary().await.unwrap();

        // Create segments: xorb 1 chunks 0-2, then chunks 2-4 (adjacent)
        let term_spec = &[(1, (0, 2)), (1, (2, 4))];
        let (file_data, file_hash) = client.upload_random_file(term_spec, 2048).await.unwrap();

        // Verify reconstruction merges adjacent ranges
        let reconstruction = client.get_reconstruction(&file_hash, None).await.unwrap().unwrap();
        assert_eq!(reconstruction.terms.len(), 2);
        assert_eq!(reconstruction.fetch_info.len(), 1);

        let xorb_hash_hex = reconstruction.terms[0].hash;
        let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
        assert_eq!(fetch_infos.len(), 1);
        assert_eq!(fetch_infos[0].range.start, 0);
        assert_eq!(fetch_infos[0].range.end, 4);

        // Verify file retrieval
        assert_eq!(client.get_file_data(&file_hash, None).await.unwrap(), file_data);
    }

    #[tokio::test]
    async fn test_get_reconstruction_with_multiple_xorbs() {
        let client = LocalClient::temporary().await.unwrap();

        // Create file with segments from different xorbs
        let term_spec = &[(1, (0, 3)), (2, (0, 2)), (1, (3, 5))];
        let (file_data, file_hash) = client.upload_random_file(term_spec, 2048).await.unwrap();

        // Verify reconstruction
        let reconstruction = client.get_reconstruction(&file_hash, None).await.unwrap().unwrap();
        assert_eq!(reconstruction.terms.len(), 3);
        assert_eq!(reconstruction.fetch_info.len(), 2);

        // Verify file retrieval
        assert_eq!(client.get_file_data(&file_hash, None).await.unwrap(), file_data);
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
            let (file_data, file_hash) = client.upload_random_file(term_spec, chunk_size).await.unwrap();

            let reconstruction = client.get_reconstruction(&file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 2);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            let xorb_hash_hex = reconstruction.terms[0].hash;
            let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
            assert_eq!(fetch_infos.len(), 1);
            assert_eq!(fetch_infos[0].range.start, 0);
            assert_eq!(fetch_infos[0].range.end, 4);

            assert_eq!(client.get_file_data(&file_hash, None).await.unwrap(), file_data);
        }

        // Test 2: Subset range - second range is fully contained in first [0,5) and [1,3) -> [0,5)
        {
            let term_spec = &[(1, (0, 5)), (1, (1, 3))];
            let (file_data, file_hash) = client.upload_random_file(term_spec, chunk_size).await.unwrap();

            let reconstruction = client.get_reconstruction(&file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 2);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            let xorb_hash_hex = reconstruction.terms[0].hash;
            let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
            assert_eq!(fetch_infos.len(), 1);
            assert_eq!(fetch_infos[0].range.start, 0);
            assert_eq!(fetch_infos[0].range.end, 5);

            assert_eq!(client.get_file_data(&file_hash, None).await.unwrap(), file_data);
        }

        // Test 3: Second range ends before first range end [0,5) and [2,4) -> [0,5)
        {
            let term_spec = &[(1, (0, 5)), (1, (2, 4))];
            let (file_data, file_hash) = client.upload_random_file(term_spec, chunk_size).await.unwrap();

            let reconstruction = client.get_reconstruction(&file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 2);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            let xorb_hash_hex = reconstruction.terms[0].hash;
            let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
            assert_eq!(fetch_infos.len(), 1);
            assert_eq!(fetch_infos[0].range.start, 0);
            assert_eq!(fetch_infos[0].range.end, 5);

            assert_eq!(client.get_file_data(&file_hash, None).await.unwrap(), file_data);
        }

        // Test 4: Multiple overlapping ranges forming a chain [0,2), [1,4), [3,6) -> [0,6)
        {
            let term_spec = &[(1, (0, 2)), (1, (1, 4)), (1, (3, 6))];
            let (file_data, file_hash) = client.upload_random_file(term_spec, chunk_size).await.unwrap();

            let reconstruction = client.get_reconstruction(&file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 3);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            let xorb_hash_hex = reconstruction.terms[0].hash;
            let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
            assert_eq!(fetch_infos.len(), 1);
            assert_eq!(fetch_infos[0].range.start, 0);
            assert_eq!(fetch_infos[0].range.end, 6);

            assert_eq!(client.get_file_data(&file_hash, None).await.unwrap(), file_data);
        }

        // Test 5: Ranges that interleave in a non-monotonic way [0,5), [1,3), [2,4) -> [0,5)
        {
            let term_spec = &[(1, (0, 5)), (1, (1, 3)), (1, (2, 4))];
            let (file_data, file_hash) = client.upload_random_file(term_spec, chunk_size).await.unwrap();

            let reconstruction = client.get_reconstruction(&file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 3);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            let xorb_hash_hex = reconstruction.terms[0].hash;
            let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
            assert_eq!(fetch_infos.len(), 1);
            assert_eq!(fetch_infos[0].range.start, 0);
            assert_eq!(fetch_infos[0].range.end, 5);

            assert_eq!(client.get_file_data(&file_hash, None).await.unwrap(), file_data);
        }

        // Test 6: Non-contiguous ranges should NOT be merged [0,2) and [4,6) -> two separate ranges
        {
            let term_spec = &[(1, (0, 2)), (1, (4, 6))];
            let (file_data, file_hash) = client.upload_random_file(term_spec, chunk_size).await.unwrap();

            let reconstruction = client.get_reconstruction(&file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 2);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            let xorb_hash_hex = reconstruction.terms[0].hash;
            let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
            assert_eq!(fetch_infos.len(), 2);
            assert_eq!(fetch_infos[0].range.start, 0);
            assert_eq!(fetch_infos[0].range.end, 2);
            assert_eq!(fetch_infos[1].range.start, 4);
            assert_eq!(fetch_infos[1].range.end, 6);

            assert_eq!(client.get_file_data(&file_hash, None).await.unwrap(), file_data);
        }

        // Test 7: Touch at boundary (adjacent) [0,3) and [3,5) -> [0,5)
        {
            let term_spec = &[(1, (0, 3)), (1, (3, 5))];
            let (file_data, file_hash) = client.upload_random_file(term_spec, chunk_size).await.unwrap();

            let reconstruction = client.get_reconstruction(&file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 2);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            let xorb_hash_hex = reconstruction.terms[0].hash;
            let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
            assert_eq!(fetch_infos.len(), 1);
            assert_eq!(fetch_infos[0].range.start, 0);
            assert_eq!(fetch_infos[0].range.end, 5);

            assert_eq!(client.get_file_data(&file_hash, None).await.unwrap(), file_data);
        }

        // Test 8: Large range followed by small contained range [0,10) and [4,6) -> [0,10)
        {
            let term_spec = &[(1, (0, 10)), (1, (4, 6))];
            let (file_data, file_hash) = client.upload_random_file(term_spec, chunk_size).await.unwrap();

            let reconstruction = client.get_reconstruction(&file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 2);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            let xorb_hash_hex = reconstruction.terms[0].hash;
            let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
            assert_eq!(fetch_infos.len(), 1);
            assert_eq!(fetch_infos[0].range.start, 0);
            assert_eq!(fetch_infos[0].range.end, 10);

            assert_eq!(client.get_file_data(&file_hash, None).await.unwrap(), file_data);
        }

        // Test 9: Same range repeated multiple times [2,5), [2,5), [2,5) -> [2,5)
        {
            let term_spec = &[(1, (2, 5)), (1, (2, 5)), (1, (2, 5))];
            let (file_data, file_hash) = client.upload_random_file(term_spec, chunk_size).await.unwrap();

            let reconstruction = client.get_reconstruction(&file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 3);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            let xorb_hash_hex = reconstruction.terms[0].hash;
            let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
            assert_eq!(fetch_infos.len(), 1);
            assert_eq!(fetch_infos[0].range.start, 2);
            assert_eq!(fetch_infos[0].range.end, 5);

            assert_eq!(client.get_file_data(&file_hash, None).await.unwrap(), file_data);
        }

        // Test 10: Mixed overlapping and non-contiguous in complex pattern
        // [0,3), [2,4), [6,8), [7,10) -> [0,4) and [6,10)
        {
            let term_spec = &[(1, (0, 3)), (1, (2, 4)), (1, (6, 8)), (1, (7, 10))];
            let (file_data, file_hash) = client.upload_random_file(term_spec, chunk_size).await.unwrap();

            let reconstruction = client.get_reconstruction(&file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 4);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            let xorb_hash_hex = reconstruction.terms[0].hash;
            let fetch_infos = reconstruction.fetch_info.get(&xorb_hash_hex).unwrap();
            assert_eq!(fetch_infos.len(), 2);
            assert_eq!(fetch_infos[0].range.start, 0);
            assert_eq!(fetch_infos[0].range.end, 4);
            assert_eq!(fetch_infos[1].range.start, 6);
            assert_eq!(fetch_infos[1].range.end, 10);

            assert_eq!(client.get_file_data(&file_hash, None).await.unwrap(), file_data);
        }
    }

    #[tokio::test]
    async fn test_range_requests() {
        let client = LocalClient::temporary().await.unwrap();
        let term_spec = &[(1, (0, 5))];
        let (file_data, file_hash) = client.upload_random_file(term_spec, 2048).await.unwrap();
        let total_file_size = file_data.len() as u64;

        // Test get_reconstruction range behaviors
        {
            // Partial out-of-range truncates
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(total_file_size / 2, total_file_size + 1000)))
                .await
                .unwrap()
                .unwrap();
            assert!(!response.terms.is_empty());
            assert!(response.offset_into_first_range > 0);

            // Entire range out of bounds returns error
            let result = client
                .get_reconstruction(&file_hash, Some(FileRange::new(total_file_size + 100, total_file_size + 1000)))
                .await;
            assert!(matches!(result.unwrap_err(), CasClientError::InvalidRange));

            // Start equals file size returns error
            let result = client
                .get_reconstruction(&file_hash, Some(FileRange::new(total_file_size, total_file_size + 100)))
                .await;
            assert!(matches!(result.unwrap_err(), CasClientError::InvalidRange));

            // Valid range within bounds succeeds
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(0, total_file_size / 2)))
                .await
                .unwrap()
                .unwrap();
            assert!(!response.terms.is_empty());
            assert_eq!(response.offset_into_first_range, 0);

            // End exactly at file size succeeds
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(0, total_file_size)))
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
                .get_file_data(&file_hash, Some(FileRange::new(partial_start, total_file_size + 1000)))
                .await
                .unwrap();
            assert_eq!(data, &file_data[partial_start as usize..]);

            // Entire range out of bounds returns error
            let result = client
                .get_file_data(&file_hash, Some(FileRange::new(total_file_size + 100, total_file_size + 1000)))
                .await;
            assert!(matches!(result.unwrap_err(), CasClientError::InvalidRange));

            // Start equals file size returns error
            let result = client
                .get_file_data(&file_hash, Some(FileRange::new(total_file_size, total_file_size + 100)))
                .await;
            assert!(matches!(result.unwrap_err(), CasClientError::InvalidRange));

            // Valid range within bounds
            let valid_end = total_file_size / 2;
            let data = client
                .get_file_data(&file_hash, Some(FileRange::new(0, valid_end)))
                .await
                .unwrap();
            assert_eq!(data, &file_data[..valid_end as usize]);

            // End exactly at file size
            let data = client
                .get_file_data(&file_hash, Some(FileRange::new(0, total_file_size)))
                .await
                .unwrap();
            assert_eq!(data, file_data);
        }
    }

    #[tokio::test]
    async fn test_get_file_with_sequential_writer() {
        use crate::output_provider::buffer_provider::ThreadSafeBuffer;

        let client = LocalClient::temporary().await.unwrap();
        let term_spec = &[(1, (0, 5))];
        let (file_data, file_hash) = client.upload_random_file(term_spec, 2048).await.unwrap();

        // Test that sequential writer correctly wraps get_file_data
        let buffer = ThreadSafeBuffer::default();
        let bytes_written = client
            .clone()
            .get_file_with_sequential_writer(&file_hash, None, buffer.clone().into(), None)
            .await
            .unwrap();

        assert_eq!(bytes_written as usize, file_data.len());
        assert_eq!(buffer.value(), file_data);

        // Test with range
        let buffer2 = ThreadSafeBuffer::default();
        let half = file_data.len() as u64 / 2;
        let bytes_written2 = client
            .clone()
            .get_file_with_sequential_writer(&file_hash, Some(FileRange::new(0, half)), buffer2.clone().into(), None)
            .await
            .unwrap();

        assert_eq!(bytes_written2, half);
        assert_eq!(buffer2.value(), &file_data[..half as usize]);
    }

    #[tokio::test]
    async fn test_upload_random_file_configurations() {
        let client = LocalClient::temporary().await.unwrap();

        // Test 1: Single segment with 3 chunks
        {
            let (file_data, file_hash) = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
            assert_eq!(client.get_file_data(&file_hash, None).await.unwrap(), file_data);
        }

        // Test 2: Multiple segments from the same xorb
        {
            let term_spec = &[(1, (0, 2)), (1, (2, 4)), (1, (4, 6))];
            let (file_data, file_hash) = client.upload_random_file(term_spec, 2048).await.unwrap();

            let reconstruction = client.get_reconstruction(&file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 3);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            assert_eq!(client.get_file_data(&file_hash, None).await.unwrap(), file_data);
        }

        // Test 3: Segments from different xorbs
        {
            let term_spec = &[(1, (0, 3)), (2, (0, 2)), (3, (0, 4))];
            let (file_data, file_hash) = client.upload_random_file(term_spec, 2048).await.unwrap();

            let reconstruction = client.get_reconstruction(&file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 3);
            assert_eq!(reconstruction.fetch_info.len(), 3);

            assert_eq!(client.get_file_data(&file_hash, None).await.unwrap(), file_data);
        }

        // Test 4: Partial range retrieval
        {
            let term_spec = &[(1, (0, 5)), (2, (0, 5))];
            let (file_data, file_hash) = client.upload_random_file(term_spec, 2048).await.unwrap();
            let half = file_data.len() as u64 / 2;

            // First half
            let first_half = client.get_file_data(&file_hash, Some(FileRange::new(0, half))).await.unwrap();
            assert_eq!(first_half, &file_data[..half as usize]);

            // Second half
            let second_half = client
                .get_file_data(&file_hash, Some(FileRange::new(half, file_data.len() as u64)))
                .await
                .unwrap();
            assert_eq!(second_half, &file_data[half as usize..]);
        }

        // Test 5: Overlapping chunk references from same xorb
        {
            let term_spec = &[(1, (0, 3)), (1, (1, 4)), (1, (2, 5))];
            let (file_data, file_hash) = client.upload_random_file(term_spec, 2048).await.unwrap();

            let reconstruction = client.get_reconstruction(&file_hash, None).await.unwrap().unwrap();
            assert_eq!(reconstruction.terms.len(), 3);
            assert_eq!(reconstruction.fetch_info.len(), 1);

            assert_eq!(client.get_file_data(&file_hash, None).await.unwrap(), file_data);
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
        let (file_data, file_hash) = client.upload_random_file(term_spec, chunk_size).await.unwrap();

        let total_file_size = file_data.len() as u64;
        assert_eq!(total_file_size, (5 * chunk_size) as u64);

        let query_file_size = client.get_file_size(&file_hash).await.unwrap();
        assert_eq!(query_file_size, total_file_size);

        // Test 1: Range starting in the middle of chunk 1 should skip chunk 0
        // Range [2048 + 500, end of file] should skip chunk 0
        {
            let start = chunk_size as u64 + 500; // Middle of chunk 1
            let end = total_file_size;
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 1);
            // chunk 0 should be excluded; starts at chunk 1
            assert_eq!(response.terms[0].range.start, 1);
            // Should include all remaining chunks
            assert_eq!(response.terms[0].range.end, 5);
            // offset_into_first_range is now the offset into the first returned chunk
            // After skipping chunk 0 (2048 bytes), offset is 500
            assert_eq!(response.offset_into_first_range, 500);
        }

        // Test 2: Range starting exactly at a chunk boundary
        // Range [2048*2, end] should start at chunk 2
        {
            let start = (chunk_size * 2) as u64; // Start of chunk 2
            let end = total_file_size;
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 1);
            assert_eq!(response.terms[0].range.start, 2);
            assert_eq!(response.terms[0].range.end, 5);
            assert_eq!(response.offset_into_first_range, 0);
        }

        // Test 3: Range ending in the middle of a chunk should include that chunk and potentially more
        // Range [0, 2048*2 + 500] should include chunks that cover this range
        {
            let start = 0u64;
            let end = (chunk_size * 2) as u64 + 500; // Middle of chunk 2
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 1);
            assert_eq!(response.terms[0].range.start, 0);
            // Should include chunks up through the one containing the end byte
            // The end-shrinking will shrink things to be three chunks -- the two full ones at indices 0 and 1, and then
            // chunk 2 which contains the partial info.
            assert_eq!(response.terms[0].range.end, 3);
            assert_eq!(response.offset_into_first_range, 0);
        }

        // Test 4: Range fully within a single chunk
        // Range [2048*2 + 100, 2048*2 + 500] (inside chunk 2)
        {
            let start = (chunk_size * 2) as u64 + 100; // Inside chunk 2
            let end = (chunk_size * 2) as u64 + 500; // Still inside chunk 2
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 1);
            // Should start at chunk 2 (chunks 0-1 pruned)
            assert_eq!(response.terms[0].range.start, 2);
            // End-shrinking will shrink this range.
            assert_eq!(response.terms[0].range.end, 3);
            // After skipping chunks 0-1, offset is 100 into chunk 2
            assert_eq!(response.offset_into_first_range, 100);
        }

        // Test 5: Range spanning exactly one chunk boundary
        // Range [2048 - 100, 2048 + 100] spans end of chunk 0 and start of chunk 1
        {
            let start = chunk_size as u64 - 100; // Near end of chunk 0
            let end = chunk_size as u64 + 100; // Near start of chunk 1
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 1);
            // Should include both chunks 0 and 1 (and possibly more due to end-shrinking)
            assert_eq!(response.terms[0].range.start, 0);
            assert_eq!(response.terms[0].range.end, 2);
            // No chunks skipped at start, offset is chunk_size - 100 into chunk 0
            assert_eq!(response.offset_into_first_range, chunk_size as u64 - 100);
        }

        // Test 6: Range starting exactly at chunk boundary, ending at chunk boundary
        // Range [2048*2, 2048*4] (chunks 2-3); test off-by-one errors.
        for delta in [0, 1] {
            let start = (chunk_size * 2) as u64 + delta; // Start of chunk 2
            let end = (chunk_size * 4) as u64 - delta; // End of chunk 3
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 1);
            assert_eq!(response.terms[0].range.start, 2);
            // End-shrinking may keep additional chunks
            assert_eq!(response.terms[0].range.end, 4);
            assert_eq!(response.offset_into_first_range, delta);
        }

        // Test 7: Range starting at chunk boundary minus 1
        {
            let start = (chunk_size * 2) as u64 - 1; // Start of chunk 2
            let end = (chunk_size * 4) as u64 + 1; // One byte of chunk 4 
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 1);
            assert_eq!(response.terms[0].range.start, 1);
            // End-shrinking may keep additional chunks
            assert_eq!(response.terms[0].range.end, 5);
            assert_eq!(response.offset_into_first_range, chunk_size as u64 - 1);
        }
    }

    /// Tests chunk boundary shrinking with multiple segments across different xorbs.
    #[tokio::test]
    async fn test_get_reconstruction_chunk_boundary_multiple_segments() {
        let client = LocalClient::temporary().await.unwrap();

        // Create a file with segments from 2 xorbs:
        // xorb 1: chunks 0-4 (4 chunks * 2048 = 8192 bytes)
        // xorb 2: chunks 0-4 (4 chunks * 2048 = 8192 bytes)
        // Total: 16384 bytes
        let chunk_size = 2048usize;
        let term_spec = &[(1, (0, 4)), (2, (0, 4))];
        let (file_data, file_hash) = client.upload_random_file(term_spec, chunk_size).await.unwrap();

        let total_file_size = file_data.len() as u64;
        assert_eq!(total_file_size, (8 * chunk_size) as u64);

        // Test 1: Range that skips first chunk of first xorb
        {
            let start = chunk_size as u64 + 500; // Middle of chunk 1 in xorb 1
            let end = total_file_size;
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 2);

            // First term (xorb 1): should skip chunk 0
            assert_eq!(response.terms[0].range.start, 1);
            assert_eq!(response.terms[0].range.end, 4);

            // Second term (xorb 2): should include all chunks
            assert_eq!(response.terms[1].range.start, 0);
            assert_eq!(response.terms[1].range.end, 4);

            // After skipping chunk 0 (2048 bytes), offset is 500 into chunk 1
            assert_eq!(response.offset_into_first_range, 500);
        }

        // Test 2: Range fully within first xorb
        // Range [2048, 6144) covers exactly chunks 1 and 2
        {
            let start = chunk_size as u64; // Start of chunk 1 in xorb 1
            let end = (chunk_size * 3) as u64; // End of chunk 2 in xorb 1
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 1);
            // Chunk 0 pruned at start
            assert_eq!(response.terms[0].range.start, 1);
            // Chunks 3+ pruned at end (only chunks 1-2 needed for bytes [2048, 6144))
            assert_eq!(response.terms[0].range.end, 3);
            // Start is exactly at chunk 1 boundary, so offset is 0
            assert_eq!(response.offset_into_first_range, 0);
        }

        // Test 3: Range fully within second xorb
        // Range [10240, 14336) covers exactly chunks 1 and 2 of xorb 2
        {
            let xorb1_size = (chunk_size * 4) as u64;
            let start = xorb1_size + chunk_size as u64; // Start of chunk 1 in xorb 2
            let end = xorb1_size + (chunk_size * 3) as u64; // End of chunk 2 in xorb 2
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 1);
            // Chunk 0 of xorb 2 pruned at start
            assert_eq!(response.terms[0].range.start, 1);
            // Chunks 3+ pruned at end
            assert_eq!(response.terms[0].range.end, 3);
            // Start is exactly at chunk 1 boundary, so offset is 0
            assert_eq!(response.offset_into_first_range, 0);
        }

        // Test 4: Range spanning xorb boundary, ending in middle of second xorb
        // Range [4096, 12788) spans from chunk 2 of xorb 1 into chunk 2 of xorb 2
        {
            let xorb1_size = (chunk_size * 4) as u64;
            let start = (chunk_size * 2) as u64; // Start of chunk 2 in xorb 1
            let end = xorb1_size + (chunk_size * 2) as u64 + 500; // Middle of chunk 2 in xorb 2
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 2);

            // First term (xorb 1): chunks 2-3 (chunks 0-1 pruned at start, all remaining needed)
            assert_eq!(response.terms[0].range.start, 2);
            assert_eq!(response.terms[0].range.end, 4);

            // Second term (xorb 2): chunks 0-2 (chunk 2 contains the end byte, chunk 3 pruned)
            assert_eq!(response.terms[1].range.start, 0);
            assert_eq!(response.terms[1].range.end, 3);

            // Start is exactly at chunk 2 boundary in xorb 1, so offset is 0
            assert_eq!(response.offset_into_first_range, 0);
        }

        // Test 5: Off-by-one tests for range within first xorb
        // Range [2048 +/- delta, 6144 -/+ delta] tests boundary precision
        for delta in [0, 1] {
            let start = chunk_size as u64 + delta; // Start of chunk 1 +/- delta
            let end = (chunk_size * 3) as u64 - delta; // End of chunk 2 -/+ delta
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 1);
            // Chunk 0 pruned at start
            assert_eq!(response.terms[0].range.start, 1);
            // Chunks 1-2 cover the range
            assert_eq!(response.terms[0].range.end, 3);
            assert_eq!(response.offset_into_first_range, delta);
        }

        // Test 6: Range starting 1 byte before chunk boundary (requires including previous chunk)
        {
            let start = chunk_size as u64 - 1; // 1 byte before chunk 1
            let end = (chunk_size * 3) as u64 + 1; // 1 byte into chunk 3
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 1);
            // Chunk 0 must be included (range starts 1 byte before chunk 1)
            assert_eq!(response.terms[0].range.start, 0);
            // Chunk 3 must be included (range ends 1 byte into chunk 3)
            assert_eq!(response.terms[0].range.end, 4);
            // Offset is chunk_size - 1 into chunk 0
            assert_eq!(response.offset_into_first_range, chunk_size as u64 - 1);
        }

        // Test 7: Off-by-one tests spanning xorb boundary
        // Range starting at xorb 1 chunk 2 boundary with delta
        for delta in [0, 1] {
            let xorb1_size = (chunk_size * 4) as u64;
            let start = (chunk_size * 2) as u64 + delta; // Chunk 2 in xorb 1
            let end = xorb1_size + (chunk_size * 2) as u64 - delta; // Chunk 1 end in xorb 2
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 2);

            // First term (xorb 1): chunks 2-3
            assert_eq!(response.terms[0].range.start, 2);
            assert_eq!(response.terms[0].range.end, 4);

            // Second term (xorb 2): chunks 0-1
            assert_eq!(response.terms[1].range.start, 0);
            assert_eq!(response.terms[1].range.end, 2);

            assert_eq!(response.offset_into_first_range, delta);
        }

        // Test 8: Range starting 1 byte before xorb boundary (requires including xorb 1 chunk 3)
        {
            let xorb1_size = (chunk_size * 4) as u64;
            let start = xorb1_size - 1; // 1 byte before xorb 2
            let end = xorb1_size + (chunk_size * 2) as u64 + 1; // 1 byte into chunk 2 of xorb 2
            let response = client
                .get_reconstruction(&file_hash, Some(FileRange::new(start, end)))
                .await
                .unwrap()
                .unwrap();

            assert_eq!(response.terms.len(), 2);

            // First term (xorb 1): chunk 3 only (contains the last byte of xorb 1)
            assert_eq!(response.terms[0].range.start, 3);
            assert_eq!(response.terms[0].range.end, 4);

            // Second term (xorb 2): chunks 0-2 (chunk 2 contains the end byte)
            assert_eq!(response.terms[1].range.start, 0);
            assert_eq!(response.terms[1].range.end, 3);

            // Offset is chunk_size - 1 into chunk 3 of xorb 1
            assert_eq!(response.offset_into_first_range, chunk_size as u64 - 1);
        }
    }
}
