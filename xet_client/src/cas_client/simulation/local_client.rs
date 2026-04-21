use std::collections::HashMap;
use std::fs::{File, metadata};
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU16, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use rand::RngExt;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use tempfile::TempDir;
use tokio::time::{Duration, Instant};
use tracing::{error, info, warn};
use xet_core_structures::merklehash::{MerkleHash, compute_data_hash};
use xet_core_structures::metadata_shard::file_structs::{FileDataSequenceHeader, MDBFileInfo, MDBFileInfoView};
use xet_core_structures::metadata_shard::shard_format::MDB_FILE_INFO_ENTRY_SIZE;
use xet_core_structures::metadata_shard::shard_in_memory::MDBInMemoryShard;
use xet_core_structures::metadata_shard::streaming_shard::MDBMinimalShard;
use xet_core_structures::metadata_shard::utils::{parse_shard_filename, shard_file_name};
use xet_core_structures::metadata_shard::xorb_structs::MDBXorbInfo;
use xet_core_structures::metadata_shard::{MDBShardFile, MDBShardFileHeader, ShardFileManager};
use xet_core_structures::serialization_utils::read_u32;
use xet_core_structures::xorb_object::{SerializedXorbObject, XorbObject};
#[cfg(feature = "fd-track")]
use xet_runtime::fd_diagnostics::{report_fd_count, track_fd_scope};
use xet_runtime::file_utils::SafeFileCreator;

use super::deletion_controls::ObjectTag;
use super::direct_access_client::DirectAccessClient;
use super::xorb_utils::{self, REFERENCE_INSTANT, duration_to_expiration_secs_ceil};
use crate::cas_client::Client;
use crate::cas_client::adaptive_concurrency::AdaptiveConcurrencyController;
use crate::cas_client::progress_tracked_streams::ProgressCallback;
use crate::cas_types::{
    BatchQueryReconstructionResponse, FileRange, HexMerkleHash, HttpRange, QueryReconstructionResponse,
    QueryReconstructionResponseV2, XorbMultiRangeFetch, XorbRangeDescriptor, XorbReconstructionFetchInfo,
};
use crate::error::{ClientError, Result};

/// Newtype wrapper for MerkleHash to implement redb Key/Value traits.
/// MerkleHash is DataHash([u64; 4]) = 32 bytes, stored as fixed-width little-endian.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RedbHash(MerkleHash);

impl From<MerkleHash> for RedbHash {
    fn from(h: MerkleHash) -> Self {
        RedbHash(h)
    }
}

impl From<RedbHash> for MerkleHash {
    fn from(h: RedbHash) -> Self {
        h.0
    }
}

impl redb::Value for RedbHash {
    type SelfType<'a> = RedbHash;
    type AsBytes<'a> = [u8; 32];

    fn fixed_width() -> Option<usize> {
        Some(32)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let mut hash = MerkleHash::default();
        let u64s: &mut [u64; 4] = &mut hash;
        for (i, chunk) in data.chunks_exact(8).enumerate() {
            u64s[i] = u64::from_le_bytes(chunk.try_into().unwrap());
        }
        RedbHash(hash)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a + 'b,
    {
        let mut bytes = [0u8; 32];
        let u64s: &[u64; 4] = &value.0;
        for (i, &val) in u64s.iter().enumerate() {
            bytes[i * 8..(i + 1) * 8].copy_from_slice(&val.to_le_bytes());
        }
        bytes
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("MerkleHash")
    }
}

impl redb::Key for RedbHash {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1.cmp(data2)
    }
}

const GLOBAL_DEDUP_TABLE: TableDefinition<RedbHash, RedbHash> = TableDefinition::new("global_dedup");

/// Maps each active file hash to the shard that owns it.  Absence means deleted.
const FILE_TO_SHARD_TABLE: TableDefinition<RedbHash, FileShardRef> = TableDefinition::new("file_to_shard");

/// Points a file hash at the shard that canonically owns it, along with the
/// byte offset and length of the file entry within the shard.  This enables
/// direct-seek reads without parsing the entire shard.
/// Stored as 48 bytes: 32 (shard_hash) + 8 (offset) + 8 (length).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileShardRef {
    shard_hash: MerkleHash,
    offset: u64,
    length: u64,
}

impl redb::Value for FileShardRef {
    type SelfType<'a> = FileShardRef;
    type AsBytes<'a> = [u8; 48];

    fn fixed_width() -> Option<usize> {
        Some(48)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let mut hash = MerkleHash::default();
        let u64s: &mut [u64; 4] = &mut hash;
        for (i, chunk) in data[..32].chunks_exact(8).enumerate() {
            u64s[i] = u64::from_le_bytes(chunk.try_into().unwrap());
        }
        let offset = u64::from_le_bytes(data[32..40].try_into().unwrap());
        let length = u64::from_le_bytes(data[40..48].try_into().unwrap());
        FileShardRef {
            shard_hash: hash,
            offset,
            length,
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a + 'b,
    {
        let mut bytes = [0u8; 48];
        let u64s: &[u64; 4] = &value.shard_hash;
        for (i, &val) in u64s.iter().enumerate() {
            bytes[i * 8..(i + 1) * 8].copy_from_slice(&val.to_le_bytes());
        }
        bytes[32..40].copy_from_slice(&value.offset.to_le_bytes());
        bytes[40..48].copy_from_slice(&value.length.to_le_bytes());
        bytes
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("FileShardRef")
    }
}

/// Weak handle so the cache never keeps a [`redb::Database`] alive; only [`Arc`]s held by
/// [`LocalClient`] (and clones) do. When the last strong ref drops, the entry can be purged.
type CachedDbWeak = Weak<redb::Database>;

/// Global cache of redb databases keyed by canonical DB file path.
/// redb enforces exclusive file locking, so multiple LocalClient instances
/// pointing at the same directory must share a single Database handle.
static DB_CACHE: LazyLock<Mutex<HashMap<PathBuf, CachedDbWeak>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Opens or returns a shared [`Arc<redb::Database>`] for `db_path`. The map stores only
/// [`Weak`] pointers ([`Arc::downgrade`]); on a hit, [`Weak::upgrade`] yields a new strong ref.
fn get_or_open_db(db_path: &Path) -> std::result::Result<Arc<redb::Database>, redb::DatabaseError> {
    #[cfg(feature = "fd-track")]
    let _fd_scope = track_fd_scope(format!("LocalClient::get_or_open_db({})", db_path.display()));

    let mut map = DB_CACHE.lock().unwrap();

    if let Some(weak) = map.get(db_path)
        && let Some(db) = Weak::upgrade(weak)
    {
        tracing::trace!(target: "xet_client::local_cas_redb", path = %db_path.display(), "DB_CACHE hit");
        #[cfg(feature = "fd-track")]
        report_fd_count("LocalClient::get_or_open_db cache hit");
        return Ok(db);
    }

    // Purge dead entries to avoid unbounded cache growth.
    map.retain(|_, weak| weak.strong_count() > 0);

    tracing::trace!(target: "xet_client::local_cas_redb", path = %db_path.display(), "DB_CACHE miss");

    let db = Arc::new(redb::Database::create(db_path)?);
    map.insert(db_path.to_owned(), Arc::downgrade(&db));
    #[cfg(feature = "fd-track")]
    report_fd_count("LocalClient::get_or_open_db opened new DB");
    Ok(db)
}

/// Scans the file-info section of a serialized shard and returns
/// `(file_hash, byte_offset, byte_length)` for every file entry.
/// The offset/length pair identifies the contiguous blob (header + data entries +
/// verification + metadata_ext) that `MDBFileInfoView::new()` can parse directly.
fn file_entry_byte_ranges(shard_bytes: &[u8]) -> std::result::Result<Vec<(MerkleHash, u64, u64)>, ClientError> {
    let mut cursor = Cursor::new(shard_bytes);
    let _ = MDBShardFileHeader::deserialize(&mut cursor)?;

    let mut entries = Vec::new();
    loop {
        let start = cursor.position();
        let header = FileDataSequenceHeader::deserialize(&mut cursor)?;
        if header.is_bookend() {
            break;
        }

        let n = header.num_entries as usize;
        let mut n_data = n;
        if header.contains_verification() {
            n_data += n;
        }
        if header.contains_metadata_ext() {
            n_data += 1;
        }

        cursor.set_position(cursor.position() + (n_data * MDB_FILE_INFO_ENTRY_SIZE) as u64);
        entries.push((header.file_hash, start, cursor.position() - start));
    }
    Ok(entries)
}

pub struct LocalClient {
    db: Arc<redb::Database>,
    shard_manager: Arc<ShardFileManager>,
    xorb_dir: PathBuf,
    shard_dir: PathBuf,
    upload_concurrency_controller: Arc<AdaptiveConcurrencyController>,
    url_expiration_ms: AtomicU64,
    /// Global dedup shard expiration in seconds (0 = disabled).
    global_dedup_expiration_secs: AtomicU64,
    /// API delay range in milliseconds as (min_ms, max_ms). (0, 0) means disabled.
    random_ms_delay_window: (AtomicU64, AtomicU64),
    /// Max ranges per XorbMultiRangeFetch entry. usize::MAX means no splitting.
    max_ranges_per_fetch: AtomicUsize,
    /// HTTP status code to return when V2 is disabled (0 = enabled).
    v2_disabled_status: AtomicU16,
    _tmp_dir: Option<TempDir>,
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
        // `std::path::absolute` does not resolve symlinks; without canonicalizing, two path strings
        // for the same directory (e.g. via symlinks or /var vs /private/var on macOS) would open
        // multiple `redb::Database` handles and duplicate file descriptors for one CAS root.
        let base_dir = std::fs::canonicalize(&base_dir).unwrap_or(base_dir);
        #[cfg(feature = "fd-track")]
        let _fd_scope = track_fd_scope(format!("LocalClient::new_internal({})", base_dir.display()));
        #[cfg(feature = "fd-track")]
        report_fd_count("LocalClient::new_internal start");

        let shard_dir = base_dir.join("shards");
        if !shard_dir.exists() {
            std::fs::create_dir_all(&shard_dir)?;
        }

        let xorb_dir = base_dir.join("xorbs");
        if !xorb_dir.exists() {
            std::fs::create_dir_all(&xorb_dir)?;
        }

        let db_path = base_dir.join("global_dedup_lookup.redb");
        let db =
            get_or_open_db(&db_path).map_err(|e| ClientError::Other(format!("Error opening redb database: {e}")))?;
        #[cfg(feature = "fd-track")]
        report_fd_count("LocalClient::new_internal after DB open");

        // Ensure tables exist by opening a write transaction.
        {
            let write_txn = db.begin_write().map_err(map_redb_db_error)?;
            let _ = write_txn.open_table(GLOBAL_DEDUP_TABLE).map_err(map_redb_db_error)?;
            let _ = write_txn.open_table(FILE_TO_SHARD_TABLE).map_err(map_redb_db_error)?;
            write_txn.commit().map_err(map_redb_db_error)?;
        }

        // Open / set up the shard lookup
        let shard_manager = ShardFileManager::new_in_session_directory(shard_dir.clone(), true).await?;
        #[cfg(feature = "fd-track")]
        report_fd_count("LocalClient::new_internal after shard manager init");

        Ok(Self {
            db,
            shard_manager,
            xorb_dir,
            shard_dir,
            upload_concurrency_controller: AdaptiveConcurrencyController::new_upload("local_uploads"),
            url_expiration_ms: AtomicU64::new(u64::MAX),
            global_dedup_expiration_secs: AtomicU64::new(0),
            random_ms_delay_window: (AtomicU64::new(0), AtomicU64::new(0)),
            max_ranges_per_fetch: AtomicUsize::new(usize::MAX),
            v2_disabled_status: AtomicU16::new(0),
            _tmp_dir: tmp_dir,
        })
    }

    /// Internal function to get the path for a given hash entry
    fn get_path_for_entry(&self, hash: &MerkleHash) -> PathBuf {
        self.xorb_dir.join(format!("default.{hash:?}"))
    }

    #[cfg(test)]
    fn is_file_deleted(&self, file_hash: &MerkleHash) -> bool {
        let Ok(read_txn) = self.db.begin_read() else {
            return true;
        };
        let Ok(table) = read_txn.open_table(FILE_TO_SHARD_TABLE) else {
            return true;
        };
        table.get(&RedbHash::from(*file_hash)).ok().flatten().is_none()
    }

    /// Returns all shard files in the shard directory as (shard_hash, path) pairs.
    fn shard_file_paths(&self) -> Result<Vec<(MerkleHash, PathBuf)>> {
        let mut result = Vec::new();
        for entry in std::fs::read_dir(&self.shard_dir).map_err(ClientError::internal)? {
            let entry = entry.map_err(ClientError::internal)?;
            let path = entry.path();
            if let Some(hash) = parse_shard_filename(&path)
                && path.is_file()
            {
                result.push((hash, path));
            }
        }
        Ok(result)
    }

    /// Finds the path for a shard file by hash.
    fn shard_path_for_hash(&self, hash: &MerkleHash) -> Result<PathBuf> {
        let path = self.shard_dir.join(shard_file_name(hash));
        if path.exists() {
            Ok(path)
        } else {
            Err(ClientError::Other(format!("Shard file not found for hash {}", hash.hex())))
        }
    }

    /// Builds an `ObjectTag` from file metadata at the given path.
    ///
    /// We hash multiple metadata fields to increase entropy and reduce false
    /// matches during rapid rewrite/delete races.
    fn object_tag_from_path(path: &Path) -> Result<ObjectTag> {
        let meta = std::fs::metadata(path).map_err(ClientError::internal)?;
        let modified = meta.modified().map_err(ClientError::internal)?;
        let modified_nanos = modified.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos();
        let created_nanos = meta
            .created()
            .ok()
            .and_then(|ts| ts.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0u128, |d| d.as_nanos());

        let mut entropy = Vec::with_capacity(16 + 16 + 8 + 1);
        entropy.extend_from_slice(&modified_nanos.to_le_bytes());
        entropy.extend_from_slice(&created_nanos.to_le_bytes());
        entropy.extend_from_slice(&meta.len().to_le_bytes());
        entropy.push(u8::from(meta.permissions().readonly()));

        Ok(compute_data_hash(&entropy).into())
    }

    /// Restores `tmp_path` back to `original_path`.  Tries `hard_link` first
    /// (fails with EEXIST if a concurrent upload recreated the path — safe).
    /// Falls back to `rename` if hard links aren't supported.  Only removes
    /// `tmp_path` after confirming the original is in place.
    fn restore_from_tmp(tmp_path: &Path, original_path: &Path) {
        if std::fs::hard_link(tmp_path, original_path).is_ok() {
            let _ = std::fs::remove_file(tmp_path);
        } else if original_path.exists() {
            // Original path was recreated by a concurrent upload; discard stale copy.
            let _ = std::fs::remove_file(tmp_path);
        } else {
            // hard_link failed (e.g. unsupported fs) and no concurrent upload —
            // fall back to rename which always works on the same filesystem.
            let _ = std::fs::rename(tmp_path, original_path);
        }
    }

    /// Clears the readonly permission on a file so it can be deleted on Windows.
    #[cfg(windows)]
    fn clear_readonly(path: &Path) {
        if let Ok(metadata) = std::fs::metadata(path) {
            let mut permissions = metadata.permissions();
            #[allow(clippy::permissions_set_readonly_false)]
            permissions.set_readonly(false);
            let _ = std::fs::set_permissions(path, permissions);
        }
    }

    /// Loads all shard data from disk into an in-memory shard.
    #[cfg(test)]
    fn load_all_shard_data(&self) -> Result<MDBInMemoryShard> {
        let mut in_memory = MDBInMemoryShard::default();
        for (_, path) in self.shard_file_paths()? {
            let shard_bytes = std::fs::read(&path)?;
            let minimal_shard = MDBMinimalShard::from_reader(&mut Cursor::new(&shard_bytes), true, true)?;

            for i in 0..minimal_shard.num_files() {
                in_memory.add_file_reconstruction_info(MDBFileInfo::from(minimal_shard.file(i).unwrap()))?;
            }
            for i in 0..minimal_shard.num_xorb() {
                in_memory.add_xorb_block(MDBXorbInfo::from(minimal_shard.xorb(i).unwrap()))?;
            }
        }
        Ok(in_memory)
    }

    /// Clears shard files from disk, writes the given in-memory shard, and
    /// registers the new shard file with the shard manager.
    #[cfg(test)]
    async fn write_shard_data_and_register(&self, in_memory: &MDBInMemoryShard) -> Result<()> {
        for (_, path) in self.shard_file_paths()? {
            std::fs::remove_file(&path)?;
        }

        if !in_memory.is_empty() {
            let shard_path = in_memory.write_to_directory(&self.shard_dir, None)?;
            let shard = MDBShardFile::load_from_file(&shard_path)?;
            let shard_hash = shard.shard_hash;
            self.shard_manager.register_shards(&[shard]).await?;

            // Update FILE_TO_SHARD_TABLE with byte-accurate offsets.
            let shard_bytes = std::fs::read(&shard_path)?;
            let file_ranges = file_entry_byte_ranges(&shard_bytes)?;
            let write_txn = self.db.begin_write().map_err(map_redb_db_error)?;
            {
                let mut file_table = write_txn.open_table(FILE_TO_SHARD_TABLE).map_err(map_redb_db_error)?;
                for (file_hash, offset, length) in &file_ranges {
                    file_table
                        .insert(
                            &RedbHash::from(*file_hash),
                            &FileShardRef {
                                shard_hash,
                                offset: *offset,
                                length: *length,
                            },
                        )
                        .map_err(map_redb_db_error)?;
                }
            }
            write_txn.commit().map_err(map_redb_db_error)?;
        }

        Ok(())
    }
}

impl Drop for LocalClient {
    fn drop(&mut self) {
        #[cfg(feature = "fd-track")]
        let _fd_scope = track_fd_scope(format!("LocalClient::drop({})", self.xorb_dir.display()));

        #[cfg(feature = "fd-track")]
        {
            report_fd_count("LocalClient::drop start");
            if let Ok(mut map) = DB_CACHE.lock() {
                map.retain(|_, weak| weak.strong_count() > 0);
            }
            report_fd_count("LocalClient::drop end");
        }
    }
}

#[async_trait]
impl DirectAccessClient for LocalClient {
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
        LocalClient::get_reconstruction_v1(self, file_id, bytes_range).await
    }

    async fn get_reconstruction_v2(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<Option<QueryReconstructionResponseV2>> {
        LocalClient::get_reconstruction_v2(self, file_id, bytes_range).await
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
        let mut ret = Vec::new();
        self.xorb_dir
            .read_dir()
            .map_err(ClientError::internal)?
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

    async fn get_full_xorb(&self, hash: &MerkleHash) -> Result<Bytes> {
        let file_path = self.get_path_for_entry(hash);
        let file = File::open(&file_path).map_err(|_| {
            error!("Unable to find file in local CAS {:?}", file_path);
            ClientError::XORBNotFound(*hash)
        })?;

        let mut reader = BufReader::new(file);
        let xorb_obj = XorbObject::deserialize(&mut reader)?;
        let result = xorb_obj.get_all_bytes(&mut reader)?;
        Ok(Bytes::from(result))
    }

    async fn get_xorb_ranges(&self, hash: &MerkleHash, chunk_ranges: Vec<(u32, u32)>) -> Result<Vec<Bytes>> {
        if chunk_ranges.is_empty() {
            return Ok(vec![Bytes::new()]);
        }

        let file_path = self.get_path_for_entry(hash);
        let file = File::open(&file_path).map_err(|_| {
            error!("Unable to find file in local CAS {:?}", file_path);
            ClientError::XORBNotFound(*hash)
        })?;

        let mut reader = BufReader::new(file);
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
    }

    async fn xorb_length(&self, hash: &MerkleHash) -> Result<u32> {
        let file_path = self.get_path_for_entry(hash);
        match File::open(file_path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);
                let xorb_obj = XorbObject::deserialize(&mut reader)?;
                let length = xorb_obj.get_all_bytes(&mut reader)?.len();
                Ok(length as u32)
            },
            Err(_) => Err(ClientError::XORBNotFound(*hash)),
        }
    }

    async fn xorb_exists(&self, hash: &MerkleHash) -> Result<bool> {
        let file_path = self.get_path_for_entry(hash);

        let Ok(md) = metadata(&file_path) else {
            return Ok(false);
        };

        if !md.is_file() {
            return Err(ClientError::InternalError(anyhow!(
                "Attempting to write to {file_path:?}, but it is not a file"
            )));
        }

        let Ok(file) = File::open(&file_path) else {
            return Err(ClientError::XORBNotFound(*hash));
        };

        let mut reader = BufReader::new(file);
        XorbObject::deserialize(&mut reader)?;
        Ok(true)
    }

    async fn xorb_footer(&self, hash: &MerkleHash) -> Result<XorbObject> {
        let file_path = self.get_path_for_entry(hash);
        let mut file = File::open(&file_path).map_err(|_| {
            error!("Unable to find xorb in local CAS {:?}", file_path);
            ClientError::XORBNotFound(*hash)
        })?;

        file.seek(SeekFrom::End(-(size_of::<u32>() as i64)))?;
        let info_length = read_u32(&mut file)?;

        file.seek(SeekFrom::End(-(info_length as i64)))?;

        let mut reader = BufReader::new(file);
        let xorb_obj = XorbObject::deserialize(&mut reader)?;
        Ok(xorb_obj)
    }

    async fn get_file_size(&self, hash: &MerkleHash) -> Result<u64> {
        let Some((file_info, _)) = self.get_file_info_from_table(hash)? else {
            return Err(ClientError::FileNotFound(*hash));
        };
        Ok(file_info.file_size())
    }

    async fn get_file_data(&self, hash: &MerkleHash, byte_range: Option<FileRange>) -> Result<Bytes> {
        let Some((file_info, _)) = self.get_file_info_from_table(hash)? else {
            return Err(ClientError::FileNotFound(*hash));
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
        let file_path = self.get_path_for_entry(hash);
        let data = std::fs::read(&file_path).map_err(|_| ClientError::XORBNotFound(*hash))?;

        let start = byte_range.as_ref().map(|r| r.start as usize).unwrap_or(0);
        let end = byte_range
            .as_ref()
            .map(|r| r.end as usize)
            .unwrap_or(data.len())
            .min(data.len());

        if start >= data.len() {
            return Err(ClientError::InvalidRange);
        }

        Ok(Bytes::from(data[start..end].to_vec()))
    }

    async fn xorb_raw_length(&self, hash: &MerkleHash) -> Result<u64> {
        let file_path = self.get_path_for_entry(hash);
        let metadata = std::fs::metadata(&file_path).map_err(|_| ClientError::XORBNotFound(*hash))?;
        Ok(metadata.len())
    }

    async fn fetch_term_data(
        &self,
        hash: MerkleHash,
        fetch_term: XorbReconstructionFetchInfo,
    ) -> Result<(Bytes, Vec<u32>)> {
        self.apply_api_delay().await;
        let (file_path, url_byte_range, url_timestamp) = parse_fetch_url(&fetch_term.url)?;

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
        let file = File::open(&file_path).map_err(|_| {
            error!("Unable to find xorb in local CAS {:?}", file_path);
            ClientError::XORBNotFound(hash)
        })?;

        let mut reader = BufReader::new(file);
        let xorb_obj = XorbObject::deserialize(&mut reader)?;

        let data = xorb_obj.get_bytes_by_chunk_range(&mut reader, fetch_term.range.start, fetch_term.range.end)?;

        let chunk_byte_indices = {
            let mut indices = Vec::new();
            let mut cumulative = 0u32;
            // Start with 0, matching the format from deserialize_chunks_from_stream
            indices.push(0);
            // ChunkRange is exclusive-end, so we iterate from start to end (exclusive)
            for chunk_idx in fetch_term.range.start..fetch_term.range.end {
                let chunk_len = xorb_obj
                    .uncompressed_chunk_length(chunk_idx)
                    .map_err(|e| ClientError::Other(format!("Failed to get chunk length: {e}")))?;
                cumulative += chunk_len;
                indices.push(cumulative);
            }
            indices
        };

        Ok((data.into(), chunk_byte_indices))
    }
}

impl LocalClient {
    /// Removes all FILE_TO_SHARD_TABLE entries whose shard_hash equals `shard_hash`.
    fn remove_file_entries_for_shard(&self, shard_hash: &MerkleHash) -> Result<()> {
        let to_remove: Vec<RedbHash> = {
            let read_txn = self.db.begin_read().map_err(map_redb_db_error)?;
            let table = read_txn.open_table(FILE_TO_SHARD_TABLE).map_err(map_redb_db_error)?;
            table
                .iter()
                .map_err(map_redb_db_error)?
                .filter_map(|e| e.ok())
                .filter(|(_, v)| v.value().shard_hash == *shard_hash)
                .map(|(k, _)| k.value())
                .collect()
        };
        if !to_remove.is_empty() {
            let write_txn = self.db.begin_write().map_err(map_redb_db_error)?;
            {
                let mut table = write_txn.open_table(FILE_TO_SHARD_TABLE).map_err(map_redb_db_error)?;
                for key in &to_remove {
                    table.remove(key).map_err(map_redb_db_error)?;
                }
            }
            write_txn.commit().map_err(map_redb_db_error)?;
        }
        Ok(())
    }
}

#[async_trait]
impl super::DeletionControlableClient for LocalClient {
    async fn list_shard_entries(&self) -> Result<Vec<MerkleHash>> {
        Ok(self.shard_file_paths()?.into_iter().map(|(h, _)| h).collect())
    }

    async fn get_shard_bytes(&self, hash: &MerkleHash) -> Result<Bytes> {
        let path = self.shard_path_for_hash(hash)?;
        let data = std::fs::read(&path)?;
        Ok(Bytes::from(data))
    }

    async fn delete_shard_entry(&self, hash: &MerkleHash) -> Result<()> {
        let path = self.shard_path_for_hash(hash)?;
        self.remove_file_entries_for_shard(hash)?;
        std::fs::remove_file(&path)?;
        Ok(())
    }

    async fn list_file_shard_entries(&self) -> Result<Vec<(MerkleHash, MerkleHash)>> {
        let read_txn = self.db.begin_read().map_err(map_redb_db_error)?;
        let table = read_txn.open_table(FILE_TO_SHARD_TABLE).map_err(map_redb_db_error)?;
        let mut entries = Vec::new();
        for entry in table.iter().map_err(map_redb_db_error)? {
            let (key, value) = entry.map_err(map_redb_db_error)?;
            let file_hash: MerkleHash = key.value().into();
            let shard_ref: FileShardRef = value.value();
            entries.push((file_hash, shard_ref.shard_hash));
        }
        Ok(entries)
    }

    async fn delete_file_entry(&self, file_hash: &MerkleHash) -> Result<()> {
        let write_txn = self.db.begin_write().map_err(map_redb_db_error)?;
        {
            let mut table = write_txn.open_table(FILE_TO_SHARD_TABLE).map_err(map_redb_db_error)?;
            table.remove(&RedbHash::from(*file_hash)).map_err(map_redb_db_error)?;
        }
        write_txn.commit().map_err(map_redb_db_error)?;
        Ok(())
    }

    async fn remove_shard_dedup_entries(&self, shard_hash: &MerkleHash) -> Result<()> {
        let shard_redb = RedbHash::from(*shard_hash);
        for _ in 0..4 {
            let to_delete: Vec<RedbHash> = {
                let read_txn = self.db.begin_read().map_err(map_redb_db_error)?;
                let table = read_txn.open_table(GLOBAL_DEDUP_TABLE).map_err(map_redb_db_error)?;
                table
                    .iter()
                    .map_err(map_redb_db_error)?
                    .filter_map(|entry| entry.ok())
                    .filter(|(_, v)| v.value() == shard_redb)
                    .map(|(k, _)| k.value())
                    .collect()
            };

            if to_delete.is_empty() {
                return Ok(());
            }

            let write_txn = self.db.begin_write().map_err(map_redb_db_error)?;
            {
                let mut table = write_txn.open_table(GLOBAL_DEDUP_TABLE).map_err(map_redb_db_error)?;
                for chunk_hash in &to_delete {
                    table.remove(chunk_hash).map_err(map_redb_db_error)?;
                }
            }
            write_txn.commit().map_err(map_redb_db_error)?;
        }

        let still_present = {
            let read_txn = self.db.begin_read().map_err(map_redb_db_error)?;
            let table = read_txn.open_table(GLOBAL_DEDUP_TABLE).map_err(map_redb_db_error)?;
            table
                .iter()
                .map_err(map_redb_db_error)?
                .filter_map(|entry| entry.ok())
                .any(|(_, v)| v.value() == shard_redb)
        };

        if still_present {
            return Err(ClientError::Other(format!(
                "Unable to fully remove dedup entries for shard {} due to concurrent updates",
                shard_hash.hex()
            )));
        }

        Ok(())
    }

    async fn delete_xorb(&self, hash: &MerkleHash) {
        let file_path = self.get_path_for_entry(hash);

        #[cfg(windows)]
        Self::clear_readonly(&file_path);

        let _ = std::fs::remove_file(file_path);
    }

    async fn list_xorbs_and_tags(&self) -> Result<Vec<(MerkleHash, ObjectTag)>> {
        let mut ret = Vec::new();
        for entry in self.xorb_dir.read_dir().map_err(ClientError::internal)? {
            let entry = entry.map_err(ClientError::internal)?;
            let path = entry.path();
            let Some(name) = entry.file_name().into_string().ok() else {
                continue;
            };
            if let Some(pos) = name.rfind('.') {
                let hex = &name[(pos + 1)..];
                if let Ok(hash) = MerkleHash::from_hex(hex) {
                    let tag = Self::object_tag_from_path(&path)?;
                    ret.push((hash, tag));
                }
            }
        }
        Ok(ret)
    }

    async fn delete_xorb_if_tag_matches(&self, hash: &MerkleHash, tag: &ObjectTag) -> Result<bool> {
        let file_path = self.get_path_for_entry(hash);

        // Atomically move the file out of the namespace before checking the
        // tag.  This closes the TOCTOU window with concurrent upload_xorb
        // (which always rewrites via SafeFileCreator atomic rename).
        let tmp_path = file_path.with_extension(format!("gc_del_{:x}", rand::random::<u64>()));
        if std::fs::rename(&file_path, &tmp_path).is_err() {
            return Err(ClientError::XORBNotFound(*hash));
        }

        let current_tag = match Self::object_tag_from_path(&tmp_path) {
            Ok(t) => t,
            Err(e) => {
                Self::restore_from_tmp(&tmp_path, &file_path);
                return Err(e);
            },
        };

        if &current_tag != tag {
            Self::restore_from_tmp(&tmp_path, &file_path);
            return Ok(false);
        }

        #[cfg(windows)]
        Self::clear_readonly(&tmp_path);

        std::fs::remove_file(&tmp_path)?;
        Ok(true)
    }

    async fn list_shards_with_tags(&self) -> Result<Vec<(MerkleHash, ObjectTag)>> {
        let mut ret = Vec::new();
        for (hash, path) in self.shard_file_paths()? {
            let tag = Self::object_tag_from_path(&path)?;
            ret.push((hash, tag));
        }
        Ok(ret)
    }

    async fn delete_shard_if_tag_matches(&self, hash: &MerkleHash, tag: &ObjectTag) -> Result<bool> {
        let path = self.shard_path_for_hash(hash)?;

        let tmp_path = path.with_extension(format!("gc_del_{:x}", rand::random::<u64>()));
        if std::fs::rename(&path, &tmp_path).is_err() {
            return Err(ClientError::Other(format!("Shard not found: {}", hash.hex())));
        }

        let current_tag = match Self::object_tag_from_path(&tmp_path) {
            Ok(t) => t,
            Err(e) => {
                Self::restore_from_tmp(&tmp_path, &path);
                return Err(e);
            },
        };

        if &current_tag != tag {
            Self::restore_from_tmp(&tmp_path, &path);
            return Ok(false);
        }

        if let Err(e) = self.remove_file_entries_for_shard(hash) {
            Self::restore_from_tmp(&tmp_path, &path);
            return Err(e);
        }
        std::fs::remove_file(&tmp_path)?;
        Ok(true)
    }

    /// Verifies referential integrity of all shards on disk:
    /// 1. For each XORB entry listed in any shard, the corresponding XORB file must exist on disk.
    /// 2. For each file entry in any shard, every referenced XORB must exist on disk. (Global-dedup allows file entries
    ///    to reference XORBs described in a different shard, so we do a cross-shard check against disk rather than a
    ///    within-shard check.)
    async fn verify_all_reachable(&self) -> Result<()> {
        let shard_files = self.shard_file_paths()?;

        // Build a map of file_hash -> shard_hash from the authoritative table.
        // A file entry in a shard is only considered active if the table maps that
        // file hash to that specific shard, preventing stale entries from resurrecting.
        let file_to_shard: HashMap<MerkleHash, MerkleHash> = {
            let read_txn = self.db.begin_read().map_err(map_redb_db_error)?;
            let table = read_txn.open_table(FILE_TO_SHARD_TABLE).map_err(map_redb_db_error)?;
            let mut map = HashMap::new();
            for entry in table.iter().map_err(map_redb_db_error)? {
                let (k, v) = entry.map_err(map_redb_db_error)?;
                let fh: MerkleHash = k.value().into();
                let sr: FileShardRef = v.value();
                map.insert(fh, sr.shard_hash);
            }
            map
        };

        // Collect xorbs claimed by shard xorb-entries, xorbs referenced by active file
        // entries (cross-shard dedup), and which shards have at least one active file.
        let mut xorbs_in_shard_entries: std::collections::HashSet<MerkleHash> = std::collections::HashSet::new();
        let mut xorbs_in_active_file_entries: std::collections::HashSet<MerkleHash> = std::collections::HashSet::new();
        let mut shards_with_active_files: std::collections::HashSet<MerkleHash> = std::collections::HashSet::new();
        let mut shard_xorbs: std::collections::HashMap<MerkleHash, Vec<MerkleHash>> = std::collections::HashMap::new();

        for (shard_hash, path) in &shard_files {
            let shard_bytes = std::fs::read(path)?;
            let minimal_shard = MDBMinimalShard::from_reader(&mut Cursor::new(&shard_bytes), true, true)?;

            for i in 0..minimal_shard.num_xorb() {
                let xorb_hash = minimal_shard.xorb(i).unwrap().xorb_hash();
                xorbs_in_shard_entries.insert(xorb_hash);
                shard_xorbs.entry(*shard_hash).or_default().push(xorb_hash);
            }

            let mut has_active_file = false;
            for i in 0..minimal_shard.num_files() {
                let file_view = minimal_shard.file(i).unwrap();
                let fh = file_view.file_hash();
                if file_to_shard.get(&fh) == Some(shard_hash) {
                    has_active_file = true;
                    for seg_idx in 0..file_view.num_entries() {
                        xorbs_in_active_file_entries.insert(file_view.entry(seg_idx).xorb_hash);
                    }
                }
            }
            if has_active_file {
                shards_with_active_files.insert(*shard_hash);
            }
        }

        let mut errors: Vec<String> = Vec::new();

        // Check 1: every on-disk shard must be "reachable".
        // A shard is reachable if it has at least one active (non-deleted) file entry, OR if it is
        // a compact shard (no file entries) that holds xorb-entries for xorbs referenced by some
        // active file (cross-shard dedup case — the compact shard is needed for dedup lookups).
        // A shard that satisfies neither condition is truly orphaned and GC should have deleted it.
        for (shard_hash, _) in &shard_files {
            if !shards_with_active_files.contains(shard_hash) {
                let has_file_referenced_xorb = shard_xorbs
                    .get(shard_hash)
                    .is_some_and(|xorbs| xorbs.iter().any(|x| xorbs_in_active_file_entries.contains(x)));
                if !has_file_referenced_xorb {
                    errors.push(format!(
                        "Reachability error: shard {} has no active file entries and no \
                         xorbs referenced by any active file (GC should have deleted it)",
                        shard_hash.hex()
                    ));
                }
            }
        }

        // Check 2: every on-disk xorb must be reachable — either indexed by a shard's
        // xorb entries, or referenced directly by an active file's file entries (cross-shard
        // dedup). Xorbs that satisfy neither are orphaned and GC should have deleted them.
        for xorb_hash in self.list_xorbs().await? {
            if !xorbs_in_shard_entries.contains(&xorb_hash) && !xorbs_in_active_file_entries.contains(&xorb_hash) {
                errors.push(format!(
                    "Reachability error: xorb {} exists on disk but is not referenced by \
                     any shard xorb entry or active file entry (GC should have deleted it)",
                    xorb_hash.hex()
                ));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ClientError::Other(errors.join("\n")))
        }
    }

    async fn verify_integrity(&self) -> Result<()> {
        // Snapshot the dedup table before reading the directory.  This prevents
        // a TOCTOU race: upload_shard writes the file then commits the dedup
        // entry, so any entry visible in this MVCC snapshot is guaranteed to
        // have its shard file already on disk when we read the directory below.
        let read_txn = self.db.begin_read().map_err(map_redb_db_error)?;

        let shard_files = self.shard_file_paths()?;

        // Pass 1: collect all XORB hashes listed across all shards and build
        // a global chunk-count index.  Also verify each listed XORB file exists.
        let mut global_xorb_chunk_counts: HashMap<MerkleHash, usize> = HashMap::new();
        for (shard_hash, path) in &shard_files {
            let shard_bytes = std::fs::read(path)?;
            let minimal_shard = MDBMinimalShard::from_reader(&mut Cursor::new(&shard_bytes), true, true)?;

            for i in 0..minimal_shard.num_xorb() {
                let xorb_view = minimal_shard.xorb(i).unwrap();
                let xorb_hash = xorb_view.xorb_hash();

                let xorb_path = self.get_path_for_entry(&xorb_hash);
                if !xorb_path.exists() {
                    return Err(ClientError::Other(format!(
                        "Integrity error: shard {} references non-existent XORB {}",
                        shard_hash.hex(),
                        xorb_hash.hex()
                    )));
                }

                global_xorb_chunk_counts.entry(xorb_hash).or_insert(xorb_view.num_entries());
            }
        }

        // Pass 2: validate every file entry registered in FILE_TO_SHARD_TABLE.
        // Uses offset/length for direct-seek reads — no full-shard parsing needed.
        // Reuses `read_txn` from the top so all passes see a consistent MVCC snapshot.
        let file_table = read_txn.open_table(FILE_TO_SHARD_TABLE).map_err(map_redb_db_error)?;

        for entry in file_table.iter().map_err(map_redb_db_error)? {
            let (key, value) = entry.map_err(map_redb_db_error)?;
            let file_hash: MerkleHash = key.value().into();
            let shard_ref: FileShardRef = value.value();

            let shard_path = self.shard_dir.join(shard_file_name(&shard_ref.shard_hash));
            if !shard_path.exists() {
                return Err(ClientError::Other(format!(
                    "Integrity error: FILE_TO_SHARD_TABLE maps file {} to shard {} which does not exist on disk",
                    file_hash.hex(),
                    shard_ref.shard_hash.hex()
                )));
            }

            let mut shard_file = File::open(&shard_path)?;
            shard_file.seek(SeekFrom::Start(shard_ref.offset))?;
            let mut buf = vec![0u8; shard_ref.length as usize];
            shard_file.read_exact(&mut buf)?;

            let file_view = MDBFileInfoView::new(Bytes::from(buf)).map_err(|e| {
                ClientError::Other(format!(
                    "Integrity error: cannot parse file entry for {} in shard {} at offset {}: {}",
                    file_hash.hex(),
                    shard_ref.shard_hash.hex(),
                    shard_ref.offset,
                    e
                ))
            })?;

            if file_view.file_hash() != file_hash {
                return Err(ClientError::Other(format!(
                    "Integrity error: FILE_TO_SHARD_TABLE maps file {} to shard {} offset {} but found file {} there",
                    file_hash.hex(),
                    shard_ref.shard_hash.hex(),
                    shard_ref.offset,
                    file_view.file_hash().hex()
                )));
            }

            for seg_idx in 0..file_view.num_entries() {
                let segment = file_view.entry(seg_idx);
                let xorb_path = self.get_path_for_entry(&segment.xorb_hash);

                if let Some(&chunk_count) = global_xorb_chunk_counts.get(&segment.xorb_hash) {
                    if segment.chunk_index_end as usize > chunk_count {
                        return Err(ClientError::Other(format!(
                            "Integrity error: file {} references chunk range {}..{} \
                             but XORB block {} only has {} chunks",
                            file_hash.hex(),
                            segment.chunk_index_start,
                            segment.chunk_index_end,
                            segment.xorb_hash.hex(),
                            chunk_count
                        )));
                    }
                } else if xorb_path.exists() {
                    // XORB not in any shard index but file exists — dedup reference, OK.
                } else {
                    return Err(ClientError::Other(format!(
                        "Integrity error: file {} in shard {} references XORB {} \
                         that has no shard index entry and no XORB file on disk",
                        file_hash.hex(),
                        shard_ref.shard_hash.hex(),
                        segment.xorb_hash.hex()
                    )));
                }
            }
        }

        // Pass 3: verify that all shards referenced by the global dedup chunk table
        // are present on disk.  Uses the read transaction snapshotted at the top
        // of this function to avoid TOCTOU races with concurrent uploads.
        let shard_hashes_on_disk: std::collections::HashSet<MerkleHash> = shard_files.iter().map(|(h, _)| *h).collect();

        let dedup_table = read_txn.open_table(GLOBAL_DEDUP_TABLE).map_err(map_redb_db_error)?;
        for entry in dedup_table.iter().map_err(map_redb_db_error)? {
            let (chunk_key, shard_val) = entry.map_err(map_redb_db_error)?;
            let shard_hash: MerkleHash = shard_val.value().into();
            if !shard_hashes_on_disk.contains(&shard_hash) {
                let chunk_hash: MerkleHash = chunk_key.value().into();
                return Err(ClientError::Other(format!(
                    "Integrity error: global dedup table maps chunk {} to shard {} \
                     which does not exist on disk",
                    chunk_hash.hex(),
                    shard_hash.hex()
                )));
            }
        }

        Ok(())
    }
}

impl LocalClient {
    /// Looks up a file hash in FILE_TO_SHARD_TABLE and reads its reconstruction info
    /// via a direct-seek into the canonical shard on disk.  Returns `None` if the file
    /// is not registered (i.e. deleted or never uploaded).
    fn get_file_info_from_table(&self, file_hash: &MerkleHash) -> Result<Option<(MDBFileInfo, MerkleHash)>> {
        let read_txn = self.db.begin_read().map_err(map_redb_db_error)?;
        let table = read_txn.open_table(FILE_TO_SHARD_TABLE).map_err(map_redb_db_error)?;
        let Some(entry) = table.get(&RedbHash::from(*file_hash)).map_err(map_redb_db_error)? else {
            return Ok(None);
        };
        let shard_ref: FileShardRef = entry.value();
        let shard_path = self.shard_dir.join(shard_file_name(&shard_ref.shard_hash));

        let mut file = File::open(&shard_path)?;
        file.seek(SeekFrom::Start(shard_ref.offset))?;
        let mut buf = vec![0u8; shard_ref.length as usize];
        file.read_exact(&mut buf)?;

        let file_view = MDBFileInfoView::new(Bytes::from(buf))?;
        Ok(Some((MDBFileInfo::from(&file_view), shard_ref.shard_hash)))
    }

    async fn compute_reconstruction_ranges(
        &self,
        file_id: &MerkleHash,
        bytes_range: Option<FileRange>,
    ) -> Result<xorb_utils::ReconstructionRangesResult> {
        let Some((file_info, _)) = self.get_file_info_from_table(file_id)? else {
            return Ok(None);
        };

        xorb_utils::compute_reconstruction_ranges(&file_info, bytes_range, &mut |hash| self.xorb_footer_sync(hash))
    }

    fn xorb_footer_sync(&self, hash: &MerkleHash) -> Result<XorbObject> {
        let file_path = self.get_path_for_entry(hash);
        let mut file = File::open(&file_path).map_err(|_| {
            error!("Unable to find file in local CAS {:?}", file_path);
            ClientError::XORBNotFound(*hash)
        })?;
        XorbObject::deserialize(&mut file).map_err(Into::into)
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
            let file_path = self.get_path_for_entry(&hash);
            let entries = ranges
                .into_iter()
                .map(|r| XorbReconstructionFetchInfo {
                    range: r.chunk_range,
                    url: generate_fetch_url(&file_path, &r.byte_range, timestamp),
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

#[async_trait]
impl Client for LocalClient {
    async fn get_file_reconstruction_info(
        &self,
        file_hash: &MerkleHash,
    ) -> Result<Option<(MDBFileInfo, Option<MerkleHash>)>> {
        self.apply_api_delay().await;
        Ok(self.get_file_info_from_table(file_hash)?.map(|(info, sh)| (info, Some(sh))))
    }

    async fn query_for_global_dedup_shard(&self, _prefix: &str, chunk_hash: &MerkleHash) -> Result<Option<Bytes>> {
        self.apply_api_delay().await;
        let read_txn = self.db.begin_read().map_err(map_redb_db_error)?;
        let table = read_txn.open_table(GLOBAL_DEDUP_TABLE).map_err(map_redb_db_error)?;

        if let Some(shard) = table.get(&RedbHash::from(*chunk_hash)).map_err(map_redb_db_error)? {
            let shard_hash: MerkleHash = shard.value().into();
            let filename = self.shard_dir.join(shard_file_name(&shard_hash));

            let expiration_secs = self.global_dedup_expiration_secs.load(Ordering::Relaxed);
            if expiration_secs == 0 {
                return Ok(Some(std::fs::read(filename)?.into()));
            }

            let expiry = std::time::SystemTime::now() + Duration::from_secs(expiration_secs);
            let shard_bytes = std::fs::read(filename)?;

            let mut reader = Cursor::new(&shard_bytes);
            let minimal_shard = MDBMinimalShard::from_reader(&mut reader, true, true)?;

            let mut out = Vec::new();
            minimal_shard.serialize_xorb_subset_with_expiry(&mut out, Some(expiry), |_| true)?;
            Ok(Some(out.into()))
        } else {
            Ok(None)
        }
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

        // Rebuild a full in-memory shard to rebuild the new shard.  Quick and convenient.
        let mut in_memory_shard = MDBInMemoryShard::default();

        // Add file info from the views
        for i in 0..minimal_shard.num_files() {
            let file_view = minimal_shard.file(i).unwrap();
            in_memory_shard.add_file_reconstruction_info(MDBFileInfo::from(file_view))?;
        }

        // Add XORB info from the views
        for i in 0..minimal_shard.num_xorb() {
            let xorb_view = minimal_shard.xorb(i).unwrap();
            in_memory_shard.add_xorb_block(MDBXorbInfo::from(xorb_view))?;
        }

        // Write the rebuilt shard to disk (creates proper lookup tables)
        let shard_path = in_memory_shard.write_to_directory(&self.shard_dir, None)?;
        let shard = MDBShardFile::load_from_file(&shard_path)?;
        let shard_hash = shard.shard_hash;

        self.shard_manager.register_shards(&[shard]).await?;

        // Get global dedup chunks from the minimal shard
        let chunk_hashes = minimal_shard.global_dedup_eligible_chunks();

        // Compute byte ranges for each file entry in the written shard
        let written_shard_bytes = std::fs::read(&shard_path)?;
        let file_ranges = file_entry_byte_ranges(&written_shard_bytes)?;

        let shard_hash_redb = RedbHash::from(shard_hash);
        let write_txn = self.db.begin_write().map_err(map_redb_db_error)?;
        {
            let mut dedup_table = write_txn.open_table(GLOBAL_DEDUP_TABLE).map_err(map_redb_db_error)?;
            for chunk in chunk_hashes {
                dedup_table
                    .insert(&RedbHash::from(chunk), &shard_hash_redb)
                    .map_err(map_redb_db_error)?;
            }

            let mut file_table = write_txn.open_table(FILE_TO_SHARD_TABLE).map_err(map_redb_db_error)?;
            for (file_hash, offset, length) in &file_ranges {
                file_table
                    .insert(
                        &RedbHash::from(*file_hash),
                        &FileShardRef {
                            shard_hash,
                            offset: *offset,
                            length: *length,
                        },
                    )
                    .map_err(map_redb_db_error)?;
            }
        }
        write_txn.commit().map_err(map_redb_db_error)?;

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

        // Always rewrite: even if the xorb already exists, the file must be
        // re-created so its filesystem metadata (mtime/ctime) changes, producing
        // a new tag for delete_xorb_if_tag_matches.  SafeFileCreator uses
        // temp-file + atomic rename, so concurrent readers are safe.

        // Reconstruct footer if not present
        let data_to_write = if footer_start.is_some() {
            serialized_data
        } else {
            let mut data_with_footer = Vec::new();
            let (_, computed_hash) = xet_core_structures::xorb_object::reconstruct_xorb_with_footer(
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
        // Retry loop: try to fetch, and if URL expired, refresh and retry once.
        for attempt in 0..2 {
            self.apply_api_delay().await;
            let (url, http_ranges) = url_info.retrieve_url().await?;

            let (file_path, url_timestamp) = if let Ok((path, _, ts)) = parse_fetch_url(&url) {
                (path, ts)
            } else {
                let (hash, ts, _) = xorb_utils::parse_v2_fetch_url(&url)?;
                (self.get_path_for_entry(&hash), ts)
            };

            // Check if URL has expired
            let expiration_ms = self.url_expiration_ms.load(Ordering::Relaxed);
            let elapsed_ms = Instant::now().saturating_duration_since(url_timestamp).as_millis() as u64;
            if elapsed_ms > expiration_ms {
                if attempt == 0 {
                    // First attempt failed due to expiration - refresh URL and retry.
                    url_info.refresh_url().await?;
                    continue;
                }
                return Err(ClientError::PresignedUrlExpirationError);
            }

            // Read each byte range from the serialized file and deserialize the chunks.
            let mut file = File::open(&file_path).map_err(|_| ClientError::XORBNotFound(MerkleHash::default()))?;

            let mut all_decompressed = Vec::new();
            let mut all_chunk_indices = Vec::<u32>::new();
            let mut total_transfer = 0u64;

            for http_range in &http_ranges {
                let len = http_range.length() as usize;
                total_transfer += http_range.length();

                file.seek(SeekFrom::Start(http_range.start))?;
                let mut data = vec![0u8; len];
                std::io::Read::read_exact(&mut file, &mut data)?;

                let (decompressed, chunk_indices) =
                    xet_core_structures::xorb_object::deserialize_chunks(&mut Cursor::new(&data))?;

                xet_core_structures::xorb_object::append_chunk_segment(
                    &mut all_decompressed,
                    &mut all_chunk_indices,
                    &decompressed,
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
            return Ok((Bytes::from(all_decompressed), all_chunk_indices));
        }

        // Should not reach here, but return error if we do.
        Err(ClientError::PresignedUrlExpirationError)
    }
}

fn map_redb_db_error(e: impl std::fmt::Debug) -> ClientError {
    let msg = format!("Global shard dedup database error: {e:?}");
    warn!("{msg}");
    ClientError::Other(msg)
}

fn generate_fetch_url(file_path: &Path, byte_range: &FileRange, timestamp: Instant) -> String {
    let timestamp_ms = timestamp.saturating_duration_since(*REFERENCE_INSTANT).as_millis() as u64;
    format!("{}:{}:{}:{}", file_path.display(), byte_range.start, byte_range.end, timestamp_ms)
}

fn parse_fetch_url(url: &str) -> Result<(PathBuf, FileRange, Instant)> {
    let mut parts = url.rsplitn(4, ':').collect::<Vec<_>>();
    parts.reverse();

    if parts.len() != 4 {
        return Err(ClientError::InvalidArguments);
    }

    let file_path_str = parts[0];
    let start_pos: u64 = parts[1].parse().map_err(|_| ClientError::InvalidArguments)?;
    let end_pos: u64 = parts[2].parse().map_err(|_| ClientError::InvalidArguments)?;
    let timestamp_ms: u64 = parts[3].parse().map_err(|_| ClientError::InvalidArguments)?;

    let file_path: PathBuf = file_path_str.into();
    let byte_range = FileRange::new(start_pos, end_pos);
    let timestamp = *REFERENCE_INSTANT + Duration::from_millis(timestamp_ms);

    Ok((file_path, byte_range, timestamp))
}

fn generate_v2_fetch_url(hash: &MerkleHash, ranges: &[XorbRangeDescriptor], timestamp: Instant) -> String {
    xorb_utils::generate_v2_fetch_url(hash, ranges, timestamp)
}
#[cfg(test)]
mod tests {
    use xet_core_structures::xorb_object::CompressionScheme;
    use xet_core_structures::xorb_object::xorb_format_test_utils::{
        ChunkSize, build_and_verify_xorb_object, build_raw_xorb,
    };

    use super::*;
    use crate::cas_client::simulation::DeletionControlableClient;
    use crate::cas_client::simulation::client_testing_utils::ClientTestingUtils;
    use crate::cas_types::{ChunkRange, XorbReconstructionFetchInfo};

    /// Runs the common TestingClient trait test suite for LocalClient.
    #[tokio::test]
    async fn test_common_client_suite() {
        crate::cas_client::simulation::client_unit_testing::test_client_functionality(|| async {
            LocalClient::temporary().await.unwrap()
                as std::sync::Arc<dyn crate::cas_client::simulation::DirectAccessClient>
        })
        .await;
    }

    /// Two different path strings for the same directory (symlink) must share one `redb::Database`
    /// in `DB_CACHE`. Without `canonicalize` in `new_internal`, `redb` returns
    /// `Database already open. Cannot acquire lock.` (duplicate opens for the same file).
    /// That can also surface as EMFILE under parallel tests (extra FDs per duplicate handle).
    #[cfg(unix)]
    #[tokio::test]
    async fn db_cache_unifies_symlink_equivalent_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("real");
        std::fs::create_dir_all(&real).unwrap();
        let link = tmp.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let c1 = LocalClient::new(&link).await.unwrap();
        let c2 = LocalClient::new(&real).await.unwrap();
        assert!(Arc::ptr_eq(&c1.db, &c2.db));
    }

    #[tokio::test]
    async fn test_download_fetch_term_data_validation() {
        // Setup: Create a client and upload a xorb
        let xorb = build_raw_xorb(3, ChunkSize::Fixed(2048));
        let xorb_obj = build_and_verify_xorb_object(xorb, CompressionScheme::Auto);
        let hash = xorb_obj.hash;

        let client = LocalClient::temporary().await.unwrap();
        let permit = client.acquire_upload_permit().await.unwrap();
        client.upload_xorb("default", xorb_obj, None, permit).await.unwrap();

        // Get the actual byte offsets for a chunk range
        let file_path = client.get_path_for_entry(&hash);
        let file = File::open(&file_path).unwrap();
        let mut reader = BufReader::new(file);
        let xorb_obj = XorbObject::deserialize(&mut reader).unwrap();
        let (fetch_byte_start, fetch_byte_end) = xorb_obj.get_byte_offset(0, 1).unwrap();

        let timestamp = Instant::now();
        let byte_range = FileRange::new(fetch_byte_start as u64, fetch_byte_end as u64);
        let valid_url = generate_fetch_url(&file_path, &byte_range, timestamp);
        // HttpRange uses inclusive end, FileRange uses exclusive end
        let valid_url_range = HttpRange::from(byte_range);

        // Test 1: Valid URL and fetch_term should succeed
        let valid_fetch_term = XorbReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: valid_url.clone(),
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, valid_fetch_term).await;
        assert!(result.is_ok(), "Valid fetch_term should succeed");

        // Test 2: Invalid URL format - too few parts (3 instead of 4)
        let too_few_parts = "filename:123:456";
        let invalid_fetch_term = XorbReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: too_few_parts.to_string(),
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "URL with too few parts should fail");
        assert!(matches!(result.unwrap_err(), ClientError::InvalidArguments));

        // Test 3: Invalid start_pos - doesn't match url_range.start
        let wrong_byte_range = FileRange::new(fetch_byte_start as u64 + 1, fetch_byte_end as u64);
        let wrong_start_pos = generate_fetch_url(&file_path, &wrong_byte_range, timestamp);
        let invalid_fetch_term = XorbReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: wrong_start_pos,
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "Wrong start_pos should fail");
        assert!(matches!(result.unwrap_err(), ClientError::InvalidArguments));

        // Test 4: Invalid end_pos - doesn't match url_range.end
        let wrong_byte_range = FileRange::new(fetch_byte_start as u64, fetch_byte_end as u64 + 1);
        let wrong_end_pos = generate_fetch_url(&file_path, &wrong_byte_range, timestamp);
        let invalid_fetch_term = XorbReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: wrong_end_pos,
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "Wrong end_pos should fail");
        assert!(matches!(result.unwrap_err(), ClientError::InvalidArguments));

        // Test 5: Invalid start_pos - non-numeric
        let timestamp_ms = timestamp.saturating_duration_since(*REFERENCE_INSTANT).as_millis() as u64;
        let non_numeric_start = format!("{}:not_a_number:{}:{}", file_path.display(), fetch_byte_end, timestamp_ms);
        let invalid_fetch_term = XorbReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: non_numeric_start,
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "Non-numeric start_pos should fail");
        assert!(matches!(result.unwrap_err(), ClientError::InvalidArguments));

        // Test 6: Invalid end_pos - non-numeric
        let non_numeric_end = format!("{}:{}:not_a_number:{}", file_path.display(), fetch_byte_start, timestamp_ms);
        let invalid_fetch_term = XorbReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: non_numeric_end,
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "Non-numeric end_pos should fail");
        assert!(matches!(result.unwrap_err(), ClientError::InvalidArguments));

        // Test 7: Empty URL
        let invalid_fetch_term = XorbReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: String::new(),
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "Empty URL should fail");
        assert!(matches!(result.unwrap_err(), ClientError::InvalidArguments));

        // Test 8: Invalid timestamp - non-numeric
        let non_numeric_timestamp =
            format!("{}:{}:{}:not_a_number", file_path.display(), fetch_byte_start, fetch_byte_end);
        let invalid_fetch_term = XorbReconstructionFetchInfo {
            range: ChunkRange::new(0, 1),
            url: non_numeric_timestamp,
            url_range: valid_url_range,
        };
        let result = client.fetch_term_data(hash, invalid_fetch_term).await;
        assert!(result.is_err(), "Non-numeric timestamp should fail");
        assert!(matches!(result.unwrap_err(), ClientError::InvalidArguments));

        // Test 9: Non-existent file path
        let non_existent_path = PathBuf::from("/nonexistent/path/file.xorb");
        let non_existent_url = generate_fetch_url(&non_existent_path, &byte_range, timestamp);
        let invalid_fetch_term = XorbReconstructionFetchInfo {
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
            LocalClient::temporary().await.unwrap()
                as std::sync::Arc<dyn crate::cas_client::simulation::DirectAccessClient>
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_api_delay() {
        super::super::client_unit_testing::test_api_delay_functionality(|| async {
            LocalClient::temporary().await.unwrap()
                as std::sync::Arc<dyn crate::cas_client::simulation::DirectAccessClient>
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_global_dedup_shard_expiration() {
        super::super::client_unit_testing::test_global_dedup_shard_expiration_functionality(|| async {
            LocalClient::temporary().await.unwrap()
                as std::sync::Arc<dyn crate::cas_client::simulation::DirectAccessClient>
        })
        .await;
    }

    #[tokio::test]
    #[cfg_attr(feature = "smoke-test", ignore)]
    async fn test_global_dedup_shard_expiration_stress() {
        super::super::client_unit_testing::test_global_dedup_shard_expiration_stress(|| async {
            LocalClient::temporary().await.unwrap()
                as std::sync::Arc<dyn crate::cas_client::simulation::DirectAccessClient>
        })
        .await;
    }

    #[tokio::test]
    async fn test_deletion_suite() {
        super::super::deletion_unit_testing::test_deletion_functionality(|| async {
            LocalClient::temporary().await.unwrap()
        })
        .await;
    }

    #[tokio::test]
    async fn test_verify_integrity_detects_missing_cas_block_reference() {
        let client = LocalClient::temporary().await.unwrap();
        client.upload_random_file(&[(3, (0, 3)), (4, (0, 2))], 2048).await.unwrap();
        client.verify_integrity().await.unwrap();

        let mut in_memory = client.load_all_shard_data().unwrap();
        let removed_hash = *in_memory.xorb_content.keys().next().unwrap();
        in_memory.xorb_content.remove(&removed_hash);
        client.delete_xorb(&removed_hash).await;
        client.write_shard_data_and_register(&in_memory).await.unwrap();

        assert!(client.verify_integrity().await.is_err());
    }

    #[tokio::test]
    async fn test_verify_integrity_detects_invalid_chunk_range() {
        let client = LocalClient::temporary().await.unwrap();
        client.upload_random_file(&[(5, (0, 3))], 2048).await.unwrap();
        client.verify_integrity().await.unwrap();

        let mut in_memory = client.load_all_shard_data().unwrap();
        let file_info = in_memory.file_content.values_mut().next().unwrap();
        let segment = file_info.segments.first_mut().unwrap();
        let xorb_entry_count = in_memory.xorb_content.get(&segment.xorb_hash).unwrap().metadata.num_entries;
        segment.chunk_index_end = xorb_entry_count + 1;
        client.write_shard_data_and_register(&in_memory).await.unwrap();

        assert!(client.verify_integrity().await.is_err());
    }

    /// Verifies that delete_file_entry does not rewrite shard files (shard hashes remain stable).
    #[tokio::test]
    async fn test_delete_file_entry_does_not_rewrite_shards() {
        let client = LocalClient::temporary().await.unwrap();
        client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();

        let shard_hashes_before: Vec<_> = client.shard_file_paths().unwrap().into_iter().map(|(h, _)| h).collect();
        assert!(!shard_hashes_before.is_empty());

        client
            .delete_file_entry(&client.list_file_shard_entries().await.unwrap()[0].0)
            .await
            .unwrap();

        let shard_hashes_after: Vec<_> = client.shard_file_paths().unwrap().into_iter().map(|(h, _)| h).collect();
        assert_eq!(shard_hashes_before, shard_hashes_after, "Shard file hashes must not change after delete");
    }

    /// Verifies that file deletion (removal from FILE_TO_SHARD_TABLE) persists across restarts.
    #[tokio::test]
    async fn test_deletion_status_persists_across_restart() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().to_owned();

        let file_hash;
        {
            let client = LocalClient::new(&path).await.unwrap();
            let file = client.upload_random_file(&[(1, (0, 3)), (2, (0, 2))], 2048).await.unwrap();
            file_hash = file.file_hash;
            assert!(!client.list_file_shard_entries().await.unwrap().is_empty());

            client.delete_file_entry(&file_hash).await.unwrap();
            assert!(client.list_file_shard_entries().await.unwrap().is_empty());
        }

        {
            let client = LocalClient::new(&path).await.unwrap();
            assert!(
                client.is_file_deleted(&file_hash),
                "Entry should be absent from FILE_TO_SHARD_TABLE after restart"
            );
            assert!(
                client.list_file_shard_entries().await.unwrap().is_empty(),
                "Deleted files should remain hidden after restart"
            );
        }
    }

    /// Tests cross-shard dedup integrity: a file entry referencing an XORB that is not indexed
    /// in any shard but exists on disk should pass verify_integrity (dedup case).
    #[tokio::test]
    async fn test_verify_integrity_cross_shard_dedup_ok() {
        let client = LocalClient::temporary().await.unwrap();
        client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
        client.verify_integrity().await.unwrap();

        // Clear dedup entries for old shards before rewriting, so pass 3 doesn't
        // flag stale references to the about-to-be-replaced shard files.
        for h in client.list_shard_entries().await.unwrap() {
            client.remove_shard_dedup_entries(&h).await.unwrap();
        }

        let mut in_memory = client.load_all_shard_data().unwrap();
        in_memory.xorb_content.clear();
        client.write_shard_data_and_register(&in_memory).await.unwrap();

        client
            .verify_integrity()
            .await
            .expect("Integrity should pass: XORB files exist on disk even though no shard indexes them");
    }

    /// Tests that verify_integrity ignores deleted files (absent from FILE_TO_SHARD_TABLE),
    /// so missing XORBs for deleted files do not cause false integrity failures.
    #[tokio::test]
    async fn test_verify_integrity_skips_deleted_files() {
        let client = LocalClient::temporary().await.unwrap();
        let deleted_file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
        let live_file = client.upload_random_file(&[(2, (0, 2))], 2048).await.unwrap();
        client.verify_integrity().await.unwrap();

        client.delete_file_entry(&deleted_file.file_hash).await.unwrap();

        for t in &deleted_file.terms {
            client.delete_xorb(&t.xorb_hash).await;
        }

        // Clear dedup entries for old shards before rewriting, so pass 3 doesn't
        // flag stale references to the about-to-be-replaced shard files.
        for h in client.list_shard_entries().await.unwrap() {
            client.remove_shard_dedup_entries(&h).await.unwrap();
        }

        // Remove deleted-file entries from shard metadata too, so pass 1 doesn't fail
        // on deliberately removed XORB files and the deleted file isn't re-registered.
        let mut in_memory = client.load_all_shard_data().unwrap();
        in_memory.file_content.remove(&deleted_file.file_hash);
        for t in &deleted_file.terms {
            in_memory.xorb_content.remove(&t.xorb_hash);
        }
        client.write_shard_data_and_register(&in_memory).await.unwrap();

        client
            .verify_integrity()
            .await
            .expect("Integrity should pass: missing XORBs are only referenced by a deleted file");

        // Sanity check: the surviving file remains readable.
        let live_data = client.get_file_data(&live_file.file_hash, None).await.unwrap();
        assert_eq!(live_data, live_file.data);
    }

    /// Tests that verify_integrity catches stale global dedup table entries pointing
    /// to shard files that have been removed.
    #[tokio::test]
    async fn test_verify_integrity_detects_stale_dedup_shard_reference() {
        let client = LocalClient::temporary().await.unwrap();
        let file = client.upload_random_file(&[(10, (0, 3))], 2048).await.unwrap();
        client.verify_integrity().await.unwrap();

        // Confirm dedup entries exist for the file's chunks.
        let has_dedup = client
            .query_for_global_dedup_shard("default", &file.terms[0].chunk_hashes[0])
            .await
            .unwrap()
            .is_some();
        assert!(has_dedup, "Dedup entry should exist after upload");

        // Remove file entry so Pass 2 doesn't fail on the missing shard.
        client.delete_file_entry(&file.file_hash).await.unwrap();

        // Delete the shard file without clearing its dedup entries.
        let shard_hashes = client.list_shard_entries().await.unwrap();
        assert!(!shard_hashes.is_empty());
        for h in &shard_hashes {
            let path = client.shard_path_for_hash(h).unwrap();
            std::fs::remove_file(&path).unwrap();
        }

        let result = client.verify_integrity().await;
        assert!(result.is_err(), "verify_integrity should fail when dedup table references a missing shard");
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("global dedup table"), "Error should mention global dedup table");
    }

    /// Exercises the root-cause scenario: re-uploading the same file hash in a new shard
    /// after deleting the original file and its xorbs must not resurrect stale entries.
    #[tokio::test]
    async fn test_reupload_same_file_hash_does_not_resurrect_stale_entries() {
        let client = LocalClient::temporary().await.unwrap();

        // 1. Upload file F in shard S1 referencing xorb X.
        let file = client.upload_random_file(&[(1, (0, 3))], 2048).await.unwrap();
        let file_hash = file.file_hash;
        let xorb_hash = file.terms[0].xorb_hash;
        client.verify_integrity().await.unwrap();

        // 2. Delete file F (removes from FILE_TO_SHARD_TABLE).
        client.delete_file_entry(&file_hash).await.unwrap();
        assert!(client.is_file_deleted(&file_hash));

        // 3. Delete xorb X from disk.
        client.delete_xorb(&xorb_hash).await;
        assert!(!client.get_path_for_entry(&xorb_hash).exists());

        // 4. Clean up old shard's xorb metadata so Pass 1 doesn't fail on the deliberately removed xorb file.  Also
        //    clean up dedup entries.
        for h in client.list_shard_entries().await.unwrap() {
            client.remove_shard_dedup_entries(&h).await.unwrap();
        }
        let mut in_memory = client.load_all_shard_data().unwrap();
        in_memory.file_content.remove(&file_hash);
        for t in &file.terms {
            in_memory.xorb_content.remove(&t.xorb_hash);
        }
        client.write_shard_data_and_register(&in_memory).await.unwrap();

        // 5. Upload a new, different file in a new shard S2.
        let file2 = client.upload_random_file(&[(2, (0, 2))], 2048).await.unwrap();
        let file2_hash = file2.file_hash;
        assert!(!client.is_file_deleted(&file2_hash));

        // 6. verify_integrity should pass — S1's stale file entry for F is not checked because FILE_TO_SHARD_TABLE no
        //    longer maps F to S1.
        client
            .verify_integrity()
            .await
            .expect("Integrity should pass: the old shard's stale file entry with dangling xorb refs is not consulted");
    }

    /// Tests that list_xorbs_and_tags tags change after file re-creation with a timestamp delay.
    #[tokio::test]
    async fn test_list_xorbs_and_tags_timestamp_changes() {
        let client = LocalClient::temporary().await.unwrap();

        let file1 = client.upload_random_file(&[(1, (0, 2))], 2048).await.unwrap();
        let xorb_hash = file1.terms[0].xorb_hash;

        let tags1 = client.list_xorbs_and_tags().await.unwrap();
        let (_, tag1) = tags1.iter().find(|(h, _)| *h == xorb_hash).unwrap();

        // Delete and wait 1 second so the filesystem timestamp advances.
        client.delete_xorb(&xorb_hash).await;
        std::thread::sleep(Duration::from_secs(1));

        // Re-upload a file that creates a new xorb with the same hash seed.
        let file2 = client.upload_random_file(&[(1, (0, 2))], 2048).await.unwrap();
        let xorb_hash2 = file2.terms[0].xorb_hash;

        let tags2 = client.list_xorbs_and_tags().await.unwrap();
        let (_, tag2) = tags2.iter().find(|(h, _)| *h == xorb_hash2).unwrap();

        assert_ne!(tag1, tag2, "Tags should differ after re-creation with timestamp delay");
    }
}
