use std::collections::HashMap;
use std::fs::{DirEntry, File};
use std::io::{self, Cursor, ErrorKind, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use base64::engine::general_purpose::URL_SAFE;
use base64::engine::GeneralPurpose;
use base64::Engine;
use cas_types::{ChunkRange, Key};
use error_printer::ErrorPrinter;
use file_utils::SafeFileCreator;
use merklehash::MerkleHash;
use tokio::sync::RwLock;
use tracing::{debug, error};
use utils::output_bytes;

use crate::disk::cache_file_header::CacheFileHeader;
use crate::disk::cache_item::{CacheItem, VerificationCell};
use crate::error::ChunkCacheError;
use crate::{CacheConfig, CacheRange, ChunkCache};

mod cache_file_header;
mod cache_item;
pub mod test_utils;

// consistently use URL_SAFE (also file path safe) base64 codec
pub(crate) const BASE64_ENGINE: GeneralPurpose = URL_SAFE;
pub const DEFAULT_CHUNK_CACHE_CAPACITY: u64 = 10 << 30; // 10 GB
const PREFIX_DIR_NAME_LEN: usize = 2;

type OptionResult<T, E> = Result<Option<T>, E>;

#[derive(Debug, Clone)]
struct CacheState {
    inner: HashMap<Key, Vec<VerificationCell<CacheItem>>>,
    num_items: usize,
    total_bytes: u64,
}

impl CacheState {
    fn new(state: HashMap<Key, Vec<VerificationCell<CacheItem>>>, num_items: usize, total_bytes: u64) -> Self {
        Self {
            inner: state,
            num_items,
            total_bytes,
        }
    }

    fn find_match(&self, key: &Key, range: &ChunkRange) -> Option<VerificationCell<CacheItem>> {
        let items = self.inner.get(key)?;

        // attempt to find a matching range in the given key's items using
        for item in items.iter() {
            if item.range.start <= range.start && range.end <= item.range.end {
                return Some(item.clone());
            }
        }
        None
    }

    /// removed items from the cache (including deleting from file system)
    /// until at least to_remove number of bytes have been removed
    ///
    /// removes data from in memory state and returns a list of file paths to delete
    /// (so that deletion can occur after the locked state is dropped)
    fn evict_to_capacity(
        &mut self,
        max_total_bytes: u64,
    ) -> Result<Vec<(Key, VerificationCell<CacheItem>)>, ChunkCacheError> {
        let original_total_bytes = self.total_bytes;
        let mut ret = Vec::new();

        while self.total_bytes > max_total_bytes {
            let Some((key, idx)) = self.random_item() else {
                error!("attempted to evict item, but no item could be found to be evicted");
                break;
            };
            let items = self.inner.get_mut(&key).ok_or(ChunkCacheError::Infallible)?;
            let cache_item = items.swap_remove(idx);
            let len = cache_item.len;

            if items.is_empty() {
                self.inner.remove(&key);
            }

            ret.push((key, cache_item));

            self.total_bytes -= len;
            self.num_items -= 1;
        }
        debug!(
            "cache evicting {} items totaling {}",
            ret.len(),
            output_bytes(original_total_bytes - self.total_bytes)
        );

        Ok(ret)
    }

    /// returns the key and index within that key for a random item
    fn random_item(&self) -> Option<(Key, usize)> {
        debug_assert_eq!(
            self.inner.values().map(|v| v.len()).sum::<usize>(),
            self.num_items,
            "real num items != stored num items"
        );

        if self.num_items == 0 {
            error!("cache random_item for eviction: no items in cache");
            return None;
        }
        let random_item = rand::random::<u32>() as usize % self.num_items;
        let mut count = 0;
        for (key, items) in self.inner.iter() {
            if random_item < count + items.len() {
                return Some((key.clone(), random_item - count));
            }
            count += items.len();
        }
        // should never occur
        error!("cache random_item for eviction: tried to return random item error not enough items");
        None
    }
}

/// DiskCache is a ChunkCache implementor that saves data on the file system
#[derive(Debug, Clone)]
pub struct DiskCache {
    cache_root: PathBuf,
    capacity: u64,
    state: Arc<RwLock<CacheState>>,
}

// helper for analysis binary to print inner state
#[cfg(feature = "analysis")]
impl DiskCache {
    pub async fn print(&self) {
        let state = self.state.read().await;
        let total_num_items = state.num_items;
        let total_total_bytes = state.total_bytes;

        println!(
            "total items: {}, total bytes {} for the whole cache",
            total_num_items,
            output_bytes(total_total_bytes)
        );

        for (key, items) in state.inner.iter() {
            println!();
            let num_items = items.len();
            let total_bytes: usize = items.iter().map(|item| item.len).fold(0usize, |acc, len| acc + len as usize);
            println!("key: {key}");
            println!("\ttotal items: {}, total bytes {} for key {key}", num_items, output_bytes(total_bytes as u64));
            println!();
            for item in items.iter() {
                println!(
                    "\titem: chunk range [{}-{}) ; len({}); checksum({})",
                    item.range.start,
                    item.range.end,
                    output_bytes(item.len),
                    item.checksum,
                );
            }
        }
    }
}

impl DiskCache {
    pub async fn num_items(&self) -> usize {
        self.state.read().await.num_items
    }

    pub async fn total_bytes(&self) -> u64 {
        self.state.read().await.total_bytes
    }

    /// initialize will create a new DiskCache with the capacity and cache root based on the config
    /// the cache file system layout is rooted at the provided config.cache_directory and initialize
    /// will attempt to load any pre-existing cache state into memory.
    ///
    /// an configured size of 0 caused initialization to fail
    ///
    /// The cache layout is as follows:
    ///
    /// each key (cas hash) in the cache is a directory, containing "cache items" that each provide
    /// some range of data.
    ///
    /// keys are grouped into subdirectories under the cache rootbased on the first 2 chacters of their
    /// file name, which is base64 encoded, leading to at most 64 * 64 directories under the cache root.
    ///
    /// cache_root/
    /// ├── [ab]/
    /// │   ├── [key 1 (ab123...)]/
    /// │   │   ├── [range 0-100, file_len, file_hash]
    /// │   │   ├── [range 102-300, file_len, file_hash]
    /// │   │   └── [range 900-1024, file_len, file_hash]
    /// │   ├── [key 2 (ab456...)]/
    /// │       └── [range 0-1020, file_len, file_hash]
    /// ├── [cd]/
    /// │   └── [key 3 (cd123...)]/
    /// │       ├── [range 30-31, file_len, file_hash]
    /// │       ├── [range 400-402, file_len, file_hash]
    /// │       ├── [range 404-405, file_len, file_hash]
    /// │       └── [range 679-700, file_len, file_hash]
    pub fn initialize(config: &CacheConfig) -> Result<Self, ChunkCacheError> {
        if config.cache_size == 0 {
            return Err(ChunkCacheError::InvalidArguments);
        }
        let capacity = config.cache_size;
        let cache_root = config.cache_directory.clone();

        // May take a while; don't block the runtime for this.
        let state = Self::initialize_state(&cache_root, capacity)?;

        Ok(Self {
            state: Arc::new(RwLock::new(state)),
            cache_root: config.cache_directory.clone(),
            capacity,
        })
    }

    fn initialize_state(cache_root: &PathBuf, capacity: u64) -> Result<CacheState, ChunkCacheError> {
        let mut state = HashMap::new();
        let mut total_bytes = 0;
        let mut num_items = 0;
        let max_num_bytes = 2 * capacity;

        let Some(cache_root_readdir) = read_dir(cache_root)? else {
            return Ok(CacheState::new(state, 0, 0));
        };

        // loop through cache root directory, first level containing "prefix" directories
        // each of which may contain key directories with cache items
        for key_prefix_dir in cache_root_readdir {
            let Some(key_prefix_dir) = is_ok_dir(key_prefix_dir)? else {
                continue;
            };

            let key_prefix_dir_name = key_prefix_dir.file_name();
            if key_prefix_dir_name.as_encoded_bytes().len() != PREFIX_DIR_NAME_LEN {
                debug!("prefix dir name len != {PREFIX_DIR_NAME_LEN}");
                continue;
            }

            let Some(key_prefix_readdir) = read_dir(key_prefix_dir.path())? else {
                continue;
            };

            // loop through key directories inside prefix directory
            for key_dir in key_prefix_readdir {
                let key_dir = match is_ok_dir(key_dir) {
                    Ok(Some(dirent)) => dirent,
                    Ok(None) => continue,
                    Err(e) => return Err(e),
                };

                let key_dir_name = key_dir.file_name();

                // asserts that the prefix dir name is actually the prefix of this key dir
                debug_assert_eq!(
                    key_dir_name.as_encoded_bytes()[..PREFIX_DIR_NAME_LEN].to_ascii_uppercase(),
                    key_prefix_dir_name.as_encoded_bytes().to_ascii_uppercase(),
                    "{key_dir_name:?}",
                );

                let key = match try_parse_key(key_dir_name.as_encoded_bytes()) {
                    Ok(key) => key,
                    Err(e) => {
                        debug!("failed to decoded a directory name as a key: {e}");
                        continue;
                    },
                };

                let mut items = Vec::new();

                let key_readdir = match read_dir(key_dir.path()) {
                    Ok(Some(krd)) => krd,
                    Ok(None) => continue,
                    Err(e) => return Err(e),
                };

                // loop through cache items inside key directory
                for item in key_readdir {
                    let cache_item = match try_parse_cache_file(item, capacity) {
                        Ok(Some(ci)) => ci,
                        Ok(None) => continue,
                        Err(e) => return Err(e),
                    };

                    total_bytes += cache_item.len;
                    num_items += 1;
                    items.push(VerificationCell::new_unverified(cache_item));

                    // if already filled capacity, stop iterating over cache items
                    if total_bytes >= max_num_bytes {
                        state.insert(key, items);
                        return Ok(CacheState::new(state, num_items, total_bytes));
                    }
                }

                if !items.is_empty() {
                    state.insert(key, items);
                }
            }
        }

        Ok(CacheState::new(state, num_items, total_bytes))
    }

    async fn get_impl(&self, key: &Key, range: &ChunkRange) -> OptionResult<CacheRange, ChunkCacheError> {
        if range.start >= range.end {
            return Err(ChunkCacheError::InvalidArguments);
        }

        loop {
            let Some(cache_item) = self.state.read().await.find_match(key, range) else {
                return Ok(None);
            };

            let path = self.item_path(key, &cache_item)?;

            let mut file = match File::open(&path) {
                Ok(file) => file,
                Err(e) => match e.kind() {
                    ErrorKind::NotFound => {
                        self.remove_item(key, &cache_item).await?;
                        continue;
                    },
                    _ => return Err(e.into()),
                },
            };

            if !cache_item.is_verified() {
                let checksum = crc32_from_reader(&mut file)?;
                if checksum == cache_item.checksum {
                    cache_item.verify();
                    file.rewind()?;
                } else {
                    debug!("computed checksum {checksum} mismatch on cache item {key}/{cache_item}");
                    self.remove_item(key, &cache_item).await?;
                    continue;
                }
            }

            let mut file_reader = std::io::BufReader::new(file);

            let Ok(header) = CacheFileHeader::deserialize(&mut file_reader)
                .debug_error(format!("failed to deserialize cache file header on path: {path:?}"))
            else {
                self.remove_item(key, &cache_item).await?;
                continue;
            };

            let start = cache_item.range.start;
            let result_buf = get_range_from_cache_file(&header, &mut file_reader, range, start)?;
            return Ok(Some(result_buf));
        }
    }

    async fn put_impl(
        &self,
        key: &Key,
        range: &ChunkRange,
        chunk_byte_indices: &[u32],
        data: &[u8],
    ) -> Result<(), ChunkCacheError> {
        if range.start >= range.end
            || chunk_byte_indices.len() != (range.end - range.start + 1) as usize
            // chunk_byte_indices is guaranteed to be more than 1 element at this point
            || chunk_byte_indices[0] != 0
            || *chunk_byte_indices.last().unwrap() as usize != data.len()
            || !strictly_increasing(chunk_byte_indices)
        {
            return Err(ChunkCacheError::InvalidArguments);
        }

        // check if we already contain the range
        while let Some(cache_item) = self.state.read().await.find_match(key, range) {
            if self.validate_match(key, range, chunk_byte_indices, data, &cache_item).await? {
                return Ok(());
            }
        }

        let header = CacheFileHeader::new(chunk_byte_indices);
        let mut header_buf = Vec::with_capacity(header.header_len());
        header.serialize(&mut header_buf)?;
        let len = (header_buf.len() + data.len()) as u64;
        if len > self.capacity {
            // refusing to add this item as it is too large for the cache with configured capacity
            return Ok(());
        }

        let checksum = {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&header_buf);
            hasher.update(data);
            hasher.finalize()
        };

        let cache_item = CacheItem {
            range: *range,
            len,
            checksum,
        };

        // write cache item file
        let path = self.item_path(key, &cache_item)?;
        let mut fw = SafeFileCreator::new(path)?;
        fw.write_all(&header_buf)?;
        fw.write_all(data)?;

        // evict items after ensuring the file write but before committing to cache state
        // to avoid removing new item.
        let mut state_write = self.state.write().await;

        // acquiring lock to state before closing the file
        // this will ensure that this thread is the only one writing to the final
        // cache file but allowing other threads to modify the state while we write the file
        // before committing it.
        if state_write.find_match(key, range).is_some() {
            // another thread already added this item or overlapping item while this thread
            // was writing the file
            fw.abort()?;
            return Ok(());
        }
        fw.close()?;

        // Evict entries to make sure we have enough room.
        let evicted_paths = state_write.evict_to_capacity(self.capacity - cache_item.len)?;

        // add the item info in-memory state after evictions are done
        state_write.num_items += 1;
        state_write.total_bytes += cache_item.len;
        let item_set = state_write.inner.entry(key.clone()).or_default();
        item_set.push(VerificationCell::new_verified(cache_item));

        // release lock
        drop(state_write);

        // remove files after done with modifying in memory state and releasing lock
        for (key, cache_item) in evicted_paths {
            let path = self.item_path(&key, &cache_item)?;
            remove_file(&path)?;
            // check and try to remove key path if all items evicted for key
            let dir_path = path.parent().ok_or(ChunkCacheError::Infallible)?;
            check_remove_dir(dir_path)?;
        }

        Ok(())
    }

    // on a non-error case, returns true if the item is a good match and a new item should not be inserted
    // returns false if not a good match and should be removed.
    async fn validate_match(
        &self,
        key: &Key,
        range: &ChunkRange,
        chunk_byte_indices: &[u32],
        data: &[u8],
        cache_item: &VerificationCell<CacheItem>,
    ) -> Result<bool, ChunkCacheError> {
        // this is a redundant check
        if range.start < cache_item.range.start || range.end > cache_item.range.end {
            return Err(ChunkCacheError::BadRange);
        }

        // validate stored data
        let path = self.item_path(key, cache_item)?;

        let Ok(mut file) = File::open(path) else {
            self.remove_item(key, cache_item).await?;
            return Ok(false);
        };
        let md = file.metadata()?;
        if md.len() != cache_item.len {
            self.remove_item(key, cache_item).await?;
            return Ok(false);
        }
        let mut buf = Vec::with_capacity(md.len() as usize);
        file.read_to_end(&mut buf)?;
        let checksum = crc32fast::hash(&buf);
        if checksum != cache_item.checksum {
            self.remove_item(key, cache_item).await?;
            return Ok(false);
        }
        let mut reader = Cursor::new(buf);
        let Ok(header) = CacheFileHeader::deserialize(&mut reader) else {
            self.remove_item(key, cache_item).await?;
            return Ok(false);
        };

        // validate the chunk_byte_indices and data input against stored data
        // the chunk_byte_indices should match the chunk lengths, if the ranges
        // don't start at the same chunk, values will be different, what's important
        // to match is the chunk lengths, i.e. difference in the offsets.
        let idx_start = (range.start - cache_item.range.start) as usize;
        let idx_end = (range.end - cache_item.range.start + 1) as usize;
        for i in idx_start..idx_end - 1 {
            let stored_diff = header.chunk_byte_indices[i + 1] - header.chunk_byte_indices[i];
            let given_diff = chunk_byte_indices[i + 1 - idx_start] - chunk_byte_indices[i - idx_start];
            if stored_diff != given_diff {
                debug!(
                    "failed to match chunk lens for these chunk offsets {} {:?}\n{} {:?}",
                    cache_item.range,
                    &header.chunk_byte_indices[idx_start..idx_end],
                    range,
                    chunk_byte_indices
                );
                return Err(ChunkCacheError::InvalidArguments);
            }
        }

        let stored = get_range_from_cache_file(&header, &mut reader, range, cache_item.range.start)?;
        if data != stored.data {
            return Err(ChunkCacheError::InvalidArguments);
        }
        Ok(true)
    }

    /// removes an item from both the in-memory state of the cache and the file system
    async fn remove_item(&self, key: &Key, cache_item: &VerificationCell<CacheItem>) -> Result<(), ChunkCacheError> {
        {
            let mut state = self.state.write().await;
            if let Some(items) = state.inner.get_mut(key) {
                let idx = match index_of(items, cache_item) {
                    Some(idx) => idx,
                    // item is no longer in the state
                    None => return Ok(()),
                };

                items.swap_remove(idx);
                if items.is_empty() {
                    state.inner.remove(key);
                }
                state.total_bytes -= cache_item.len;
                state.num_items -= 1;
            }
        }

        let path = self.item_path(key, cache_item)?;

        if !path.exists() {
            return Ok(());
        }
        remove_file(&path)?;
        let dir_path = path.parent().ok_or(ChunkCacheError::Infallible)?;
        check_remove_dir(dir_path)
    }

    fn item_path(&self, key: &Key, cache_item: &CacheItem) -> Result<PathBuf, ChunkCacheError> {
        Ok(self.cache_root.join(key_dir(key)).join(cache_item.file_name()?))
    }
}

fn crc32_from_reader(reader: &mut impl Read) -> Result<u32, ChunkCacheError> {
    const CRC_BUFFER_SIZE: usize = 4096;
    let mut buf = [0u8; CRC_BUFFER_SIZE];
    let mut hasher = crc32fast::Hasher::new();
    loop {
        let num_read = reader.read(&mut buf)?;
        if num_read == 0 {
            break;
        }
        hasher.update(&buf[..num_read])
    }
    Ok(hasher.finalize())
}

#[inline]
fn index_of<T: PartialEq>(list: &[T], value: &T) -> Option<usize> {
    for (i, list_value) in list.iter().enumerate() {
        if list_value == value {
            return Some(i);
        }
    }
    None
}

fn strictly_increasing(chunk_byte_indices: &[u32]) -> bool {
    for i in 1..chunk_byte_indices.len() {
        if chunk_byte_indices[i - 1] >= chunk_byte_indices[i] {
            return false;
        }
    }
    true
}

fn get_range_from_cache_file<R: Read + Seek>(
    header: &CacheFileHeader,
    file_contents: &mut R,
    range: &ChunkRange,
    start: u32,
) -> Result<CacheRange, ChunkCacheError> {
    let start_idx = (range.start - start) as usize;
    let end_idx = (range.end - start) as usize;
    let start_byte = header.chunk_byte_indices.get(start_idx).ok_or(ChunkCacheError::BadRange)?;
    let end_byte = header.chunk_byte_indices.get(end_idx).ok_or(ChunkCacheError::BadRange)?;
    file_contents.seek(SeekFrom::Start((*start_byte as usize + header.header_len()) as u64))?;
    let mut data = vec![0; (end_byte - start_byte) as usize];
    file_contents.read_exact(&mut data)?;
    let offsets: Vec<u32> = header.chunk_byte_indices[start_idx..=end_idx]
        .iter()
        .map(|v| *v - header.chunk_byte_indices[start_idx])
        .collect();

    debug_assert_eq!(range.end - range.start, offsets.len() as u32 - 1);

    Ok(CacheRange {
        offsets,
        data,
        range: *range,
    })
}

// wrapper over std::fs::read_dir
// returns Ok(None) on a not found error
fn read_dir(path: impl AsRef<Path>) -> OptionResult<std::fs::ReadDir, ChunkCacheError> {
    match std::fs::read_dir(path) {
        Ok(rd) => Ok(Some(rd)),
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                Ok(None)
            } else {
                Err(e.into())
            }
        },
    }
}

// returns Ok(Some(_)) if result dirent is a directory, Ok(None) if was removed
// also returns an Ok(None) if the dirent is not a directory, in which case we should
//   not remove it in case the user put something inadvertantly or intentionally,
//   but not attempt to parse it as a valid cache directory.
// Err(_) if an unrecoverable error occurred
fn is_ok_dir(dir_result: Result<DirEntry, io::Error>) -> OptionResult<DirEntry, ChunkCacheError> {
    let dirent = match dir_result {
        Ok(kd) => kd,
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(e.into());
        },
    };
    let md = match dirent.metadata() {
        Ok(md) => md,
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(e.into());
        },
    };
    if !md.is_dir() {
        debug!("CACHE: expected directory at {:?}, is not directory", dirent.path());
        return Ok(None);
    }
    Ok(Some(dirent))
}

// given a result from readdir attempts to parse it as a cache file handle
// i.e. validate its file name against the contents (excluding file-hash-validation)
// validate that it is a file, correct len, and is not too large.
fn try_parse_cache_file(file_result: io::Result<DirEntry>, capacity: u64) -> OptionResult<CacheItem, ChunkCacheError> {
    let item = match file_result {
        Ok(item) => item,
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(e.into());
        },
    };
    let md = match item.metadata() {
        Ok(md) => md,
        Err(e) => {
            if e.kind() == ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(e.into());
        },
    };

    if !md.is_file() {
        return Ok(None);
    }
    if md.len() > DEFAULT_CHUNK_CACHE_CAPACITY {
        return Err(ChunkCacheError::general(format!(
            "Cache directory contains a file larger than {} GB, cache directory state is invalid",
            (DEFAULT_CHUNK_CACHE_CAPACITY as f64 / (1 << 30) as f64)
        )));
    }

    // don't track an item that takes up the whole capacity
    if md.len() > capacity {
        return Ok(None);
    }

    let cache_item = match CacheItem::parse(item.file_name().as_encoded_bytes())
        .debug_error("failed to decode a file name as a cache item")
    {
        Ok(i) => i,
        Err(e) => {
            debug!("not a valid cache file, removing: {:?} {e:?}", item.file_name());
            remove_file(item.path())?;
            return Ok(None);
        },
    };
    if md.len() != cache_item.len {
        // file is invalid, remove it
        debug!(
            "cache file len {} does not match expected length {}, removing path: {:?}",
            md.len(),
            cache_item.len,
            item.path()
        );
        remove_file(item.path())?;
        return Ok(None);
    }
    Ok(Some(cache_item))
}

/// removes a file but disregards a "NotFound" error if the file is already gone
fn remove_file(path: impl AsRef<Path>) -> Result<(), ChunkCacheError> {
    if let Err(e) = std::fs::remove_file(path) {
        if e.kind() != ErrorKind::NotFound {
            return Err(e.into());
        }
    }
    Ok(())
}

/// removes a directory but disregards a "NotFound" error if the directory is already gone
fn remove_dir(path: impl AsRef<Path>) -> Result<(), ChunkCacheError> {
    if let Err(e) = std::fs::remove_dir(path) {
        if e.kind() != ErrorKind::NotFound {
            return Err(e.into());
        }
    }
    Ok(())
}

// assumes dir_path is a path to a key directory i.e. cache_root/<prefix_dir>/<key_dir>
// assumes a misformatted path is an error
// checks if the directory is empty and removes it if so, then checks if the prefix dir is empty and removes it if so
fn check_remove_dir(dir_path: impl AsRef<Path>) -> Result<(), ChunkCacheError> {
    let readdir = match read_dir(&dir_path)? {
        Some(rd) => rd,
        None => return Ok(()),
    };
    if readdir.peekable().peek().is_some() {
        return Ok(());
    }
    // directory empty, remove it
    remove_dir(&dir_path)?;

    // try to check and remove the prefix dir
    let prefix_dir = dir_path.as_ref().parent().ok_or(ChunkCacheError::Infallible)?;

    let prefix_readdir = match read_dir(prefix_dir)? {
        Some(prd) => prd,
        None => return Ok(()),
    };
    if prefix_readdir.peekable().peek().is_some() {
        return Ok(());
    }
    // directory empty, remove it
    remove_dir(prefix_dir)
}

/// tries to parse just a Key from a file name encoded by fn `key_dir`
/// expects only the key portion of the file path, with the prefix not present.
fn try_parse_key(file_name: &[u8]) -> Result<Key, ChunkCacheError> {
    let buf = BASE64_ENGINE.decode(file_name)?;
    let hash = MerkleHash::from_slice(&buf[..size_of::<MerkleHash>()])?;
    let prefix = String::from(std::str::from_utf8(&buf[size_of::<MerkleHash>()..])?);
    Ok(Key { prefix, hash })
}

/// key_dir returns a directory name string formed from the key
/// the format is BASE64_encode([ key.hash[..], key.prefix.as_bytes()[..] ])
fn key_dir(key: &Key) -> PathBuf {
    let prefix_bytes = key.prefix.as_bytes();
    let mut buf = vec![0u8; size_of::<MerkleHash>() + prefix_bytes.len()];
    buf[..size_of::<MerkleHash>()].copy_from_slice(key.hash.as_bytes());
    buf[size_of::<MerkleHash>()..].copy_from_slice(prefix_bytes);
    let encoded = BASE64_ENGINE.encode(&buf);
    let prefix_dir = &encoded[..PREFIX_DIR_NAME_LEN];
    let dir_str = format!("{prefix_dir}/{encoded}");
    PathBuf::from(dir_str)
}

#[async_trait]
impl ChunkCache for DiskCache {
    async fn get(&self, key: &Key, range: &ChunkRange) -> Result<Option<CacheRange>, ChunkCacheError> {
        self.get_impl(key, range).await
    }

    async fn put(
        &self,
        key: &Key,
        range: &ChunkRange,
        chunk_byte_indices: &[u32],
        data: &[u8],
    ) -> Result<(), ChunkCacheError> {
        self.put_impl(key, range, chunk_byte_indices, data).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use cas_types::{ChunkRange, Key};
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use tempdir::TempDir;
    use utils::output_bytes;

    use super::{DiskCache, DEFAULT_CHUNK_CACHE_CAPACITY};
    use crate::disk::test_utils::*;
    use crate::disk::try_parse_key;
    use crate::{CacheConfig, ChunkCache};

    const RANDOM_SEED: u64 = 9089 << 20 | 120043;

    #[tokio::test]
    async fn test_get_cache_empty() {
        let mut rng = StdRng::seed_from_u64(RANDOM_SEED);
        let cache_root = TempDir::new("empty").unwrap();
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: DEFAULT_CHUNK_CACHE_CAPACITY,
            ..Default::default()
        };
        let cache = DiskCache::initialize(&config).unwrap();
        assert!(cache
            .get(&random_key(&mut rng), &random_range(&mut rng))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_put_get_simple() {
        let mut rng = StdRng::seed_from_u64(RANDOM_SEED);
        let cache_root = TempDir::new("put_get_simple").unwrap();
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: DEFAULT_CHUNK_CACHE_CAPACITY,
            ..Default::default()
        };
        let cache = DiskCache::initialize(&config).unwrap();

        let key = random_key(&mut rng);
        let range = ChunkRange::new(0, 4);
        let (chunk_byte_indices, data) = random_bytes(&mut rng, &range, RANGE_LEN);
        let put_result = cache.put(&key, &range, &chunk_byte_indices, data.as_slice()).await;
        assert!(put_result.is_ok(), "{put_result:?}");

        print_directory_contents(cache_root.as_ref());

        // hit
        let cache_result = cache.get(&key, &range).await.unwrap();
        assert!(cache_result.is_some());
        let cache_range = cache_result.unwrap();
        assert_eq!(cache_range.data, data);
        assert_eq!(cache_range.range, range);
        assert_eq!(cache_range.offsets, chunk_byte_indices);

        let miss_range = ChunkRange::new(100, 101);
        // miss
        assert!(cache.get(&key, &miss_range).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_put_get_subrange() {
        let mut rng = StdRng::seed_from_u64(RANDOM_SEED);
        let cache_root = TempDir::new("put_get_subrange").unwrap();
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: DEFAULT_CHUNK_CACHE_CAPACITY,
            ..Default::default()
        };
        let cache = DiskCache::initialize(&config).unwrap();

        let key = random_key(&mut rng);
        // following parts of test assume overall inserted range includes chunk 0
        let range = ChunkRange::new(0, 4);
        let (chunk_byte_indices, data) = random_bytes(&mut rng, &range, RANGE_LEN);
        let put_result = cache.put(&key, &range, &chunk_byte_indices, data.as_slice()).await;
        assert!(put_result.is_ok(), "{put_result:?}");

        print_directory_contents(cache_root.as_ref());

        for start in range.start..range.end {
            for end in (start + 1)..=range.end {
                let sub_range = ChunkRange::new(start, end);
                let get_result = cache.get(&key, &sub_range).await.unwrap();
                assert!(get_result.is_some(), "range: [{start} {end})");
                let cache_range = get_result.unwrap();
                assert_eq!(cache_range.range, sub_range);
                // assert that offsets has 1 more item than the range len difference
                assert_eq!(cache_range.offsets.len() as u32, sub_range.end - sub_range.start + 1);

                for (expected, actual) in chunk_byte_indices[(start as usize)..=(end as usize)]
                    .iter()
                    .map(|v| *v - chunk_byte_indices[start as usize])
                    .zip(cache_range.offsets.iter())
                {
                    assert_eq!(*actual, expected);
                }

                let start_byte = chunk_byte_indices[sub_range.start as usize] as usize;
                let end_byte = chunk_byte_indices[sub_range.end as usize] as usize;
                let data_portion = &data[start_byte..end_byte];
                assert_eq!(data_portion, &cache_range.data);
            }
        }
    }

    #[tokio::test]
    async fn test_puts_eviction() {
        const MIN_NUM_KEYS: u32 = 12;
        const CAP: u64 = (RANGE_LEN * (MIN_NUM_KEYS - 1)) as u64;
        let cache_root = TempDir::new("puts_eviction").unwrap();
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: CAP,
            ..Default::default()
        };
        let cache = DiskCache::initialize(&config).unwrap();
        let mut it = RandomEntryIterator::std_from_seed(RANDOM_SEED);

        // fill the cache to almost capacity
        for _ in 0..MIN_NUM_KEYS {
            let (key, range, offsets, data) = it.next().unwrap();
            assert!(cache.put(&key, &range, &offsets, &data).await.is_ok());
        }
        let total_bytes = cache.total_bytes().await;
        assert!(total_bytes <= CAP, "cache size: {} <= {}", output_bytes(total_bytes), output_bytes(CAP));

        let (key, range, offsets, data) = it.next().unwrap();
        let result = cache.put(&key, &range, &offsets, &data).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_same_puts_noop() {
        let cache_root = TempDir::new("same_puts_noop").unwrap();
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: DEFAULT_CHUNK_CACHE_CAPACITY,
            ..Default::default()
        };
        let cache = DiskCache::initialize(&config).unwrap();
        let mut it = RandomEntryIterator::std_from_seed(RANDOM_SEED).with_range_len(1000);
        let (key, range, offsets, data) = it.next().unwrap();
        assert!(cache.put(&key, &range, &offsets, &data).await.is_ok());
        assert!(cache.put(&key, &range, &offsets, &data).await.is_ok());
    }

    #[tokio::test]
    async fn test_overlap_range_data_mismatch_fail() {
        let setup = || async move {
            let mut it = RandomEntryIterator::std_from_seed(RANDOM_SEED);
            let cache_root = TempDir::new("overlap_range_data_mismatch_fail").unwrap();
            let config = CacheConfig {
                cache_directory: cache_root.path().to_path_buf(),
                cache_size: DEFAULT_CHUNK_CACHE_CAPACITY,
                ..Default::default()
            };
            let cache = DiskCache::initialize(&config).unwrap();
            let (key, range, offsets, data) = it.next().unwrap();
            assert!(cache.put(&key, &range, &offsets, &data).await.is_ok());
            (cache_root, cache, key, range, offsets, data)
        };

        // bad offsets
        // totally random, mismatch len from range
        let (_cache_root, cache, key, range, mut offsets, data) = setup().await;
        offsets.remove(1);
        assert!(cache.put(&key, &range, &offsets, &data).await.is_err());

        // start isn't 0
        let (_cache_root, cache, key, range, mut offsets, data) = setup().await;
        offsets[0] = 100;
        assert!(cache.put(&key, &range, &offsets, &data).await.is_err());

        // end isn't data.len()
        let (_cache_root, cache, key, range, mut offsets, data) = setup().await;
        *offsets.last_mut().unwrap() = data.len() as u32 + 1;
        assert!(cache.put(&key, &range, &offsets, &data).await.is_err());

        // not strictly increasing
        let (_cache_root, cache, key, range, mut offsets, data) = setup().await;
        offsets[2] = offsets[1];
        assert!(cache.put(&key, &range, &offsets, &data).await.is_err());

        // not matching
        let (_cache_root, cache, key, range, mut offsets, data) = setup().await;
        offsets[1] += 1;
        assert!(cache.put(&key, &range, &offsets, &data).await.is_err());

        // bad data
        // size mismatch given offsets
        let (_cache_root, cache, key, range, offsets, data) = setup().await;
        assert!(cache.put(&key, &range, &offsets, &data[1..]).await.is_err());

        // data changed
        let (_cache_root, cache, key, range, offsets, mut data) = setup().await;
        data[0] += 1;
        assert!(cache.put(&key, &range, &offsets, &data).await.is_err());
    }

    #[tokio::test]
    async fn test_initialize_non_empty() {
        let cache_root = TempDir::new("initialize_non_empty").unwrap();
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: DEFAULT_CHUNK_CACHE_CAPACITY,
            ..Default::default()
        };
        let cache = DiskCache::initialize(&config).unwrap();

        let mut it = RandomEntryIterator::std_from_seed(RANDOM_SEED);

        let mut keys_and_ranges = Vec::new();

        for _ in 0..20 {
            let (key, range, offsets, data) = it.next().unwrap();
            assert!(cache.put(&key, &range, &offsets, &data).await.is_ok());
            keys_and_ranges.push((key, range));
        }

        let cache2 = DiskCache::initialize(&config).unwrap();
        for (i, (key, range)) in keys_and_ranges.iter().enumerate() {
            let get_result = cache2.get(&key, &range).await;
            assert!(get_result.is_ok(), "{i} {get_result:?}");
            assert!(get_result.unwrap().is_some(), "{i}");
        }

        let cache_keys = cache.state.read().await.inner.keys().cloned().collect::<BTreeSet<_>>();
        let cache2_keys = cache2.state.read().await.inner.keys().cloned().collect::<BTreeSet<_>>();
        assert_eq!(cache_keys, cache2_keys);
    }

    #[tokio::test]
    async fn test_initialize_too_large_file() {
        const LARGE_FILE: u64 = 1000;
        let cache_root = TempDir::new("initialize_too_large_file").unwrap();
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: DEFAULT_CHUNK_CACHE_CAPACITY,
            ..Default::default()
        };
        let cache = DiskCache::initialize(&config).unwrap();
        let mut it = RandomEntryIterator::std_from_seed(RANDOM_SEED).with_range_len(LARGE_FILE as u32);

        let (key, range, offsets, data) = it.next().unwrap();
        cache.put(&key, &range, &offsets, &data).await.unwrap();
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: LARGE_FILE - 1,
            ..Default::default()
        };
        let cache2 = DiskCache::initialize(&config).unwrap();

        assert_eq!(cache2.total_bytes().await, 0);
    }

    #[tokio::test]
    async fn test_initialize_stops_loading_early_with_too_many_files() {
        const LARGE_FILE: u64 = 1000;
        let cache_root = TempDir::new("initialize_stops_loading_early_with_too_many_files").unwrap();
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: LARGE_FILE * 10,
            ..Default::default()
        };
        let cache = DiskCache::initialize(&config).unwrap();
        let mut it = RandomEntryIterator::std_from_seed(RANDOM_SEED).with_range_len(LARGE_FILE as u32);
        for _ in 0..10 {
            let (key, range, offsets, data) = it.next().unwrap();
            cache.put(&key, &range, &offsets, &data).await.unwrap();
        }

        let cap2 = LARGE_FILE * 2;
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: cap2,
            ..Default::default()
        };
        let cache2 = DiskCache::initialize(&config).unwrap();

        assert!(cache2.total_bytes().await < cap2 * 3, "{} < {}", cache2.total_bytes().await, cap2 * 3);
    }

    #[test]
    fn test_dir_name_to_key() {
        let s = "oL-Xqk1J00kVe1U4kCko-Kw4zaVv3-4U73i27w5DViBkZWZhdWx0";
        let key = try_parse_key(s.as_bytes());
        assert!(key.is_ok(), "{key:?}")
    }

    #[tokio::test]
    async fn test_unknown_eviction() {
        let cache_root = TempDir::new("initialize_non_empty").unwrap();
        let capacity = 12 * RANGE_LEN as u64;
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: capacity,
            ..Default::default()
        };
        let cache = DiskCache::initialize(&config).unwrap();
        let mut it = RandomEntryIterator::std_from_seed(RANDOM_SEED);
        let (key, range, chunk_byte_indices, data) = it.next().unwrap();
        cache.put(&key, &range, &chunk_byte_indices, &data).await.unwrap();

        let cache2 = DiskCache::initialize(&config).unwrap();
        let get_result = cache2.get(&key, &range).await;
        assert!(get_result.is_ok());
        assert!(get_result.unwrap().is_some());

        let (key2, range2, chunk_byte_indices2, data2) = it.next().unwrap();
        assert!(cache2.put(&key2, &range2, &chunk_byte_indices2, &data2).await.is_ok());

        let mut get_result_1 = cache2.get(&key, &range).await.unwrap();
        let mut i = 0;
        while get_result_1.is_some() && i < 50 {
            i += 1;
            let (key2, range2, chunk_byte_indices2, data2) = it.next().unwrap();
            cache2.put(&key2, &range2, &chunk_byte_indices2, &data2).await.unwrap();
            get_result_1 = cache2.get(&key, &range).await.unwrap();
        }
        if get_result_1.is_some() {
            // randomness didn't evict the record after 50 tries, don't test this case now
            return;
        }
        // we've evicted the original record from the cache
        // note using the original cache handle without updates!
        let get_result_post_eviction = cache.get(&key, &range).await;
        assert!(get_result_post_eviction.is_ok());
        assert!(get_result_post_eviction.unwrap().is_none());
    }

    #[tokio::test]
    async fn put_subrange() {
        let cache_root = TempDir::new("put_subrange").unwrap();
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: DEFAULT_CHUNK_CACHE_CAPACITY,
            ..Default::default()
        };
        let cache = DiskCache::initialize(&config).unwrap();

        let (key, range, chunk_byte_indices, data) = RandomEntryIterator::std_from_seed(RANDOM_SEED).next().unwrap();
        cache.put(&key, &range, &chunk_byte_indices, &data).await.unwrap();
        let total_bytes = cache.total_bytes().await;

        // left range
        let left_range = ChunkRange::new(range.start, range.end - 1);
        let left_chunk_byte_indices = &chunk_byte_indices[..chunk_byte_indices.len() - 1];
        let left_data = &data[..*left_chunk_byte_indices.last().unwrap() as usize];
        assert!(cache.put(&key, &left_range, left_chunk_byte_indices, left_data).await.is_ok());
        assert_eq!(total_bytes, cache.total_bytes().await);

        // right range
        let right_range = ChunkRange::new(range.start + 1, range.end);
        let right_chunk_byte_indices: Vec<u32> =
            (&chunk_byte_indices[1..]).iter().map(|v| v - chunk_byte_indices[1]).collect();
        let right_data = &data[chunk_byte_indices[1] as usize..];
        assert!(cache
            .put(&key, &right_range, &right_chunk_byte_indices, right_data)
            .await
            .is_ok());
        assert_eq!(total_bytes, cache.total_bytes().await);

        // middle range
        let middle_range = ChunkRange::new(range.start + 1, range.end - 1);
        let middle_chunk_byte_indices: Vec<u32> = (&chunk_byte_indices[1..(chunk_byte_indices.len() - 1)])
            .iter()
            .map(|v| v - chunk_byte_indices[1])
            .collect();
        let middle_data =
            &data[chunk_byte_indices[1] as usize..chunk_byte_indices[chunk_byte_indices.len() - 2] as usize];

        assert!(cache
            .put(&key, &middle_range, &middle_chunk_byte_indices, middle_data)
            .await
            .is_ok());
        assert_eq!(total_bytes, cache.total_bytes().await);
    }

    #[tokio::test]
    async fn test_evictions_with_multiple_range_per_key() {
        const NUM: u32 = 12;
        let cache_root = TempDir::new("multiple_range_per_key").unwrap();
        let capacity = (NUM * RANGE_LEN) as u64;
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: capacity,
            ..Default::default()
        };
        let cache = DiskCache::initialize(&config).unwrap();
        let mut it = RandomEntryIterator::std_from_seed(RANDOM_SEED).with_one_chunk_ranges(true);
        let (key, _, _, _) = it.next().unwrap();
        let mut previously_put: Vec<(Key, ChunkRange)> = Vec::new();

        for _ in 0..(NUM / 2) {
            let (key2, mut range, chunk_byte_indices, data) = it.next().unwrap();
            while previously_put.iter().any(|(_, r)| r.start == range.start) {
                range.start += 1 % 1000;
            }
            cache.put(&key, &range, &chunk_byte_indices, &data).await.unwrap();
            previously_put.push((key.clone(), range.clone()));
            cache.put(&key2, &range, &chunk_byte_indices, &data).await.unwrap();
            previously_put.push((key2, range));
        }

        let mut num_hits = 0;
        for (key, range) in &previously_put {
            let result = cache.get(key, range).await;
            assert!(result.is_ok());
            let result = result.unwrap();
            if result.is_some() {
                num_hits += 1;
            }
        }
        // assert got some hits, exact number depends on item size
        assert_ne!(num_hits, 0);

        // assert that we haven't evicted all keys for key with multiple items
        assert!(cache.state.read().await.inner.contains_key(&key), "evicted key that should have remained in cache");
    }

    #[test]
    fn test_initialize_with_cache_size_0() {
        assert!(DiskCache::initialize(&CacheConfig {
            cache_directory: "/tmp".into(),
            cache_size: 0,
        })
        .is_err());
    }
}

#[cfg(test)]
mod concurrency_tests {
    use tempdir::TempDir;

    use super::DiskCache;
    use crate::disk::DEFAULT_CHUNK_CACHE_CAPACITY;
    use crate::{CacheConfig, ChunkCache, RandomEntryIterator, RANGE_LEN};

    const NUM_ITEMS_PER_TASK: usize = 20;
    const RANDOM_SEED: u64 = 878987298749287;

    #[tokio::test]
    async fn test_run_concurrently() {
        let cache_root = TempDir::new("run_concurrently").unwrap();

        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: DEFAULT_CHUNK_CACHE_CAPACITY,
            ..Default::default()
        };
        let cache = DiskCache::initialize(&config).unwrap();

        let num_tasks = 2 + rand::random::<u8>() % 14;

        let mut handles = Vec::with_capacity(num_tasks as usize);
        for _ in 0..num_tasks {
            let cache_clone = cache.clone();
            handles.push(tokio::spawn(async move {
                let mut it = RandomEntryIterator::std_from_seed(RANDOM_SEED);
                let mut kr = Vec::with_capacity(NUM_ITEMS_PER_TASK);
                for _ in 0..NUM_ITEMS_PER_TASK {
                    let (key, range, chunk_byte_indices, data) = it.next().unwrap();
                    assert!(cache_clone.put(&key, &range, &chunk_byte_indices, &data).await.is_ok());
                    kr.push((key, range));
                }
                for (key, range) in kr {
                    assert!(cache_clone.get(&key, &range).await.is_ok());
                }
            }))
        }

        for handle in handles {
            handle.await.expect("join should not error");
        }
    }

    #[tokio::test]
    async fn test_run_concurrently_with_evictions() {
        let cache_root = TempDir::new("run_concurrently_with_evictions").unwrap();
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: RANGE_LEN as u64 * NUM_ITEMS_PER_TASK as u64,
            ..Default::default()
        };
        let cache = DiskCache::initialize(&config).unwrap();

        let num_tasks = 2 + rand::random::<u8>() % 14;

        let mut handles = Vec::with_capacity(num_tasks as usize);
        for _ in 0..num_tasks {
            let cache_clone = cache.clone();
            handles.push(tokio::spawn(async move {
                let mut it = RandomEntryIterator::std_from_seed(RANDOM_SEED);
                let mut kr = Vec::with_capacity(NUM_ITEMS_PER_TASK);
                for _ in 0..NUM_ITEMS_PER_TASK {
                    let (key, range, chunk_byte_indices, data) = it.next().unwrap();
                    assert!(cache_clone.put(&key, &range, &chunk_byte_indices, &data).await.is_ok());
                    kr.push((key, range));
                }
                for (key, range) in kr {
                    assert!(cache_clone.get(&key, &range).await.is_ok());
                }
            }))
        }

        for handle in handles {
            handle.await.expect("join should not error");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_run_concurrently_thundering_herd() {
        let cache_root = TempDir::new("run_concurrently_thundering_herd").unwrap();
        let config = CacheConfig {
            cache_directory: cache_root.path().to_path_buf(),
            cache_size: RANGE_LEN as u64 * NUM_ITEMS_PER_TASK as u64,
        };
        let cache = DiskCache::initialize(&config).unwrap();

        // data inserted is the same
        let mut it = RandomEntryIterator::std_from_seed(RANDOM_SEED);
        let (key, range, chunk_byte_indices, data) = it.next().unwrap();

        // Spawn tasks to simultaneously insert into cache
        let num_tasks = 64;
        let mut handles = Vec::with_capacity(num_tasks as usize);
        for _ in 0..num_tasks {
            let cache_clone = cache.clone();
            let key = key.clone();
            let range = range.clone();
            let chunk_byte_indices = chunk_byte_indices.clone();
            let data_clone = data.clone();
            handles.push(tokio::spawn(async move {
                let res = cache_clone.put(&key, &range, &chunk_byte_indices, &data_clone).await;
                assert!(res.is_ok(), "err: {res:?}");
            }))
        }

        for handle in handles {
            handle.await.expect("join should not error");
        }

        // check that there is only 1 term in the cache for this data
        let state = cache.state.read().await;
        let items = state.inner.get(&key).unwrap();

        let num = items.iter().filter(|item| item.range == range).count();
        assert_eq!(num, 1);
    }
}
