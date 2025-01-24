use crate::disk::cache_file_header::CacheFileHeader;
use crate::disk::cache_item::CacheItem;
use crate::error::ChunkCacheError;
use crate::{CacheConfig, ChunkCache};
use base64::engine::general_purpose::URL_SAFE;
use base64::engine::GeneralPurpose;
use base64::Engine;
use cas_types::{ChunkRange, Key};
use dashmap::DashMap;
use error_printer::ErrorPrinter;
use file_utils::SafeFileCreator;
use merklehash::MerkleHash;
use std::fs::{DirEntry, File};
use std::future::Future;
use std::io;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use utils::ThreadPool;

mod cache_file_header;
mod cache_item;
pub mod test_utils;

// consistently use URL_SAFE (also file path safe) base64 codec
pub(crate) const BASE64_ENGINE: GeneralPurpose = URL_SAFE;
pub const DEFAULT_CAPACITY: u64 = 10 << 30; // 10 GB
const PREFIX_DIR_NAME_LEN: usize = 2;

type OptionResult<T, E> = Result<Option<T>, E>;
type CacheState = DashMap<Key, Vec<CacheItem>>;

struct RemoveItemOp {
    key: Key,
    item: CacheItem,
    remove_file: bool,
}

struct AddItemOp {
    key: Key,
    item: CacheItem,
}

enum StateEditorOp {
    RemoveItem(RemoveItemOp),
    AddItem(AddItemOp),
}

pub struct DiskCache {
    cache_root: PathBuf,
    capacity: u64,
    state: Arc<CacheState>,
    thread_pool: Arc<ThreadPool>,
    file_remover_jh: JoinHandle<Result<(), ChunkCacheError>>,
    file_remove_send: Sender<PathBuf>,
    state_editor_op_jh: JoinHandle<Result<(), ChunkCacheError>>,
    state_editor_op_send: Sender<StateEditorOp>,
}

impl DiskCache {
    /// initialize will create a new DiskCache with the capacity and cache root based on the config
    /// the cache file system layout is rooted at the provided config.cache_directory and initialize
    /// will attempt to load any pre-existing cache state into memory.
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
    pub fn initialize(config: &CacheConfig, thread_pool: Arc<ThreadPool>) -> Result<Self, ChunkCacheError> {
        let capacity = config.cache_size;
        let cache_root = config.cache_directory.clone();

        let state = Arc::new(Self::initialize_state(&cache_root, capacity)?);

        let (file_remove_send, file_remove_recv) = channel();
        let file_remover_jh = thread_pool.spawn(FileRemover::new(file_remove_recv));

        let (state_editor_op_send, state_editor_op_recv) = channel();
        let state_editor_op_jh = thread_pool.spawn(StateEditor::new(
            state.clone(),
            state_editor_op_recv,
            file_remove_send.clone(),
            cache_root.clone(),
            capacity,
        ));

        Ok(Self {
            state,
            cache_root,
            capacity,
            thread_pool,
            file_remover_jh,
            file_remove_send,
            state_editor_op_jh,
            state_editor_op_send,
        })
    }

    fn initialize_state(cache_root: &PathBuf, capacity: u64) -> Result<CacheState, ChunkCacheError> {
        let state = DashMap::new();
        let mut total_bytes = 0;
        let max_num_bytes = 2 * capacity;

        let Some(readdir) = read_dir(cache_root)? else {
            return Ok(state);
        };

        // loop through cache root directory, first level containing "prefix" directories
        // each of which may contain key directories with cache items
        for key_prefix_dir in readdir {
            // this match pattern appears often in this function, and we could write a macro to replace it
            // however this puts an implicit change of control flow with continue/return cases that is
            // hard to decipher from a macro, so avoid replace it for readability
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
                let Some(key_dir) = is_ok_dir(key_dir)? else {
                    continue;
                };

                let key_dir_name = key_dir.file_name();

                // asserts that the prefix dir name is actually the prefix of this key dir
                debug_assert_eq!(
                    key_dir_name.as_encoded_bytes()[..PREFIX_DIR_NAME_LEN].to_ascii_uppercase(),
                    key_prefix_dir_name.as_encoded_bytes().to_ascii_uppercase(),
                    "{key_dir_name:?}",
                );

                let Ok(key) = try_parse_key(key_dir_name.as_encoded_bytes())
                    .debug_error(format!("failed to decoded a directory name \"{key_dir_name:?}\" as a key"))
                else {
                    continue;
                };

                let mut items = Vec::new();

                let Some(key_readdir) = read_dir(key_dir.path())? else {
                    continue;
                };

                // loop through cache items inside key directory
                for item in key_readdir {
                    let Some(cache_item) = try_parse_cache_file(item, capacity)? else {
                        continue;
                    };

                    total_bytes += cache_item.len;
                    items.push(cache_item);

                    // if already filled capacity, stop iterating over cache items
                    if total_bytes >= max_num_bytes {
                        state.insert(key, items);
                        return Ok(state);
                    }
                }

                if !items.is_empty() {
                    state.insert(key, items);
                }
            }
        }

        Ok(state)
    }

    fn item_path(&self, key: &Key, cache_item: &CacheItem) -> Result<PathBuf, ChunkCacheError> {
        Ok(self.cache_root.join(key_dir(key)).join(cache_item.file_name()?))
    }

    fn find_matches(&self, key: &Key, range: &ChunkRange) -> Result<Vec<CacheItem>, ChunkCacheError> {
        let items = if let Some(items) = self.state.get(key) {
            items
        } else {
            return Ok(Vec::new());
        };

        // attempt to find a matching range in the given key's items using
        Ok(items
            .iter()
            .filter(|item| range_encompass(&item.range, range))
            .cloned()
            .collect())
    }

    fn remove_item(&self, key: Key, item: CacheItem, remove_file: bool) -> Result<(), ChunkCacheError> {
        self.state_editor_op_send
            .send(StateEditorOp::RemoveItem(RemoveItemOp { key, item, remove_file }))?;
        Ok(())
    }

    fn enqueue_new_cache_item(&self, key: Key, item: CacheItem) -> Result<(), ChunkCacheError> {
        self.state_editor_op_send
            .send(StateEditorOp::AddItem(AddItemOp { key, item }))?;
        Ok(())
    }

    fn write_cache_file(
        &self,
        key: &Key,
        range: &ChunkRange,
        chunk_byte_indices: &[u32],
        data: &[u8],
    ) -> Result<CacheItem, ChunkCacheError> {
        let header = CacheFileHeader::new(chunk_byte_indices);

        let cache_item = CacheItem {
            range: range.clone(),
            len: (header.header_len() + data.len()) as u64,
        };

        let path = self.item_path(key, &cache_item)?;

        let mut fw = SafeFileCreator::new(path)?;

        header.serialize(&mut fw)?;
        fw.write_all(data)?;
        fw.close()?;
        Ok(cache_item)
    }
    fn get_impl(&self, key: &Key, range: &ChunkRange) -> Result<Option<Vec<u8>>, ChunkCacheError> {
        let item_matches = self.find_matches(key, range)?;
        if item_matches.is_empty() {
            return Ok(None);
        }

        for cache_item in item_matches.into_iter() {
            let path = self.item_path(key, &cache_item)?;
            let mut file = match File::open(&path) {
                Ok(file) => file,
                Err(e) => match e.kind() {
                    ErrorKind::NotFound => {
                        self.remove_item(key.clone(), cache_item, false)?;
                        continue;
                    },
                    _ => return Err(e.into()),
                },
            };

            let header_result = CacheFileHeader::deserialize(&mut file)
                .debug_error(format!("failed to deserialize cache file header on path: {path:?}"));
            let header = if let Ok(header) = header_result {
                header
            } else {
                self.remove_item(key.clone(), cache_item, true)?;
                continue;
            };

            let start = cache_item.range.start;
            let result_buf = get_range_from_cache_file(&header, &mut file, range, start)?;
            return Ok(Some(result_buf));
        }
        Ok(None)
    }

    fn put_impl(
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
            // assert 1 new range doesn't take up more than 10% of capacity
            || data.len() > (self.capacity as usize / 10)
        {
            return Err(ChunkCacheError::InvalidArguments);
        }

        let item_matches = self.find_matches(key, range)?;
        if !item_matches.is_empty() {
            // TODO: validate a match
            return Ok(());
        }

        let cache_item = self.write_cache_file(key, range, chunk_byte_indices, data)?;
        self.enqueue_new_cache_item(key.clone(), cache_item)?;

        Ok(())
    }
}

struct FileRemover {
    inc: Receiver<PathBuf>,
}

impl FileRemover {
    fn new(inc: Receiver<PathBuf>) -> Self {
        Self { inc }
    }
}

impl Future for FileRemover {
    type Output = Result<(), ChunkCacheError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        // TODO: do better batching
        // relinquish faster
        loop {
            let next_path = match self.inc.try_recv() {
                Ok(path) => path,
                Err(TryRecvError::Empty) => return Poll::Pending,
                Err(TryRecvError::Disconnected) => return Poll::Ready(Ok(())),
            };
            remove_file(&next_path)?;
            if let Some(parent) = next_path.parent() {
                check_remove_dir(parent)?;
            }
        }

        // let mut buffer = Vec::with_capacity(MAX_BUFFER_SIZE);
        // let num_to_delete = ready!(self.inc.  (cx, &mut buffer, MAX_BUFFER_SIZE));
        // if num_to_delete == 0 {
        //     // graceful shutdown, sender is closed;
        //     return Poll::Ready(Ok(()));
        // }
        // let mut parent_paths = HashSet::with_capacity(MAX_BUFFER_SIZE);
        // for file_path in buffer.iter() {
        //     remove_file(file_path)?;
        //     if let Some(parent) = file_path.parent() {
        //         parent_paths.insert(parent);
        //     }
        // }
        // for parent_path in parent_paths {
        //     check_remove_dir(parent_path)?;
        // }
        //
        // Poll::Pending
    }
}

struct StateEditor {
    state: Arc<CacheState>,
    op_recv: Receiver<StateEditorOp>,
    file_remover_send: Sender<PathBuf>,
    cache_root: PathBuf,
    capacity: u64,
}

impl StateEditor {
    fn new(
        state: Arc<CacheState>,
        op_recv: Receiver<StateEditorOp>,
        file_remover_send: Sender<PathBuf>,
        cache_root: PathBuf,
        capacity: u64,
    ) -> Self {
        Self {
            state,
            op_recv,
            file_remover_send,
            cache_root,
            capacity,
        }
    }

    fn remove_item(&mut self, op: RemoveItemOp) -> Result<(), ChunkCacheError> {
        let RemoveItemOp { key, item, remove_file } = op;
        if let Some(mut items) = self.state.get_mut(&key) {
            if let Some(idx) = index_of(items.as_ref(), &item) {
                items.swap_remove(idx);
            }
            let is_empty = items.is_empty();
            drop(items);
            if is_empty {
                self.state.remove(&key);
            }
        }
        if remove_file {
            let path = item_path(&self.cache_root, &key, &item)?;
            self.file_remover_send.send(path)?;
        }

        Ok(())
    }

    fn add_item(&mut self, op: AddItemOp) -> Result<(), ChunkCacheError> {
        let AddItemOp { key, item } = op;

        // remove obsolete items
        {
            if let Some(mut existing_items) = self.state.get_mut(&key) {
                let mut to_remove = Vec::new();
                for (idx, existing_item) in existing_items.iter().enumerate() {
                    if range_encompass(&item.range, &existing_item.range) {
                        to_remove.push(idx);
                    }
                }
                for idx in to_remove.into_iter().rev() {
                    let rm_item = existing_items.swap_remove(idx);
                    let path = item_path(&self.cache_root, &key, &rm_item)?;
                    self.file_remover_send.send(path)?;
                }
                let is_empty = existing_items.is_empty();
                drop(existing_items);
                if is_empty {
                    // remove to help assumptions for eviction
                    self.state.remove(&key);
                }
            }
        }

        // evict
        let (mut total_bytes, mut num_items) = self.state_stat();
        while total_bytes > self.capacity - item.len {
            let mut count = 0;
            let random_item = rand::random::<usize>() % num_items;
            let mut key_idx = None;
            for items in self.state.iter() {
                if random_item < count + items.len() {
                    key_idx = Some((key.clone(), random_item - count));
                    break;
                }
                count -= items.len();
            }
            // if we failed to set a random item, there is a state invariant violation
            // this indicates the state was modified by another thread
            // but only the state editor thread is allowed to modify the state!
            debug_assert!(key_idx.is_some());
            // to be safe we ignore it and retry with updated stats
            let Some((evict_key, evict_idx)) = key_idx else {
                let (updated_total_bytes, updated_num_items) = self.state_stat();
                total_bytes = updated_total_bytes;
                num_items = updated_num_items;
                continue;
            };
            if let Some(mut items) = self.state.get_mut(&evict_key) {
                let rm_item = items.swap_remove(evict_idx);
                total_bytes -= rm_item.len;
                num_items -= 1;
                let path = item_path(&self.cache_root, &evict_key, &rm_item)?;
                self.file_remover_send.send(path)?;

                let is_empty = items.is_empty();
                drop(items);
                if is_empty {
                    self.state.remove(&evict_key);
                }
            }
        }

        // finally insert
        self.state.entry(key).or_default().push(item);

        Ok(())
    }

    fn state_stat(&self) -> (u64, usize) {
        let mut total_bytes = 0;
        let mut num_items = 0;
        for key_items in self.state.iter() {
            for item in key_items.iter() {
                total_bytes += item.len;
                num_items += 1;
            }
        }
        (total_bytes, num_items)
    }
}

impl Future for StateEditor {
    type Output = Result<(), ChunkCacheError>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let op = match self.op_recv.try_recv() {
                Ok(op) => op,
                Err(TryRecvError::Empty) => return Poll::Pending,
                Err(TryRecvError::Disconnected) => return Poll::Ready(Ok(())),
            };
            match op {
                StateEditorOp::RemoveItem(op) => self.remove_item(op),
                StateEditorOp::AddItem(op) => self.add_item(op),
            }?;
        }

        // let mut buffer = Vec::with_capacity(MAX_BUFFER_SIZE);
        // let num_to_delete = ready!(self.op_recv.poll_recv_many(cx, &mut buffer, MAX_BUFFER_SIZE));
        // if num_to_delete == 0 {
        //     // graceful shutdown, sender is closed;
        //     return Poll::Ready(Ok(()));
        // }
        //
        // for op in buffer.into_iter() {
        //     if let Err(e) = match op {
        //         StateEditorOp::RemoveItem(op) => self.remove_item(op),
        //         StateEditorOp::AddItem(op) => self.add_item(op),
        //     } {
        //         return Poll::Ready(Err(e));
        //     }
        // }
        //
        // Poll::Pending
    }
}

impl ChunkCache for DiskCache {
    fn get(&self, key: &Key, range: &ChunkRange) -> Result<Option<Vec<u8>>, ChunkCacheError> {
        self.get_impl(key, range)
    }

    fn put(
        &self,
        key: &Key,
        range: &ChunkRange,
        chunk_byte_indices: &[u32],
        data: &[u8],
    ) -> Result<(), ChunkCacheError> {
        self.put_impl(key, range, chunk_byte_indices, data)
    }
}

fn item_path(cache_root: &PathBuf, key: &Key, cache_item: &CacheItem) -> Result<PathBuf, ChunkCacheError> {
    Ok(cache_root.join(key_dir(key)).join(cache_item.file_name()?))
}

#[inline]
fn range_encompass(range: &ChunkRange, sub_range: &ChunkRange) -> bool {
    range.start <= sub_range.start && sub_range.end <= range.end
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
    if md.len() > DEFAULT_CAPACITY {
        return Err(ChunkCacheError::general(format!(
            "Cache directory contains a file larger than {} GB, cache directory state is invalid",
            (DEFAULT_CAPACITY as f64 / (1 << 30) as f64)
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
            warn!("not a valid cache file, removing: {:?} {e:?}", item.file_name());
            remove_file(item.path())?;
            return Ok(None);
        },
    };
    if md.len() != cache_item.len {
        // file is invalid, remove it
        warn!(
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
) -> Result<Vec<u8>, ChunkCacheError> {
    let start_byte = header
        .chunk_byte_indices
        .get((range.start - start) as usize)
        .ok_or(ChunkCacheError::BadRange)?;
    let end_byte = header
        .chunk_byte_indices
        .get((range.end - start) as usize)
        .ok_or(ChunkCacheError::BadRange)?;
    file_contents.seek(SeekFrom::Start((*start_byte as usize + header.header_len()) as u64))?;
    let mut buf = vec![0; (end_byte - start_byte) as usize];
    file_contents.read_exact(&mut buf)?;
    Ok(buf)
}
