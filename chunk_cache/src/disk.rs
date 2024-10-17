use std::{
    collections::HashMap,
    fs::{read_dir, File},
    io::{Cursor, ErrorKind, Read, Seek, SeekFrom, Write},
    mem::size_of,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use base64::engine::general_purpose::URL_SAFE;
use base64::{engine::GeneralPurpose, Engine};
use cache_file_header::CacheFileHeader;
use cache_item::{range_contained_fn, CacheItem};
use cas_types::{Key, Range};
use error_printer::ErrorPrinter;
use file_utils::SafeFileCreator;
use merklehash::MerkleHash;
use sorted_vec::SortedVec;
use tracing::debug;

use crate::{error::ChunkCacheError, ChunkCache};

mod cache_file_header;
mod cache_item;
pub mod test_utils;

// consistently use URL_SAFE (also file path safe) base64 codec
pub(crate) const BASE64_ENGINE: GeneralPurpose = URL_SAFE;
pub const DEFAULT_CAPACITY: u64 = 1 << 30; // 1 GB

#[derive(Debug, Clone)]
struct CacheState {
    inner: HashMap<Key, SortedVec<CacheItem>>,
    num_items: usize,
    total_bytes: u64,
}

impl CacheState {
    fn new(state: HashMap<Key, SortedVec<CacheItem>>, num_items: usize, total_bytes: u64) -> Self {
        Self {
            inner: state,
            num_items,
            total_bytes,
        }
    }
}

/// DiskCache is a ChunkCache implementor that saves data on the file system
#[derive(Debug, Clone)]
pub struct DiskCache {
    cache_root: PathBuf,
    capacity: u64,
    state: Arc<Mutex<CacheState>>,
}

fn parse_key(file_name: &[u8]) -> Result<Key, ChunkCacheError> {
    let buf = BASE64_ENGINE.decode(file_name)?;
    let hash = MerkleHash::from_slice(&buf[..size_of::<MerkleHash>()])?;
    let prefix = String::from(std::str::from_utf8(&buf[size_of::<MerkleHash>()..])?);
    Ok(Key { prefix, hash })
}

impl DiskCache {
    pub fn num_items(&self) -> Result<usize, ChunkCacheError> {
        let state = self.state.lock()?;
        Ok(state.num_items)
    }

    pub fn total_bytes(&self) -> Result<u64, ChunkCacheError> {
        let state = self.state.lock()?;
        Ok(state.total_bytes)
    }

    pub fn initialize(cache_root: PathBuf, capacity: u64) -> Result<Self, ChunkCacheError> {
        let mut state = HashMap::new();
        let mut total_bytes = 0;
        let mut num_items = 0;
        let max_num_bytes = 2 * capacity;

        let readdir = match std::fs::read_dir(&cache_root) {
            Ok(rd) => rd,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    return Ok(Self {
                        cache_root,
                        capacity,
                        state: Arc::new(Mutex::new(CacheState::new(state, 0, 0))),
                    });
                }
                return Err(e.into());
            }
        };

        for key_dir in readdir {
            let key_dir = match key_dir {
                Ok(kd) => kd,
                Err(e) => {
                    if e.kind() == ErrorKind::NotFound {
                        continue;
                    }
                    return Err(e.into());
                }
            };
            let md = match key_dir.metadata() {
                Ok(md) => md,
                Err(e) => {
                    if e.kind() == ErrorKind::NotFound {
                        continue;
                    }
                    return Err(e.into());
                }
            };
            if !md.is_dir() {
                debug!(
                    "CACHE: expected key directory at {:?}, is not directory",
                    key_dir.path()
                );
                continue;
            }
            let key = match parse_key(key_dir.file_name().as_encoded_bytes())
                .debug_error("failed to decoded a directory name as a key")
            {
                Ok(key) => key,
                Err(_) => continue,
            };
            let mut items = SortedVec::new();

            let key_readdir = match std::fs::read_dir(key_dir.path()) {
                Ok(krd) => krd,
                Err(e) => {
                    if e.kind() == ErrorKind::NotFound {
                        continue;
                    }
                    return Err(e.into());
                }
            };

            for item in key_readdir {
                let item = match item {
                    Ok(item) => item,
                    Err(e) => {
                        if e.kind() == ErrorKind::NotFound {
                            continue;
                        }
                        return Err(e.into());
                    }
                };
                let md = match item.metadata() {
                    Ok(md) => md,
                    Err(e) => {
                        if e.kind() == ErrorKind::NotFound {
                            continue;
                        }
                        return Err(e.into());
                    }
                };

                if !md.is_file() {
                    continue;
                }
                if md.len() > DEFAULT_CAPACITY {
                    return Err(ChunkCacheError::general(format!("Cache directory contains a file larger than {} GB, cache directory state is invalid", (DEFAULT_CAPACITY as f64 / (1 << 30) as f64))));
                }

                // don't track an item that takes up the whole capacity
                if md.len() > capacity {
                    continue;
                }

                let cache_item = match CacheItem::parse(item.file_name().as_encoded_bytes())
                    .debug_error("failed to decode a file name as a cache item")
                {
                    Ok(i) => i,
                    Err(e) => {
                        debug!(
                            "error parsing cache item file info from path: {:?}, {e}",
                            item.path()
                        );
                        continue;
                    }
                };
                if md.len() != cache_item.len {
                    // file is invalid, remove it
                    remove_file(item.path())?;
                }

                total_bytes += cache_item.len;
                num_items += 1;
                items.push(cache_item);

                if total_bytes >= max_num_bytes {
                    break;
                }
            }

            if !items.is_empty() {
                state.insert(key, items);
            }

            if total_bytes >= max_num_bytes {
                break;
            }
        }

        Ok(Self {
            state: Arc::new(Mutex::new(CacheState::new(state, num_items, total_bytes))),
            cache_root,
            capacity,
        })
    }

    fn get_impl(&self, key: &Key, range: &Range) -> Result<Option<Vec<u8>>, ChunkCacheError> {
        if range.start >= range.end {
            return Err(ChunkCacheError::InvalidArguments);
        }

        loop {
            let cache_item = if let Some(item) = self.find_match(key, range)? {
                item
            } else {
                return Ok(None);
            };

            let path = self.item_path(key, &cache_item)?;

            let mut file_buf = {
                let mut file = match File::open(&path) {
                    Ok(file) => file,
                    Err(e) => match e.kind() {
                        ErrorKind::NotFound => {
                            self.remove_item(key, &cache_item)?;
                            continue;
                        }
                        _ => return Err(e.into()),
                    },
                };
                let mut buf = Vec::with_capacity(file.metadata()?.len() as usize);
                file.read_to_end(&mut buf)?;
                Cursor::new(buf)
            };
            let hash = compute_hash_from_reader(&mut file_buf)?;
            if hash != cache_item.hash {
                debug!("file hash mismatch on path: {path:?}, key: {key}, item: {cache_item}");
                self.remove_item(key, &cache_item)?;
                continue;
            }

            file_buf.seek(SeekFrom::Start(0))?;
            let header_result = CacheFileHeader::deserialize(&mut file_buf).debug_error(format!(
                "failed to deserialize cache file header on path: {path:?}"
            ));
            let header = if let Ok(header) = header_result {
                header
            } else {
                self.remove_item(key, &cache_item)?;
                continue;
            };

            let start = cache_item.range.start;
            let result_buf = get_range_from_cache_file(&header, &mut file_buf, range, start)?;
            return Ok(Some(result_buf));
        }
    }

    fn find_match(&self, key: &Key, range: &Range) -> Result<Option<CacheItem>, ChunkCacheError> {
        let state = self.state.lock()?;
        let items = if let Some(items) = state.inner.get(key) {
            items
        } else {
            return Ok(None);
        };

        // attempt to find a matching range in the given key's items using binary search
        let idx = items.binary_search_by(range_contained_fn(range));
        if idx.is_err() {
            // no matching range for this key
            return Ok(None);
        }
        let idx = idx.expect("already checked for error case");
        let item = items.get(idx).ok_or(ChunkCacheError::Infallible)?;
        Ok(Some(item.clone()))
    }

    fn put_impl(
        &self,
        key: &Key,
        range: &Range,
        chunk_byte_indicies: &[u32],
        data: &[u8],
    ) -> Result<(), ChunkCacheError> {
        if range.start >= range.end
            || chunk_byte_indicies.len() != (range.end - range.start + 1) as usize
            // chunk_byte_indices is guarenteed to be more than 1 element at this point
            || chunk_byte_indicies[0] != 0
            || *chunk_byte_indicies.last().unwrap() as usize != data.len()
            || !strictly_increasing(chunk_byte_indicies)
            // assert 1 new range doesn't take up more than 10% of capacity
            || data.len() > (self.capacity as usize / 10)
        {
            return Err(ChunkCacheError::InvalidArguments);
        }

        // check if we already contain the range
        while let Some(cache_item) = self.find_match(key, range)? {
            if self.validate_match(key, range, chunk_byte_indicies, data, &cache_item)? {
                return Ok(());
            }
        }

        let header = CacheFileHeader::new(chunk_byte_indicies);
        let mut header_buf = Vec::with_capacity(header.header_len());
        header.serialize(&mut header_buf)?;
        let hash = compute_hash(&header_buf, data);

        let cache_item = CacheItem {
            range: range.clone(),
            len: (header_buf.len() + data.len()) as u64,
            hash,
        };

        let path = self.item_path(key, &cache_item)?;

        let mut fw = SafeFileCreator::new(path)?;

        fw.write_all(&header_buf)?;
        fw.write_all(data)?;
        fw.close()?;

        // evict items after ensuring the file write but before committing to cache state
        // to avoid removing new item.
        self.maybe_evict(cache_item.len)?;

        let mut state = self.state.lock()?;
        state.num_items += 1;
        state.total_bytes += cache_item.len;
        let item_set = state.inner.entry(key.clone()).or_default();
        item_set.insert(cache_item);

        Ok(())
    }

    // on a non-error case, returns true if the item is a good match and a new item should not be inserted
    // returns false if not a good match and should be removed.
    fn validate_match(
        &self,
        key: &Key,
        range: &Range,
        chunk_byte_indicies: &[u32],
        data: &[u8],
        cache_item: &CacheItem,
    ) -> Result<bool, ChunkCacheError> {
        // this is a redundant check
        if range.start < cache_item.range.start || range.end > cache_item.range.end {
            return Err(ChunkCacheError::BadRange);
        }

        // validate stored data
        let path = self.item_path(key, cache_item)?;
        let mut r = {
            let mut file = if let Ok(file) = File::open(path) {
                file
            } else {
                self.remove_item(key, cache_item)?;
                return Ok(false);
            };
            let md = file.metadata()?;
            if md.len() != cache_item.len {
                self.remove_item(key, cache_item)?;
                return Ok(false);
            }
            let mut buf = Vec::with_capacity(md.len() as usize);
            file.read_to_end(&mut buf)?;
            Cursor::new(buf)
        };
        let hash = blake3::Hasher::new().update_reader(&mut r)?.finalize();
        if hash != cache_item.hash {
            self.remove_item(key, cache_item)?;
            return Ok(false);
        }
        r.seek(SeekFrom::Start(0))?;
        let header = if let Ok(header) = CacheFileHeader::deserialize(&mut r) {
            header
        } else {
            self.remove_item(key, cache_item)?;
            return Ok(false);
        };

        // validate the chunk_byte_indicies and data input against stored data
        // the chunk_byte_indicies should match the chunk lengths, if the ranges
        // don't start at the same chunk, values will be different, what's important
        // to match is the chunk lengths, i.e. difference in the offsets.
        let idx_start = (range.start - cache_item.range.start) as usize;
        let idx_end = (range.end - cache_item.range.start + 1) as usize;
        for i in idx_start..idx_end - 1 {
            let stored_diff = header.chunk_byte_indicies[i + 1] - header.chunk_byte_indicies[i];
            let given_diff =
                chunk_byte_indicies[i + 1 - idx_start] - chunk_byte_indicies[i - idx_start];
            if stored_diff != given_diff {
                debug!(
                    "failed to match chunk lens for these chunk offsets {} {:?}\n{} {:?}",
                    cache_item.range,
                    &header.chunk_byte_indicies[idx_start..idx_end],
                    range,
                    chunk_byte_indicies
                );
                return Err(ChunkCacheError::InvalidArguments);
            }
        }

        let stored_data =
            get_range_from_cache_file(&header, &mut r, range, cache_item.range.start)?;
        if data != stored_data {
            return Err(ChunkCacheError::InvalidArguments);
        }
        Ok(true)
    }

    /// removed items from the cache (including deleting from file system)
    /// until at least to_remove number of bytes have been removed
    fn maybe_evict(&self, expected_add: u64) -> Result<(), ChunkCacheError> {
        let mut state = self.state.lock()?;
        let total_bytes = state.total_bytes;
        let to_remove = total_bytes as i64 - self.capacity as i64 + expected_add as i64;
        let mut bytes_removed = 0;
        let mut paths = Vec::new();
        while to_remove > bytes_removed {
            let (key, idx) = self.random_item(&state);
            let items = state
                .inner
                .get_mut(&key)
                .ok_or(ChunkCacheError::Infallible)?;
            let cache_item = &items[idx];
            let len = cache_item.len;
            let path = self.item_path(&key, cache_item)?;
            paths.push(path);
            items.remove_index(idx);
            if items.is_empty() {
                state.inner.remove(&key);
            }
            state.total_bytes -= len;
            state.num_items -= 1;

            bytes_removed += len as i64;
        }
        drop(state);

        // remove files after done with modifyinf in memory state and releasing lock
        for path in paths {
            remove_file(&path)?;
            let dir_path = path.parent().ok_or(ChunkCacheError::Infallible)?;
            // check if directory exists and if it does and is empty then remove the directory
            if let Ok(mut readdir) = std::fs::read_dir(dir_path) {
                if readdir.next().is_none() {
                    // no more files in that directory, remove it
                    remove_dir(dir_path)?;
                }
            }
        }
        Ok(())
    }

    /// returns the key and index within that key for a random item
    fn random_item(&self, state: &MutexGuard<'_, CacheState>) -> (Key, usize) {
        let num_items = state.num_items;
        let random_item = rand::random::<usize>() % num_items;
        let mut count = 0;
        for (key, items) in state.inner.iter() {
            if random_item < count + items.len() {
                return (key.clone(), random_item - count);
            }
            count += items.len();
        }

        panic!("should have returned")
    }

    fn remove_item(&self, key: &Key, cache_item: &CacheItem) -> Result<(), ChunkCacheError> {
        {
            let mut state = self.state.lock()?;
            if let Some(items) = state.inner.get_mut(key) {
                items.remove_item(cache_item);
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

        if let Ok(readir) = read_dir(dir_path) {
            if readir.peekable().peek().is_none() {
                // directory empty, remove it
                remove_dir(dir_path)?;
            }
        }

        Ok(())
    }

    fn item_path(&self, key: &Key, cache_item: &CacheItem) -> Result<PathBuf, ChunkCacheError> {
        Ok(self
            .cache_root
            .join(key_dir(key))
            .join(cache_item.file_name()?))
    }
}

fn strictly_increasing(chunk_byte_indicies: &[u32]) -> bool {
    for i in 1..chunk_byte_indicies.len() {
        if chunk_byte_indicies[i - 1] >= chunk_byte_indicies[i] {
            return false;
        }
    }
    true
}

fn get_range_from_cache_file<R: Read + Seek>(
    header: &CacheFileHeader,
    file_contents: &mut R,
    range: &Range,
    start: u32,
) -> Result<Vec<u8>, ChunkCacheError> {
    let start_byte = header
        .chunk_byte_indicies
        .get((range.start - start) as usize)
        .ok_or(ChunkCacheError::BadRange)?;
    let end_byte = header
        .chunk_byte_indicies
        .get((range.end - start) as usize)
        .ok_or(ChunkCacheError::BadRange)?;
    file_contents.seek(SeekFrom::Start(
        (*start_byte as usize + header.header_len()) as u64,
    ))?;
    let mut buf = vec![0; (end_byte - start_byte) as usize];
    file_contents.read_exact(&mut buf)?;
    Ok(buf)
}

fn compute_hash(header: &[u8], data: &[u8]) -> blake3::Hash {
    blake3::Hasher::new().update(header).update(data).finalize()
}

fn compute_hash_from_reader(r: &mut impl Read) -> Result<blake3::Hash, ChunkCacheError> {
    Ok(blake3::Hasher::new().update_reader(r)?.finalize())
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

/// key_dir returns a directory name string formed from the key
/// the format is BASE64_encode([ key.hash[..], key.prefix.as_bytes()[..] ])
fn key_dir(key: &Key) -> String {
    let prefix_bytes = key.prefix.as_bytes();
    let mut buf = vec![0u8; size_of::<MerkleHash>() + prefix_bytes.len()];
    buf[..size_of::<MerkleHash>()].copy_from_slice(key.hash.as_bytes());
    buf[size_of::<MerkleHash>()..].copy_from_slice(prefix_bytes);
    BASE64_ENGINE.encode(buf)
}

impl ChunkCache for DiskCache {
    fn get(&self, key: &Key, range: &Range) -> Result<Option<Vec<u8>>, ChunkCacheError> {
        self.get_impl(key, range)
    }

    fn put(
        &self,
        key: &Key,
        range: &Range,
        chunk_byte_indicies: &[u32],
        data: &[u8],
    ) -> Result<(), ChunkCacheError> {
        self.put_impl(key, range, chunk_byte_indicies, data)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::disk::{parse_key, test_utils::*};

    use cas_types::{Key, Range};
    use rand::thread_rng;
    use tempdir::TempDir;

    use crate::ChunkCache;

    use super::{DiskCache, DEFAULT_CAPACITY};

    #[test]
    fn test_get_cache_empty() {
        let mut rng = thread_rng();
        let cache_root = TempDir::new("empty").unwrap();
        let cache = DiskCache::initialize(cache_root.into_path(), DEFAULT_CAPACITY).unwrap();
        assert!(cache
            .get(&random_key(&mut rng), &random_range(&mut rng))
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_put_get_simple() {
        let mut rng = thread_rng();
        let cache_root = TempDir::new("put_get_simple").unwrap();
        let cache =
            DiskCache::initialize(cache_root.path().to_path_buf(), DEFAULT_CAPACITY).unwrap();

        let key = random_key(&mut rng);
        let range = Range { start: 0, end: 4 };
        let (chunk_byte_indicies, data) = random_bytes(&mut rng, &range, RANGE_LEN);
        let put_result = cache.put(&key, &range, &chunk_byte_indicies, data.as_slice());
        assert!(put_result.is_ok(), "{put_result:?}");

        print_directory_contents(cache_root.as_ref());

        // hit
        assert!(cache.get(&key, &range).unwrap().is_some());
        let miss_range = Range {
            start: 100,
            end: 101,
        };
        // miss
        println!("{:?}", cache.get(&key, &miss_range));
        assert!(cache.get(&key, &miss_range).unwrap().is_none());
    }

    #[test]
    fn test_put_get_subrange() {
        let mut rng = thread_rng();
        let cache_root = TempDir::new("put_get_subrange").unwrap();
        let cache =
            DiskCache::initialize(cache_root.path().to_path_buf(), DEFAULT_CAPACITY).unwrap();

        let key = random_key(&mut rng);
        let range = Range { start: 0, end: 4 };
        let (chunk_byte_indicies, data) = random_bytes(&mut rng, &range, RANGE_LEN);
        let put_result = cache.put(&key, &range, &chunk_byte_indicies, data.as_slice());
        assert!(put_result.is_ok(), "{put_result:?}");

        print_directory_contents(cache_root.as_ref());

        for start in range.start..range.end {
            for end in (start + 1)..=range.end {
                let get_result = cache.get(&key, &Range { start, end }).unwrap();
                assert!(get_result.is_some(), "range: [{start} {end})");
                let data_portion = get_data(&Range { start, end }, &chunk_byte_indicies, &data);
                assert_eq!(data_portion, get_result.unwrap())
            }
        }
    }

    fn get_data<'a>(range: &Range, chunk_byte_indicies: &[u32], data: &'a [u8]) -> &'a [u8] {
        let start = chunk_byte_indicies[range.start as usize] as usize;
        let end = chunk_byte_indicies[range.end as usize] as usize;
        &data[start..end]
    }

    #[test]
    fn test_puts_eviction() {
        const MIN_NUM_KEYS: u32 = 12;
        const CAP: u64 = (RANGE_LEN * (MIN_NUM_KEYS - 1)) as u64;
        let cache_root = TempDir::new("puts_eviction").unwrap();
        let cache = DiskCache::initialize(cache_root.path().to_path_buf(), CAP).unwrap();
        let mut it = RandomEntryIterator::default();

        // fill the cache to almost capacity
        for _ in 0..MIN_NUM_KEYS {
            let (key, range, offsets, data) = it.next().unwrap();
            assert!(cache.put(&key, &range, &offsets, &data).is_ok());
        }
        assert!(cache.total_bytes().unwrap() <= CAP);

        let (key, range, offsets, data) = it.next().unwrap();
        let result = cache.put(&key, &range, &offsets, &data);
        if result.is_err() {
            println!("{result:?}");
        }
        assert!(result.is_ok());
        assert!(cache.total_bytes().unwrap() <= CAP);
    }

    #[test]
    fn test_same_puts_noop() {
        let cache_root = TempDir::new("same_puts_noop").unwrap();
        let cache =
            DiskCache::initialize(cache_root.path().to_path_buf(), DEFAULT_CAPACITY).unwrap();
        let mut it = RandomEntryIterator::default().with_range_len(1000);
        let (key, range, offsets, data) = it.next().unwrap();
        assert!(cache.put(&key, &range, &offsets, &data).is_ok());
        assert!(cache.put(&key, &range, &offsets, &data).is_ok());
    }

    #[test]
    fn test_overlap_range_data_mismatch_fail() {
        let setup = || {
            let mut it = RandomEntryIterator::default();
            let cache_root = TempDir::new("overlap_range_data_mismatch_fail").unwrap();
            let cache =
                DiskCache::initialize(cache_root.path().to_path_buf(), DEFAULT_CAPACITY).unwrap();
            let (key, range, offsets, data) = it.next().unwrap();
            assert!(cache.put(&key, &range, &offsets, &data).is_ok());
            (cache_root, cache, key, range, offsets, data)
        };
        // bad offsets
        // totally random, mismatch len from range
        let (_cache_root, cache, key, range, mut offsets, data) = setup();
        offsets.remove(1);
        assert!(cache.put(&key, &range, &offsets, &data).is_err());

        // start isn't 0
        let (_cache_root, cache, key, range, mut offsets, data) = setup();
        offsets[0] = 100;
        assert!(cache.put(&key, &range, &offsets, &data).is_err());

        // end isn't data.len()
        let (_cache_root, cache, key, range, mut offsets, data) = setup();
        *offsets.last_mut().unwrap() = data.len() as u32 + 1;
        assert!(cache.put(&key, &range, &offsets, &data).is_err());

        // not strictly increasing
        let (_cache_root, cache, key, range, mut offsets, data) = setup();
        offsets[2] = offsets[1];
        assert!(cache.put(&key, &range, &offsets, &data).is_err());

        // not matching
        let (_cache_root, cache, key, range, mut offsets, data) = setup();
        offsets[1] = offsets[1] + 1;
        assert!(cache.put(&key, &range, &offsets, &data).is_err());

        // bad data
        // size mismatch given offsets
        let (_cache_root, cache, key, range, offsets, data) = setup();
        assert!(cache.put(&key, &range, &offsets, &data[1..]).is_err());

        // data changed
        let (_cache_root, cache, key, range, offsets, mut data) = setup();
        data[0] = data[0] + 1;
        assert!(cache.put(&key, &range, &offsets, &data).is_err());
    }

    #[test]
    fn test_initialize_non_empty() {
        let cache_root = TempDir::new("initialize_non_empty").unwrap();
        let cache =
            DiskCache::initialize(cache_root.path().to_path_buf(), DEFAULT_CAPACITY).unwrap();
        let mut it = RandomEntryIterator::default();

        let mut keys_and_ranges = Vec::new();

        for _ in 0..20 {
            let (key, range, offsets, data) = it.next().unwrap();
            assert!(cache.put(&key, &range, &offsets, &data).is_ok());
            keys_and_ranges.push((key, range));
        }
        let cache2 =
            DiskCache::initialize(cache_root.path().to_path_buf(), DEFAULT_CAPACITY).unwrap();
        for (i, (key, range)) in keys_and_ranges.iter().enumerate() {
            let get_result = cache2.get(&key, &range);
            assert!(get_result.is_ok(), "{i} {get_result:?}");
            assert!(get_result.unwrap().is_some(), "{i}");
        }

        let cache_keys = cache
            .state
            .lock()
            .unwrap()
            .inner
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let cache2_keys = cache2
            .state
            .lock()
            .unwrap()
            .inner
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(cache_keys, cache2_keys);
    }

    #[test]
    fn test_initialize_too_large_file() {
        const LARGE_FILE: u64 = 1000;
        let cache_root = TempDir::new("initialize_too_large_file").unwrap();
        let cache =
            DiskCache::initialize(cache_root.path().to_path_buf(), DEFAULT_CAPACITY).unwrap();
        let mut it = RandomEntryIterator::default().with_range_len(LARGE_FILE as u32);

        let (key, range, offsets, data) = it.next().unwrap();
        cache.put(&key, &range, &offsets, &data).unwrap();
        let cache2 =
            DiskCache::initialize(cache_root.path().to_path_buf(), LARGE_FILE - 1).unwrap();

        assert_eq!(cache2.total_bytes().unwrap(), 0);
    }

    #[test]
    fn test_initialize_stops_loading_early_with_too_many_files() {
        const LARGE_FILE: u64 = 1000;
        let cache_root =
            TempDir::new("initialize_stops_loading_early_with_too_many_files").unwrap();
        let cache =
            DiskCache::initialize(cache_root.path().to_path_buf(), LARGE_FILE * 10).unwrap();
        let mut it = RandomEntryIterator::default().with_range_len(LARGE_FILE as u32);
        for _ in 0..10 {
            let (key, range, offsets, data) = it.next().unwrap();
            cache.put(&key, &range, &offsets, &data).unwrap();
        }

        let cap2 = LARGE_FILE * 2;
        let cache2 = DiskCache::initialize(cache_root.path().to_path_buf(), cap2).unwrap();

        assert!(
            cache2.total_bytes().unwrap() < cap2 * 3,
            "{} < {}",
            cache2.total_bytes().unwrap(),
            cap2 * 3
        );
    }

    #[test]
    fn test_dir_name_to_key() {
        let s = "oL-Xqk1J00kVe1U4kCko-Kw4zaVv3-4U73i27w5DViBkZWZhdWx0";
        let key = parse_key(s.as_bytes());
        assert!(key.is_ok(), "{key:?}")
    }

    #[test]
    fn test_unknown_eviction() {
        let cache_root = TempDir::new("initialize_non_empty").unwrap();
        let capacity = 12 * RANGE_LEN as u64;
        let cache = DiskCache::initialize(cache_root.path().to_path_buf(), capacity).unwrap();
        let mut it = RandomEntryIterator::default();
        let (key, range, chunk_byte_indicies, data) = it.next().unwrap();
        cache
            .put(&key, &range, &chunk_byte_indicies, &data)
            .unwrap();

        let cache2 = DiskCache::initialize(cache_root.path().to_path_buf(), capacity).unwrap();
        let get_result = cache2.get(&key, &range);
        assert!(get_result.is_ok());
        assert!(get_result.unwrap().is_some());

        let (key2, range2, chunk_byte_indicies2, data2) = it.next().unwrap();
        assert!(cache2
            .put(&key2, &range2, &chunk_byte_indicies2, &data2)
            .is_ok());

        let mut get_result_1 = cache2.get(&key, &range).unwrap();
        let mut i = 0;
        while get_result_1.is_some() && i < 50 {
            i += 1;
            let (key2, range2, chunk_byte_indicies2, data2) = it.next().unwrap();
            cache2
                .put(&key2, &range2, &chunk_byte_indicies2, &data2)
                .unwrap();
            get_result_1 = cache2.get(&key, &range).unwrap();
        }
        if get_result_1.is_some() {
            // randomness didn't evict the record after 50 tries, don't test this case now
            return;
        }
        // we've evicted the original record from the cache
        // note using the original cache handle without updates!
        let get_result_post_eviction = cache.get(&key, &range);
        assert!(get_result_post_eviction.is_ok());
        assert!(get_result_post_eviction.unwrap().is_none());
    }

    #[test]
    fn put_subrange() {
        let cache_root = TempDir::new("put_subrange").unwrap();
        let cache =
            DiskCache::initialize(cache_root.path().to_path_buf(), DEFAULT_CAPACITY).unwrap();
        let (key, range, chunk_byte_indicies, data) =
            RandomEntryIterator::default().next().unwrap();
        cache
            .put(&key, &range, &chunk_byte_indicies, &data)
            .unwrap();
        let total_bytes = cache.total_bytes().unwrap();

        // left range
        let left_range = Range {
            start: range.start,
            end: range.end - 1,
        };
        let left_chunk_byte_indicies = &chunk_byte_indicies[..chunk_byte_indicies.len() - 1];
        let left_data = &data[..*left_chunk_byte_indicies.last().unwrap() as usize];
        assert!(cache
            .put(&key, &left_range, left_chunk_byte_indicies, left_data)
            .is_ok());
        assert_eq!(total_bytes, cache.total_bytes().unwrap());

        // right range
        let right_range = Range {
            start: range.start + 1,
            end: range.end,
        };
        let right_chunk_byte_indicies: Vec<u32> = (&chunk_byte_indicies[1..])
            .iter()
            .map(|v| v - chunk_byte_indicies[1])
            .collect();
        let right_data = &data[chunk_byte_indicies[1] as usize..];
        assert!(cache
            .put(&key, &right_range, &right_chunk_byte_indicies, right_data)
            .is_ok());
        assert_eq!(total_bytes, cache.total_bytes().unwrap());

        // middle range
        let middle_range = Range {
            start: range.start + 1,
            end: range.end - 1,
        };
        let middle_chunk_byte_indicies: Vec<u32> = (&chunk_byte_indicies
            [1..(chunk_byte_indicies.len() - 1)])
            .iter()
            .map(|v| v - chunk_byte_indicies[1])
            .collect();
        let middle_data = &data[chunk_byte_indicies[1] as usize
            ..chunk_byte_indicies[chunk_byte_indicies.len() - 2] as usize];

        assert!(cache
            .put(
                &key,
                &middle_range,
                &middle_chunk_byte_indicies,
                middle_data
            )
            .is_ok());
        assert_eq!(total_bytes, cache.total_bytes().unwrap());
    }

    #[test]
    fn test_evictions_with_multiple_range_per_key() {
        const NUM: u32 = 12;
        let cache_root = TempDir::new("multiple_range_per_key").unwrap();
        let capacity = (NUM * RANGE_LEN) as u64;
        let cache = DiskCache::initialize(cache_root.path().to_path_buf(), capacity).unwrap();
        let mut it = RandomEntryIterator::default().with_one_chunk_ranges(true);
        let (key, _, _, _) = it.next().unwrap();
        let mut previously_put: Vec<(Key, Range)> = Vec::new();

        for _ in 0..(NUM / 2) {
            let (key2, mut range, chunk_byte_indicies, data) = it.next().unwrap();
            while previously_put.iter().any(|(_, r)| r.start == range.start) {
                range.start += 1 % 1000;
            }
            cache
                .put(&key, &range, &chunk_byte_indicies, &data)
                .unwrap();
            previously_put.push((key.clone(), range.clone()));
            cache
                .put(&key2, &range, &chunk_byte_indicies, &data)
                .unwrap();
            previously_put.push((key2, range));
        }

        let mut num_hits = 0;
        for (key, range) in &previously_put {
            let result = cache.get(key, range);
            assert!(result.is_ok());
            let result = result.unwrap();
            if result.is_some() {
                num_hits += 1;
            }
        }
        // assert got some hits, exact number depends on item size
        assert_ne!(num_hits, 0);

        // assert that we haven't evicted all keys for key with multiple items
        assert!(
            cache.state.lock().unwrap().inner.contains_key(&key),
            "evicted key that should have remained in cache"
        );
    }
}

#[cfg(test)]
mod concurrency_tests {
    use tempdir::TempDir;

    use crate::{disk::DEFAULT_CAPACITY, ChunkCache, RandomEntryIterator, RANGE_LEN};

    use super::DiskCache;

    const NUM_ITEMS_PER_TASK: usize = 20;

    #[tokio::test]
    async fn test_run_concurrently() {
        let cache_root = TempDir::new("run_concurrently").unwrap();
        let cache =
            DiskCache::initialize(cache_root.path().to_path_buf(), DEFAULT_CAPACITY).unwrap();

        let num_tasks = 2 + rand::random::<u8>() % 14;

        let mut handles = Vec::with_capacity(num_tasks as usize);
        for _ in 0..num_tasks {
            let cache_clone = cache.clone();
            handles.push(tokio::spawn(async move {
                let mut it = RandomEntryIterator::default();
                let mut kr = Vec::with_capacity(NUM_ITEMS_PER_TASK);
                for _ in 0..NUM_ITEMS_PER_TASK {
                    let (key, range, chunk_byte_indicies, data) = it.next().unwrap();
                    assert!(cache_clone
                        .put(&key, &range, &chunk_byte_indicies, &data)
                        .is_ok());
                    kr.push((key, range));
                }
                for (key, range) in kr {
                    assert!(cache_clone.get(&key, &range).is_ok());
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
        let cache = DiskCache::initialize(
            cache_root.path().to_path_buf(),
            RANGE_LEN as u64 * NUM_ITEMS_PER_TASK as u64,
        )
        .unwrap();

        let num_tasks = 2 + rand::random::<u8>() % 14;

        let mut handles = Vec::with_capacity(num_tasks as usize);
        for _ in 0..num_tasks {
            let cache_clone = cache.clone();
            handles.push(tokio::spawn(async move {
                let mut it = RandomEntryIterator::default();
                let mut kr = Vec::with_capacity(NUM_ITEMS_PER_TASK);
                for _ in 0..NUM_ITEMS_PER_TASK {
                    let (key, range, chunk_byte_indicies, data) = it.next().unwrap();
                    assert!(cache_clone
                        .put(&key, &range, &chunk_byte_indicies, &data)
                        .is_ok());
                    kr.push((key, range));
                }
                for (key, range) in kr {
                    assert!(cache_clone.get(&key, &range).is_ok());
                }
            }))
        }

        for handle in handles {
            handle.await.expect("join should not error");
        }
    }
}
