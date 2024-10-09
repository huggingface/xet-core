use std::{
    collections::HashMap,
    fs::File,
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    mem::size_of,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use base64::engine::general_purpose::URL_SAFE;
use base64::{engine::GeneralPurpose, Engine};
use cache_file_header::CacheFileHeader;
use cache_item::{range_contained_fn, CacheItem};
use cas_types::{Key, Range};
use file_utils::SafeFileCreator;
use merklehash::MerkleHash;
use sorted_vec::SortedVec;
use tracing::{error, warn};

use crate::{error::ChunkCacheError, ChunkCache};

mod cache_file_header;
mod cache_item;
pub mod test_utils;

// consistently use URL_SAFE (also file path safe) base64 codec
pub(crate) const BASE64_ENGINE: GeneralPurpose = URL_SAFE;
pub const _DEFAULT_CAPACITY: u64 = 1 << 30; // 1 GB

type CacheState = HashMap<Key, SortedVec<CacheItem>>;

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
    // TODO: memoize
    pub fn num_items(&self) -> Result<usize, ChunkCacheError> {
        let state = self.state.lock()?;
        let value = num_items(&state);
        Ok(value)
    }

    // TODO: memoize
    #[allow(dead_code)]
    fn total_bytes(&self) -> Result<u64, ChunkCacheError> {
        let state = self.state.lock()?;
        let value = total_bytes(&state);
        Ok(value)
    }

    pub fn initialize(cache_root: PathBuf, capacity: u64) -> Result<Self, ChunkCacheError> {
        let mut state = HashMap::new();
        let mut num_bytes = 0;
        let max_num_bytes = 2 * capacity;

        let readdir = match std::fs::read_dir(&cache_root) {
            Ok(rd) => rd,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    return Ok(Self {
                        cache_root,
                        capacity,
                        state: Arc::new(Mutex::new(state)),
                    });
                }
                return Err(e.into());
            }
        };

        for key_dir in readdir {
            let key_dir = match key_dir {
                Ok(kd) => kd,
                Err(_) => continue,
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
                warn!(
                    "CACHE: expected key directory at {:?}, is not directory",
                    key_dir.path()
                );
                continue;
            }
            let key = match parse_key(key_dir.file_name().as_encoded_bytes()) {
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

                let cache_item = match CacheItem::parse(item.file_name().as_encoded_bytes()) {
                    Ok(i) => i,
                    Err(e) => {
                        warn!(
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

                num_bytes += cache_item.len;
                items.push(cache_item);

                if num_bytes >= max_num_bytes {
                    break;
                }
            }

            if !items.is_empty() {
                state.insert(key, items);
            }

            if num_bytes >= max_num_bytes {
                break;
            }
        }

        Ok(Self {
            state: Arc::new(Mutex::new(state)),
            cache_root,
            capacity,
        })
    }

    fn get_impl(&self, key: &Key, range: &Range) -> Result<Option<Vec<u8>>, ChunkCacheError> {
        if range.start >= range.end {
            return Err(ChunkCacheError::InvalidArguments);
        }

        let mut state = self.state.lock()?;
        let items = if let Some(items) = state.get_mut(key) {
            items
        } else {
            // no entry matched for key
            return Ok(None);
        };

        let (mut file, header, start) = loop {
            // attempt to find a matching range in the given key's items using binary search
            let idx = items.binary_search_by(range_contained_fn(range));
            if idx.is_err() {
                // no matching range for this key
                return Ok(None);
            }
            let idx = idx.expect("already checked for error case");
            let item = items.get(idx).ok_or(ChunkCacheError::Infallible)?;
            let file_name = item.to_file_name()?;
            let path = self.cache_root.join(key_dir(key)).join(file_name);

            let mut file = match File::open(&path) {
                Ok(file) => file,
                Err(e) => match e.kind() {
                    ErrorKind::NotFound => {
                        items.remove_index(idx);
                        continue;
                    }
                    _ => return Err(e.into()),
                },
            };
            let hash = blake3::Hasher::new().update_reader(&mut file)?.finalize();
            if hash != item.hash {
                error!("file hash mismatch on path: {path:?}, key: {key}, item: {item}");
                items.remove_index(idx);
                remove_file(&path)?;
                continue;
            }

            file.seek(SeekFrom::Start(0))?;
            let header = match CacheFileHeader::deserialize(&mut file) {
                Ok(header) => header,
                Err(_) => {
                    items.remove_index(idx);
                    remove_file(&path)?;
                    continue;
                }
            };

            let start = item.range.start;
            break (file, header, start);
        };
        drop(state);
        let start_byte = header
            .chunk_byte_indicies
            .get((range.start - start) as usize)
            .ok_or(ChunkCacheError::BadRange)?;
        let end_byte = header
            .chunk_byte_indicies
            .get((range.end - start) as usize)
            .ok_or(ChunkCacheError::BadRange)?;
        file.seek(SeekFrom::Start(
            (*start_byte as usize + header.header_len()) as u64,
        ))?;
        let mut buf = vec![0; (end_byte - start_byte) as usize];
        file.read_exact(&mut buf)?;
        Ok(Some(buf))
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
        {
            return Err(ChunkCacheError::InvalidArguments);
        }

        // check if we already contain the range
        if let Some(item_set) = self.state.lock()?.get_mut(key) {
            while let Ok(idx) = item_set.binary_search_by(range_contained_fn(range)) {
                let item = item_set.get(idx).ok_or(ChunkCacheError::Infallible)?;
                let path = self
                    .cache_root
                    .join(key_dir(key))
                    .join(item.to_file_name()?);
                match File::open(&path) {
                    Ok(mut file) => {
                        let hash = blake3::Hasher::new().update_reader(&mut file)?.finalize();
                        if hash == item.hash && file.metadata()?.len() == item.len {
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        if e.kind() == ErrorKind::NotFound {
                            item_set.remove_index(idx);
                        } else {
                            remove_file(path)?;
                            // should remove directory if empty as well as item set
                        }
                    }
                }
            }
        }

        let header = CacheFileHeader::new(chunk_byte_indicies);
        let mut header_buf = Vec::with_capacity(header.header_len());
        header.serialize(&mut header_buf)?;
        let hash = blake3::Hasher::new()
            .update(&header_buf)
            .update(data)
            .finalize();

        let item = CacheItem {
            range: range.clone(),
            len: (header_buf.len() + data.len()) as u64,
            hash,
        };

        self.maybe_evict(item.len)?;

        let path = self
            .cache_root
            .join(key_dir(key))
            .join(item.to_file_name()?);

        let mut fw = SafeFileCreator::new(path)?;

        fw.write_all(&header_buf)?;
        fw.write_all(data)?;
        fw.close()?;

        let mut state = self.state.lock()?;
        let item_set = state.entry(key.clone()).or_default();
        item_set.insert(item);

        Ok(())
    }

    /// removed items from the cache (including deleting from file system)
    /// until at least to_remove number of bytes have been removed
    fn maybe_evict(&self, want_to_add: u64) -> Result<(), ChunkCacheError> {
        let mut state = self.state.lock()?;
        let total_bytes = total_bytes(&state);
        let to_remove = total_bytes as i64 - self.capacity as i64 + want_to_add as i64;
        let mut bytes_removed = 0;
        while to_remove > bytes_removed {
            let (key, idx) = self.random_item(&state);
            let items = state.get_mut(&key).ok_or(ChunkCacheError::Infallible)?;
            let item = &items[idx];
            let len = item.len;
            let path = self
                .cache_root
                .join(key_dir(&key))
                .join(item.to_file_name()?);
            remove_file(path)?;
            items.remove_index(idx);
            if items.is_empty() {
                state.remove(&key);

                let dir_path = self.cache_root.join(key_dir(&key));
                // check if directory exists and if it does and is empty then remove the directory
                if let Ok(mut readdir) = std::fs::read_dir(&dir_path) {
                    if readdir.next().is_none() {
                        // no more files in that directory, remove it
                        remove_dir(dir_path)?;
                    }
                }
            }

            bytes_removed += len as i64;
        }
        Ok(())
    }

    /// returns the key and index within that key for a random item
    fn random_item(&self, state: &MutexGuard<'_, CacheState>) -> (Key, usize) {
        let num_items = num_items(state);
        let random_item = rand::random::<usize>() % num_items;
        let mut count = 0;
        for (key, items) in state.iter() {
            if random_item < count + items.len() {
                return (key.clone(), random_item - count);
            }
            count += items.len();
        }

        panic!("should have returned")
    }
}

fn total_bytes(state: &CacheState) -> u64 {
    state
        .values()
        .fold(0, |acc, v| acc + v.iter().fold(0, |acc, v| acc + v.len))
}

fn num_items(state: &CacheState) -> usize {
    state.values().fold(0, |acc, v| acc + v.len())
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

    use cas_types::Range;
    use rand::thread_rng;
    use tempdir::TempDir;

    use crate::ChunkCache;

    use super::{DiskCache, _DEFAULT_CAPACITY};

    #[test]
    fn test_get_cache_empty() {
        let mut rng = thread_rng();
        let cache_root = TempDir::new("empty").unwrap();
        let cache = DiskCache::initialize(cache_root.into_path(), _DEFAULT_CAPACITY).unwrap();
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
            DiskCache::initialize(cache_root.path().to_path_buf(), _DEFAULT_CAPACITY).unwrap();

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
            DiskCache::initialize(cache_root.path().to_path_buf(), _DEFAULT_CAPACITY).unwrap();

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
        const CAP: u64 = (RANGE_LEN * 4) as u64;
        let cache_root = TempDir::new("puts_eviction").unwrap();
        let cache = DiskCache::initialize(cache_root.path().to_path_buf(), CAP).unwrap();
        let mut it = RandomEntryIterator::default();

        // fill the cache to almost capacity
        for _ in 0..3 {
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
        let cache_root = TempDir::new("puts_eviction").unwrap();
        let cache =
            DiskCache::initialize(cache_root.path().to_path_buf(), _DEFAULT_CAPACITY).unwrap();
        let mut it = RandomEntryIterator::default();
        let (key, range, offsets, data) = it.next().unwrap();
        assert!(cache.put(&key, &range, &offsets, &data).is_ok());

        assert!(cache.put(&key, &range, &offsets, &data).is_ok());
    }

    #[test]
    fn test_initialize_non_empty() {
        let cache_root = TempDir::new("initialize_non_empty").unwrap();
        let cache =
            DiskCache::initialize(cache_root.path().to_path_buf(), _DEFAULT_CAPACITY).unwrap();
        let mut it = RandomEntryIterator::default();

        let mut keys_and_ranges = Vec::new();

        for _ in 0..20 {
            let (key, range, offsets, data) = it.next().unwrap();
            assert!(cache.put(&key, &range, &offsets, &data).is_ok());
            keys_and_ranges.push((key, range));
        }
        let cache2 =
            DiskCache::initialize(cache_root.path().to_path_buf(), _DEFAULT_CAPACITY).unwrap();
        for (i, (key, range)) in keys_and_ranges.iter().enumerate() {
            let get_result = cache2.get(&key, &range);
            assert!(get_result.is_ok(), "{i} {get_result:?}");
            assert!(get_result.unwrap().is_some(), "{i}");
        }

        let cache_keys = cache
            .state
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let cache2_keys = cache2
            .state
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(cache_keys, cache2_keys);
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
        let capacity = 2 * RANGE_LEN as u64;
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
        let get_result_2 = cache2.get(&key2, &range2).unwrap();
        assert!(get_result_1.is_some() != get_result_2.is_some());
        let mut i = 0;
        while get_result_1.is_some() && i < 10 {
            i += 1;
            let (key2, range2, chunk_byte_indicies2, data2) = it.next().unwrap();
            cache2
                .put(&key2, &range2, &chunk_byte_indicies2, &data2)
                .unwrap();
            get_result_1 = cache2.get(&key, &range).unwrap();
        }
        if get_result_1.is_some() {
            // randomness didn't evict the record after 10 tries, don't test this case now
            return;
        }
        // we've evicted the original record from the cache
        // note using the original cache handle without updates!
        let get_result_post_eviction = cache.get(&key, &range);
        assert!(get_result_post_eviction.is_ok());
        assert!(get_result_post_eviction.unwrap().is_none());
    }

    #[test]
    fn test_evictions_with_multiple_range_per_key() {
        let cache_root = TempDir::new("multiple_range_per_key").unwrap();
        let capacity = 5 * RANGE_LEN as u64;
        let cache = DiskCache::initialize(cache_root.path().to_path_buf(), capacity).unwrap();
        let mut it = RandomEntryIterator::default();
        let (key, _, _, _) = it.next().unwrap();
        let mut previously_put = Vec::new();
        for _ in 0..3 {
            let (key2, range, chunk_byte_indicies, data) = it.next().unwrap();
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
            cache.state.lock().unwrap().contains_key(&key),
            "evicted key that should have remained in cache"
        );
    }
}

#[cfg(test)]
mod concurrency_tests {
    use tempdir::TempDir;

    use crate::{disk::_DEFAULT_CAPACITY, ChunkCache, RandomEntryIterator, RANGE_LEN};

    use super::DiskCache;

    const NUM_ITEMS_PER_TASK: usize = 10;

    #[tokio::test]
    async fn test_run_concurrently() {
        let cache_root = TempDir::new("run_concurrently").unwrap();
        let cache =
            DiskCache::initialize(cache_root.path().to_path_buf(), _DEFAULT_CAPACITY).unwrap();

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
