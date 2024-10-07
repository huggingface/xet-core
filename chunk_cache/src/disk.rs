use std::{
    cmp::Ordering,
    collections::HashMap,
    fs::{read_dir, DirEntry, File},
    io::{Cursor, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use base64::engine::general_purpose::URL_SAFE;
use base64::{engine::GeneralPurpose, Engine};
use blake3::Hash;
use cache_file_header::CacheFileHeader;
use cas_types::{Key, Range};
use file_utils::SafeFileCreator;
use merklehash::MerkleHash;
use sorted_vec::SortedVec;
use tracing::warn;

use crate::{error::ChunkCacheError, ChunkCache};

mod cache_file_header;

const BASE64_ENGINE: GeneralPurpose = URL_SAFE;

pub struct DiskCache {
    cache_root: PathBuf,
    capacity: u64,
    state: HashMap<Key, SortedVec<CacheItem>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CacheItem {
    range: Range,
    len: u64,
    hash: Hash,
}

impl PartialOrd for CacheItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CacheItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.range.cmp(&other.range)
    }
}

const CACHE_ITEM_FILE_NAME_BUF_SIZE: usize = size_of::<u32>() * 2 + blake3::OUT_LEN;

impl CacheItem {
    fn to_file_name(&self) -> Result<String, ChunkCacheError> {
        let mut buf = [0u8; CACHE_ITEM_FILE_NAME_BUF_SIZE];
        let mut w = Cursor::new(&mut buf[..]);
        write_u32(&mut w, self.range.start)?;
        write_u32(&mut w, self.range.end)?;
        write_hash(&mut w, &self.hash)?;
        Ok(BASE64_ENGINE.encode(buf))
    }
}

fn parse_cache_item(item: &DirEntry) -> Result<CacheItem, ChunkCacheError> {
    let md = item.metadata()?;
    if !md.is_file() {
        warn!("expected file at path: {:?}", item.path());
        return Err(ChunkCacheError::parse("not a file"));
    }
    let len = md.len();
    let buf = BASE64_ENGINE.decode(item.file_name().as_encoded_bytes())?;
    let mut r = Cursor::new(buf);
    let start = read_u32(&mut r)?;
    let end = read_u32(&mut r)?;
    let hash = read_hash(&mut r)?;
    Ok(CacheItem {
        range: Range { start, end },
        len,
        hash,
    })
}

fn parse_key(file_name: &[u8]) -> Result<Key, ChunkCacheError> {
    let buf = BASE64_ENGINE.decode(file_name)?;
    let hash = MerkleHash::from_slice(&buf[..size_of::<MerkleHash>()])?;
    let prefix = String::from(std::str::from_utf8(&buf[size_of::<MerkleHash>()..])?);
    Ok(Key { prefix, hash })
}

pub fn read_hash(reader: &mut impl Read) -> Result<blake3::Hash, std::io::Error> {
    let mut m = [0u8; 32];
    reader.read_exact(&mut m)?;
    Ok(blake3::Hash::from_bytes(m))
}

pub fn read_u32(reader: &mut impl Read) -> Result<u32, std::io::Error> {
    let mut buf = [0u8; size_of::<u32>()];
    reader.read_exact(&mut buf[..])?;
    Ok(u32::from_le_bytes(buf))
}

pub fn write_hash(writer: &mut impl Write, hash: &blake3::Hash) -> Result<(), std::io::Error> {
    writer.write_all(hash.as_bytes())
}

pub fn write_u32(writer: &mut impl Write, v: u32) -> Result<(), std::io::Error> {
    writer.write_all(&v.to_le_bytes())
}

fn range_contained_fn(range: &Range) -> impl FnMut(&CacheItem) -> std::cmp::Ordering + '_ {
    |item: &CacheItem| {
        if item.range.start > range.start {
            Ordering::Greater
        } else if item.range.end < range.end {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }
}

impl DiskCache {
    fn num_items(&self) -> usize {
        self.state.values().fold(0, |acc, v| acc + v.len())
    }

    fn total_bytes(&self) -> u64 {
        self.state
            .values()
            .fold(0, |acc, v| acc + v.iter().fold(0, |acc, v| acc + v.len))
    }

    pub fn initialize(cache_root: PathBuf, capacity: u64) -> Result<Self, ChunkCacheError> {
        let mut state = HashMap::new();

        for key_dir in read_dir(&cache_root)? {
            let key_dir = key_dir?;
            if !key_dir.metadata()?.is_dir() {
                warn!(
                    "CACHE: expected key directory at {:?}, is not directory",
                    key_dir.path()
                );
                continue;
            }
            let key = parse_key(key_dir.file_name().as_encoded_bytes())?;
            let mut items = SortedVec::new();

            for item in read_dir(key_dir.path())? {
                let item = item?;
                let cache_item = match parse_cache_item(&item) {
                    Ok(i) => i,
                    Err(e) => {
                        warn!(
                            "error parsing cache item file info from path: {:?}, {e}",
                            item.path()
                        );
                        continue;
                    }
                };
                items.push(cache_item);
            }
            if !items.is_empty() {
                state.insert(key, items);
            }
        }

        Ok(Self {
            state,
            cache_root,
            capacity,
        })
    }

    fn get_impl(&mut self, key: &Key, range: &Range) -> Result<Option<Vec<u8>>, ChunkCacheError> {
        let items = if let Some(items) = self.state.get_mut(key) {
            items
        } else {
            return Ok(None);
        };

        loop {
            let idx = items.binary_search_by(range_contained_fn(range));
            if idx.is_err() {
                return Ok(None);
            }
            let idx = idx.expect("already checked for error case");
            let item = items.get(idx).ok_or(ChunkCacheError::Infallible)?.clone();
            let file_name = item.to_file_name()?;
            let path = self.cache_root.join(key_dir(key)).join(file_name);

            let mut file = match File::open(&path) {
                Ok(file) => file,
                Err(e) => match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        items.remove_index(idx);
                        continue;
                    }
                    _ => return Err(e.into()),
                },
            };
            let header = match CacheFileHeader::deserialize(&mut file) {
                Ok(header) => header,
                Err(_) => {
                    items.remove_index(idx);
                    std::fs::remove_file(&path)?;
                    continue;
                }
            };
            let start_byte = header
                .chunk_byte_indicies
                .get((range.start - item.range.start) as usize)
                .ok_or(ChunkCacheError::BadRange)?;
            let end_byte = header
                .chunk_byte_indicies
                .get((range.end - item.range.start) as usize)
                .ok_or(ChunkCacheError::BadRange)?;
            file.seek(SeekFrom::Start(
                (*start_byte as usize + header.header_len) as u64,
            ))?;
            let mut buf = vec![0; (end_byte - start_byte) as usize];
            file.read_exact(&mut buf)?;
            return Ok(Some(buf));
        }
    }

    fn put_impl(
        &mut self,
        key: &Key,
        range: &Range,
        chunk_byte_indicies: &[u32],
        data: &[u8],
    ) -> Result<(), ChunkCacheError> {
        let total_bytes = self.total_bytes();
        {
            // check if we already contain the range
            if let Some(item_set) = self.state.get(key) {
                if item_set.binary_search_by(range_contained_fn(range)).is_ok() {
                    return Ok(());
                }
            }
        }

        let header = CacheFileHeader::new(chunk_byte_indicies);
        let mut header_buf = Vec::with_capacity(header.header_len);
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

        if self.capacity < total_bytes + item.len {
            self.evict(total_bytes + item.len - self.capacity)?;
        }

        let item_set = self.state.entry(key.clone()).or_default();

        let path = self
            .cache_root
            .join(key_dir(key))
            .join(item.to_file_name()?);

        let mut fw = SafeFileCreator::new(path)?;

        fw.write_all(&header_buf)?;
        fw.write_all(data)?;
        fw.close()?;

        item_set.insert(item);

        Ok(())
    }

    fn evict(&mut self, to_remove: u64) -> Result<(), ChunkCacheError> {
        let mut bytes_removed = 0;
        while to_remove > bytes_removed {
            let (key, idx) = self.random_item();
            let items = self
                .state
                .get_mut(&key)
                .ok_or(ChunkCacheError::Infallible)?;
            let item = &items[idx];
            let len = item.len;
            let path = self
                .cache_root
                .join(key_dir(&key))
                .join(item.to_file_name()?);
            std::fs::remove_file(path)?;
            items.remove_index(idx);
            if items.is_empty() {
                self.state.remove(&key);

                let dir_path = self.cache_root.join(key_dir(&key));
                if std::fs::read_dir(&dir_path)?.next().is_none() {
                    // no more files in that directory, remove it
                    std::fs::remove_dir(dir_path)?;
                }
            }

            bytes_removed += len;
        }
        Ok(())
    }

    /// returns the key and index within that key for a random item
    fn random_item(&self) -> (Key, usize) {
        let num_items = self.num_items();
        let random_item = rand::random::<usize>() % num_items;
        let mut count = 0;
        for (key, items) in &self.state {
            if random_item < count + items.len() {
                return (key.clone(), random_item - count);
            }
            count += items.len();
        }

        panic!("should have returned")
    }
}

fn key_dir(key: &Key) -> String {
    let prefix_bytes = key.prefix.as_bytes();
    let mut buf = vec![0u8; size_of::<MerkleHash>() + prefix_bytes.len()];
    buf[..size_of::<MerkleHash>()].copy_from_slice(key.hash.as_bytes());
    buf[size_of::<MerkleHash>()..].copy_from_slice(prefix_bytes);
    BASE64_ENGINE.encode(buf)
}

impl ChunkCache for DiskCache {
    fn get(&mut self, key: &Key, range: &Range) -> Result<Option<Vec<u8>>, ChunkCacheError> {
        self.get_impl(key, range)
    }

    fn put(
        &mut self,
        key: &Key,
        range: &Range,
        chunk_byte_indicies: &[u32],
        data: &[u8],
    ) -> Result<(), ChunkCacheError> {
        self.put_impl(key, range, chunk_byte_indicies, data)
    }
}

#[cfg(test)]
mod test_utils {
    use std::path::Path;

    use cas_types::{Key, Range};
    use merklehash::MerkleHash;
    use rand::Rng;

    pub const DEFAULT_CAPACITY: u64 = 16 << 20;
    pub const RANGE_LEN: u32 = 4000;

    pub fn print_directory_contents(path: &Path) {
        // Read the contents of the directory
        match std::fs::read_dir(path) {
            Ok(entries) => {
                for entry in entries {
                    match entry {
                        Ok(entry) => {
                            let path = entry.path();
                            // Print the path
                            println!("{}", path.display());

                            // If it's a directory, call this function recursively
                            if path.is_dir() {
                                print_directory_contents(&path);
                            }
                        }
                        Err(e) => eprintln!("Error reading entry: {}", e),
                    }
                }
            }
            Err(e) => eprintln!("Error reading directory: {}", e),
        }
    }

    pub fn random_key() -> Key {
        Key {
            prefix: "default".to_string(),
            hash: MerkleHash::from_slice(&rand::random::<[u8; 32]>()).unwrap(),
        }
    }

    pub fn random_range() -> Range {
        let start = rand::random::<u32>() % 1024;
        let end = 1024.min(start + rand::random::<u32>() % 256);
        Range { start, end }
    }

    pub fn random_bytes(range: &Range) -> (Vec<u32>, Vec<u8>) {
        let mut rng = rand::thread_rng();
        let random_vec: Vec<u8> = (0..RANGE_LEN).map(|_| rng.gen()).collect();
        let mut offsets: Vec<u32> = Vec::with_capacity((range.end - range.start + 1) as usize);
        offsets.push(0);
        for _ in range.start..range.end - 1 {
            let mut num = rng.gen::<u32>() % RANGE_LEN;
            while offsets.contains(&num) {
                num = rng.gen::<u32>() % RANGE_LEN;
            }
            offsets.push(num);
        }
        offsets.push(4000);
        offsets.sort();
        (offsets, random_vec)
    }

    pub struct RandomEntryIterator;

    impl Iterator for RandomEntryIterator {
        type Item = (Key, Range, Vec<u32>, Vec<u8>);

        fn next(&mut self) -> Option<Self::Item> {
            let key = random_key();
            let range = random_range();
            let (offsets, data) = random_bytes(&range);
            Some((key, range, offsets, data))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::disk::{parse_key, test_utils::*};

    use cas_types::Range;
    use tempdir::TempDir;

    use crate::ChunkCache;

    use super::DiskCache;

    #[test]
    fn test_get_cache_empty() {
        let cache_root = TempDir::new("empty").unwrap();
        let mut cache = DiskCache::initialize(cache_root.into_path(), DEFAULT_CAPACITY).unwrap();
        assert!(cache.get(&random_key(), &random_range()).unwrap().is_none());
    }

    #[test]
    fn test_put_get_simple() {
        let cache_root = TempDir::new("put_get_simple").unwrap();
        let mut cache =
            DiskCache::initialize(cache_root.path().to_path_buf(), DEFAULT_CAPACITY).unwrap();

        let key = random_key();
        let range = Range { start: 0, end: 4 };
        let (chunk_byte_indicies, data) = random_bytes(&range);
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
        let cache_root = TempDir::new("put_get_subrange").unwrap();
        let mut cache =
            DiskCache::initialize(cache_root.path().to_path_buf(), DEFAULT_CAPACITY).unwrap();

        let key = random_key();
        let range = Range { start: 0, end: 4 };
        let (chunk_byte_indicies, data) = random_bytes(&range);
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
        for i in 0..10 {
            let cache_root = TempDir::new(format!("puts_eviction{i}").as_str()).unwrap();
            let mut cache = DiskCache::initialize(cache_root.path().to_path_buf(), CAP).unwrap();

            // fill the cache to almost capacity
            for _ in 0..3 {
                let (key, range, offsets, data) = RandomEntryIterator.next().unwrap();
                assert!(cache.put(&key, &range, &offsets, &data).is_ok());
            }
            assert!(cache.total_bytes() <= CAP);

            let (key, range, offsets, data) = RandomEntryIterator.next().unwrap();
            let result = cache.put(&key, &range, &offsets, &data);
            if result.is_err() {
                println!("{result:?}");
            }
            assert!(result.is_ok());
            assert!(cache.total_bytes() <= CAP);
        }
    }

    #[test]
    fn test_same_puts_noop() {
        let cache_root = TempDir::new("puts_eviction").unwrap();
        let mut cache =
            DiskCache::initialize(cache_root.path().to_path_buf(), DEFAULT_CAPACITY).unwrap();
        let (key, range, offsets, data) = RandomEntryIterator.next().unwrap();
        assert!(cache.put(&key, &range, &offsets, &data).is_ok());

        assert!(cache.put(&key, &range, &offsets, &data).is_ok());
    }

    #[test]
    fn test_initialize_non_empty() {
        let cache_root = TempDir::new("initialize_non_empty").unwrap();
        let mut cache =
            DiskCache::initialize(cache_root.path().to_path_buf(), DEFAULT_CAPACITY).unwrap();

        let mut keys_and_ranges = Vec::new();

        for _ in 0..20 {
            let (key, range, offsets, data) = RandomEntryIterator.next().unwrap();
            assert!(cache.put(&key, &range, &offsets, &data).is_ok());
            keys_and_ranges.push((key, range));
        }
        let mut cache2 =
            DiskCache::initialize(cache_root.path().to_path_buf(), DEFAULT_CAPACITY).unwrap();
        for (i, (key, range)) in keys_and_ranges.iter().enumerate() {
            let get_result = cache2.get(&key, &range);
            assert!(get_result.is_ok(), "{i} {get_result:?}");
            println!(
                "{key} {range:?} {:?}\n{:?}",
                cache2.state.get(key),
                cache2.state
            );
            assert!(get_result.unwrap().is_some(), "{i}");
        }

        let cache_keys = cache.state.keys().collect::<BTreeSet<_>>();
        let cache2_keys = cache2.state.keys().collect::<BTreeSet<_>>();
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
        let mut cache = DiskCache::initialize(cache_root.path().to_path_buf(), capacity).unwrap();
        let (key, range, chunk_byte_indicies, data) = RandomEntryIterator.next().unwrap();
        cache
            .put(&key, &range, &chunk_byte_indicies, &data)
            .unwrap();

        let mut cache2 = DiskCache::initialize(cache_root.path().to_path_buf(), capacity).unwrap();
        let get_result = cache2.get(&key, &range);
        assert!(get_result.is_ok());
        assert!(get_result.unwrap().is_some());

        let (key2, range2, chunk_byte_indicies2, data2) = RandomEntryIterator.next().unwrap();
        assert!(cache2
            .put(&key2, &range2, &chunk_byte_indicies2, &data2)
            .is_ok());

        let mut get_result_1 = cache2.get(&key, &range).unwrap();
        let get_result_2 = cache2.get(&key2, &range2).unwrap();
        assert!(get_result_1.is_some() != get_result_2.is_some());
        let mut i = 0;
        while get_result_1.is_some() && i < 10 {
            i += 1;
            let (key2, range2, chunk_byte_indicies2, data2) = RandomEntryIterator.next().unwrap();
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
        let cache_root = TempDir::new("initialize_non_empty").unwrap();
        let capacity = 5 * RANGE_LEN as u64;
        let mut cache = DiskCache::initialize(cache_root.path().to_path_buf(), capacity).unwrap();
        let key = random_key();
        let mut previously_put = Vec::new();
        for _ in 0..3 {
            let (key2, range, chunk_byte_indicies, data) = RandomEntryIterator.next().unwrap();
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

        // expecting 3 or 4 hits, depends how much the chunk headers contribute to the total len
        assert!(num_hits == 3 || num_hits == 4, "{num_hits} is not 3 or 4");
        assert!(
            cache.state.contains_key(&key),
            "evicted key that should have remained in cache"
        );
    }
}
