use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, Write},
    mem::size_of,
    path::{Path, PathBuf},
    time::SystemTime,
};

use base64::{engine::GeneralPurpose, prelude::BASE64_URL_SAFE, Engine};
use cache_file_header::CacheFileHeader;
use cas_types::{Key, Range};
use error_printer::ErrorPrinter;
use file_name::FileName;
use merklehash::MerkleHash;
use rand::{seq::IteratorRandom, thread_rng};
use tracing::warn;

use crate::{error::ChunkCacheError, ChunkCache};

mod cache_file_header;
mod file_name;

const BASE64_ENGINE: GeneralPurpose = BASE64_URL_SAFE;

#[derive(Debug, Clone)]
pub struct DiskCache {
    cache_root: PathBuf,
    total_bytes: u64,
    items: HashMap<(Key, u32, u32), (FileName, u64)>,
    capacity: Option<u64>,
}

impl DiskCache {
    pub fn initialize<T: Into<PathBuf>>(
        cache_root: T,
        capacity: Option<u64>,
    ) -> Result<Self, ChunkCacheError> {
        let cache_root = cache_root.into();
        let mut total_bytes = 0;
        let mut items = HashMap::new();
        for subdir in std::fs::read_dir(&cache_root)? {
            let subdir = subdir?;

            let key_result =
                subdir_to_key(subdir.file_name().as_encoded_bytes()).warn_error(format!(
                    "expected subdir: {:?} to be parsable as a key",
                    subdir.file_name()
                ));
            let key = match key_result {
                Ok(k) => k,
                Err(_) => continue,
            };
            let md = subdir.metadata()?;
            if !md.is_dir() {
                warn!("found a non directory in cache_root directory");
                continue;
            }
            for key_file in std::fs::read_dir(subdir.path())? {
                let key_file = key_file?;
                let file_name_result = FileName::try_parse(key_file.file_name().as_encoded_bytes())
                    .warn_error(format!(
                        "expected file name: {:?} to be parsed as a cache FileName",
                        key_file.file_name()
                    ));
                let item_file_name = match file_name_result {
                    Ok(f) => f,
                    Err(_) => continue,
                };

                let md = key_file.metadata()?;
                if !md.is_file() {
                    warn!("found non file under key dir: {:?}", key_file.path());
                    continue;
                }

                items.insert(
                    (
                        key.clone(),
                        item_file_name.start_idx,
                        item_file_name.end_idx,
                    ),
                    (item_file_name, md.len()),
                );
                total_bytes += md.len();
            }
        }

        // ensures we only handle a real capacity
        let capacity = capacity.and_then(|cap| if cap == 0 { None } else { Some(cap) });

        Ok(Self {
            cache_root,
            total_bytes,
            capacity,
            items,
        })
    }

    fn get_impl(
        &mut self,
        key: &Key,
        range: &Range,
    ) -> Result<Option<Vec<u8>>, crate::error::ChunkCacheError> {
        let mut hits = self
            .items
            .iter()
            .filter(|((k, start, end), _)| *start <= range.start && *end >= range.end && k == key)
            .peekable();
        let hit_option = hits.peek();
        let ((key, start, end), (file_name, _)) = match hit_option {
            Some(v) => v,
            None => return Ok(None),
        };
        let file_path = self.cache_file_path(key, file_name);
        let mut file = File::open(&file_path)?;
        let hash = blake3::Hasher::new().update_reader(&mut file)?.finalize();
        if hash != file_name.hash {
            std::fs::remove_file(&file_path)?;
            return Err(ChunkCacheError::parse("file checksum mismatch"));
        }

        let result = self.hit(&mut file, *start, range)?;
        self.update_time_stamp(&(key.clone(), *start, *end))?;

        Ok(Some(result))
    }

    fn hit<R: Read + Seek>(
        &self,
        file: &mut R,
        start: u32,
        range: &Range,
    ) -> Result<Vec<u8>, ChunkCacheError> {
        let header = CacheFileHeader::deserialize(file)?;
        let start_byte_index = header
            .chunk_byte_indicies
            .get((range.start - start) as usize)
            .ok_or(ChunkCacheError::BadRange)?;
        let end_byte_index = header
            .chunk_byte_indicies
            .get((range.end - start) as usize)
            .ok_or(ChunkCacheError::BadRange)?;
        let start_position = (header.header_len + *start_byte_index as usize) as u64;
        file.seek(std::io::SeekFrom::Start(start_position))?;
        let len = (end_byte_index - start_byte_index) as usize;
        let mut result = vec![0; len];
        file.read_exact(&mut result)?;

        Ok(result)
    }

    fn put_impl(
        &mut self,
        key: &Key,
        range: &Range,
        chunk_byte_indicies: &[u32],
        data: &[u8],
    ) -> Result<(), ChunkCacheError> {
        /* // match for coalescing opportunities
        let matches = self
            .items
            .keys()
            .filter(|(k, start, end)| {
                k == key
                    && ((*start..=*end).contains(&range.start)
                        || (*start..=*end).contains(&range.end))
            })
            .cloned()
            .collect::<Vec<_>>();
        */
        let matches = self
            .items
            .keys()
            .filter(|(k, start, end)| k == key && *start <= range.start && *end >= range.end)
            .collect::<Vec<_>>();
        if !matches.is_empty() {
            self.update_time_stamp(&matches[0].clone())?;
            return Ok(());
        }

        // create new cache file
        let dir = &self.cache_root.join(key_to_subdir(key));

        let header = CacheFileHeader::new(chunk_byte_indicies);
        let mut header_buf = Vec::with_capacity(header.header_len);
        header.serialize(&mut header_buf)?;

        let hash = blake3::Hasher::new()
            .update(&header_buf)
            .update(data)
            .finalize();
        let file_name = FileName::new(range.start, range.end, SystemTime::now(), hash);
        let file_path = Path::join(dir, Into::<PathBuf>::into(&file_name));

        if !dir.exists() {
            std::fs::create_dir_all(dir)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_path)?;
        file.write_all(&header_buf)?;
        file.write_all(data)?;
        let len = (header_buf.len() + data.len()) as u64;
        self.total_bytes += len;
        self.items
            .insert((key.clone(), range.start, range.end), (file_name, len));
        self.maybe_evict()?;

        Ok(())
    }

    fn maybe_evict(&mut self) -> Result<(), ChunkCacheError> {
        let capacity = match self.capacity {
            Some(cap) => cap,
            None => return Ok(()),
        };

        if self.total_bytes <= capacity {
            return Ok(());
        }

        // evict max 10 items
        let mut i = 0;
        while capacity <= self.total_bytes && i < 10 {
            i += 1;
            self.maybe_evict_one()?;
        }

        Ok(())
    }

    // assumes the cache is in the right state for eviction
    fn maybe_evict_one(&mut self) -> Result<(), ChunkCacheError> {
        let items_key = if let Some(key) = self.items.keys().choose(&mut thread_rng()) {
            key.clone()
        } else {
            return Err(ChunkCacheError::CacheEmpty);
        };
        if let Some((file_name, len)) = self.items.remove(&items_key) {
            self.total_bytes -= len;
            let mut path = self.cache_file_path(&items_key.0, &file_name);
            std::fs::remove_file(&path)?;
            path.pop();
            if std::fs::read_dir(&path)?.next().is_none() {
                std::fs::remove_dir(&path)?;
            }
        }

        Ok(())
    }

    fn cache_file_path(&self, key: &Key, file_name: &FileName) -> PathBuf {
        cache_file_path(self.cache_root.clone(), key, file_name)
    }

    fn update_time_stamp(&mut self, item_key: &(Key, u32, u32)) -> Result<(), ChunkCacheError> {
        let cache_root = self.cache_root.clone();
        let (file_name, _) = self
            .items
            .get_mut(item_key)
            .ok_or(ChunkCacheError::Infallible)?;
        let old_file_path = cache_file_path(cache_root.clone(), &item_key.0, file_name);
        file_name.timestamp = SystemTime::now();
        let new_file_path = cache_file_path(cache_root, &item_key.0, file_name);
        std::fs::rename(old_file_path, new_file_path)?;

        Ok(())
    }
}

fn cache_file_path(cache_root: PathBuf, key: &Key, file_name: &FileName) -> PathBuf {
    cache_root
        .join(key_to_subdir(key))
        .join(Into::<PathBuf>::into(file_name))
}

impl ChunkCache for DiskCache {
    fn get(
        &mut self,
        key: &Key,
        range: &Range,
    ) -> Result<Option<Vec<u8>>, crate::error::ChunkCacheError> {
        self.get_impl(key, range)
    }

    fn put(
        &mut self,
        key: &Key,
        range: &Range,
        chunk_byte_indicies: &[u32],
        data: &[u8],
    ) -> Result<(), crate::error::ChunkCacheError> {
        self.put_impl(key, range, chunk_byte_indicies, data)
    }
}

fn key_to_subdir(key: &Key) -> String {
    let prefix_bytes = key.prefix.as_bytes();
    let mut buf = vec![0u8; size_of::<MerkleHash>() + prefix_bytes.len()];
    buf[..size_of::<MerkleHash>()].copy_from_slice(key.hash.as_bytes());
    buf[size_of::<MerkleHash>()..].copy_from_slice(prefix_bytes);
    BASE64_ENGINE.encode(buf)
}

fn subdir_to_key<T: AsRef<[u8]>>(subdir: T) -> Result<Key, ChunkCacheError> {
    let mut buf = BASE64_ENGINE
        .decode(subdir)
        .map_err(ChunkCacheError::parse)?;
    if buf.len() < 1 + size_of::<MerkleHash>() {
        return Err(ChunkCacheError::parse("decoded too few bytes"));
    }
    let prefix_buf = buf.split_off(size_of::<MerkleHash>());
    let hash =
        MerkleHash::from_slice(&buf[..size_of::<MerkleHash>()]).map_err(ChunkCacheError::parse)?;
    let prefix = String::from_utf8(prefix_buf)
        .map_err(|e| ChunkCacheError::parse(format!("prefix string parse error: {e}")))?;
    Ok(Key { prefix, hash })
}

#[cfg(test)]
mod test_utils {
    use std::path::Path;

    use cas_types::{Key, Range};
    use merklehash::MerkleHash;
    use rand::Rng;

    pub const DEFAULT_CAPACITY: Option<u64> = Some(16 << 20);
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
    use crate::disk_cache::test_utils::*;

    use cas_types::Range;
    use tempdir::TempDir;

    use crate::ChunkCache;

    use super::{subdir_to_key, DiskCache};

    #[test]
    fn test_get_cache_empty() {
        let cache_root = TempDir::new("empty").unwrap();
        let mut cache = DiskCache::initialize(cache_root.path(), DEFAULT_CAPACITY).unwrap();
        assert!(cache.get(&random_key(), &random_range()).unwrap().is_none());
    }

    #[test]
    fn test_put_get_simple() {
        let cache_root = TempDir::new("put_get_simple").unwrap();
        let mut cache = DiskCache::initialize(cache_root.path(), DEFAULT_CAPACITY).unwrap();

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
        assert!(cache.get(&key, &miss_range).unwrap().is_none());
    }

    #[test]
    fn test_put_get_subrange() {
        let cache_root = TempDir::new("put_get_subrange").unwrap();
        let mut cache = DiskCache::initialize(cache_root.path(), DEFAULT_CAPACITY).unwrap();

        let key = random_key();
        let range = Range { start: 0, end: 4 };
        let (chunk_byte_indicies, data) = random_bytes(&range);
        let put_result = cache.put(&key, &range, &chunk_byte_indicies, data.as_slice());
        assert!(put_result.is_ok(), "{put_result:?}");

        print_directory_contents(cache_root.as_ref());

        for start in range.start..range.end {
            for end in (start + 1)..=range.end {
                assert!(
                    cache.get(&key, &Range { start, end }).unwrap().is_some(),
                    "range: [{start} {end})"
                );
            }
        }
    }

    #[test]
    fn test_puts_eviction() {
        const CAP: u64 = (RANGE_LEN * 4) as u64;
        let cache_root = TempDir::new("puts_eviction").unwrap();
        let mut cache = DiskCache::initialize(cache_root.path(), Some(CAP)).unwrap();

        // fill the cache to almost capacity
        for _ in 0..3 {
            let (key, range, offsets, data) = RandomEntryIterator.next().unwrap();
            assert!(cache.put(&key, &range, &offsets, &data).is_ok());
        }
        assert!(cache.total_bytes <= CAP);

        let (key, range, offsets, data) = RandomEntryIterator.next().unwrap();
        assert!(cache.put(&key, &range, &offsets, &data).is_ok());
        assert!(cache.total_bytes <= CAP);
    }

    #[test]
    fn test_same_puts_noop() {
        let cache_root = TempDir::new("puts_eviction").unwrap();
        let mut cache = DiskCache::initialize(cache_root.path(), DEFAULT_CAPACITY).unwrap();
        let (key, range, offsets, data) = RandomEntryIterator.next().unwrap();
        assert!(cache.put(&key, &range, &offsets, &data).is_ok());

        assert!(cache.put(&key, &range, &offsets, &data).is_ok());
    }

    #[test]
    fn test_initialize_non_empty() {
        let cache_root = TempDir::new("puts_eviction").unwrap();
        let mut cache = DiskCache::initialize(cache_root.path(), DEFAULT_CAPACITY).unwrap();

        let mut keys_and_ranges = Vec::new();

        for _ in 0..20 {
            let (key, range, offsets, data) = RandomEntryIterator.next().unwrap();
            assert!(cache.put(&key, &range, &offsets, &data).is_ok());
            keys_and_ranges.push((key, range));
        }
        let mut cache2 = DiskCache::initialize(cache_root.path(), DEFAULT_CAPACITY).unwrap();
        for (i, (key, range)) in keys_and_ranges.iter().enumerate() {
            let get_result = cache2.get(&key, &range);
            assert!(get_result.is_ok(), "{i} {get_result:?}");
            assert!(get_result.unwrap().is_some(), "{i}");
        }
    }

    #[test]
    fn test_subdir_to_key() {
        let s = "oL-Xqk1J00kVe1U4kCko-Kw4zaVv3-4U73i27w5DViBkZWZhdWx0";
        let key = subdir_to_key(s);
        assert!(key.is_ok(), "{key:?}")
    }
}
