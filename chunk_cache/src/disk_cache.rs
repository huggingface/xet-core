use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, Write},
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    time::SystemTime,
};

use base64::prelude::*;
use cache_file_header::CacheFileHeader;
use cas_types::{Key, Range};
use error_printer::ErrorPrinter;
use file_name::FileName;
use rand::{rngs::ThreadRng, seq::IteratorRandom};
use tracing::warn;

use crate::{error::ChunkCacheError, ChunkCache};

mod cache_file_header;
mod file_name;

pub struct DiskCache {
    cache_root: PathBuf,
    total_bytes: u64,
    items: HashMap<PathBuf, u64>,
    capacity: Option<u64>,
    rng: ThreadRng,
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
            let md = subdir.metadata()?;
            if !md.is_dir() {
                warn!("found a non directory in cache_root directory");
                continue;
            }
            for key_file in std::fs::read_dir(subdir.path())? {
                let key_file = key_file?;
                let md = key_file.metadata()?;
                if !md.is_file() {
                    warn!("found non file under key dir: {:?}", key_file.path());
                    continue;
                }
                let item: PathBuf = key_file
                    .path()
                    .strip_prefix(&cache_root)
                    .map(Path::to_path_buf)
                    .log_error("path under directory didn't strip prefix")
                    .unwrap_or_default();
                items.insert(item, md.len());
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
            rng: rand::thread_rng(),
        })
    }

    fn get_impl(
        &mut self,
        key: &Key,
        range: &Range,
    ) -> Result<Option<Vec<u8>>, crate::error::ChunkCacheError> {
        let dir = self.cache_root.join(key_to_subdir(key));

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let md = entry.metadata()?;
            if !md.is_file() {
                continue;
            }
            let file_name = FileName::try_parse(entry.file_name().as_bytes())?;
            if range.start >= file_name.start_idx && range.end <= file_name.end_idx {
                let result = self.hit(&entry.path(), file_name, range)?;
                return Ok(Some(result));
            }
        }
        Ok(None)
    }

    fn hit(
        &mut self,
        path: &PathBuf,
        mut file_name: FileName,
        range: &Range,
    ) -> Result<Vec<u8>, ChunkCacheError> {
        let mut file = File::open(path)?;
        let hash = blake3::Hasher::new().update_reader(&mut file)?.finalize();
        if hash != file_name.hash {
            std::fs::remove_file(path)?;
            return Err(ChunkCacheError::parse("file checksum mismatch"));
        }

        let header = CacheFileHeader::deserialize(&mut file)?;
        let start_byte_index = header
            .chunk_byte_indicies
            .get(range.start as usize)
            .ok_or(ChunkCacheError::BadRange)?;
        let end_byte_index = header
            .chunk_byte_indicies
            .get(range.end as usize)
            .ok_or(ChunkCacheError::BadRange)?;
        let start_position = (header.header_len + *start_byte_index as usize) as u64;
        file.seek(std::io::SeekFrom::Start(start_position))?;
        let len = (end_byte_index - start_byte_index) as usize;
        let mut result = vec![0; len];
        file.read_exact(&mut result)?;

        // update last used timestamp in file name
        file_name.timestamp = SystemTime::now();
        let key_dir = path
            .parent()
            .ok_or_else(|| ChunkCacheError::parse("failed to get key dir"))?;
        let new_path = key_dir.join(Into::<PathBuf>::into(&file_name));
        std::fs::rename(path, &new_path)?;

        if let Some(size) = self.items.remove(&self.items_key(path)) {
            self.items.insert(self.items_key(new_path), size);
        }

        Ok(result)
    }

    fn put_impl(
        &mut self,
        key: &Key,
        range: &Range,
        chunk_byte_indicies: &[u32],
        data: &[u8],
    ) -> Result<(), ChunkCacheError> {
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

        if !std::fs::exists(dir)? {
            std::fs::create_dir_all(dir)?;
        }
        let mut file = std::fs::File::create(&file_path)?;
        file.write_all(&header_buf)?;
        file.write_all(data)?;
        let len = (header_buf.len() + data.len()) as u64;
        self.total_bytes += len;
        self.items.insert(self.items_key(&file_path), len);

        self.maybe_evict()?;

        Ok(())
    }

    fn maybe_evict(&mut self) -> Result<(), ChunkCacheError> {
        let capacity = match self.capacity {
            Some(cap) => cap,
            None => return Ok(()),
        };
        if capacity <= self.total_bytes {
            return Ok(());
        }

        while capacity > self.total_bytes {
            self.maybe_evict_one()?
        }

        Ok(())
    }

    // assumes the cache is in the right state for eviction
    fn maybe_evict_one(&mut self) -> Result<(), ChunkCacheError> {
        let key = if let Some(key) = self.items.keys().choose(&mut self.rng) {
            key.clone()
        } else {
            return Err(ChunkCacheError::CacheEmpty);
        };
        if let Some(len) = self.items.remove(&key) {
            self.total_bytes -= len;
        }
        let path = self.cache_root.join(&key);
        std::fs::remove_file(path)?;

        Ok(())
    }

    fn items_key<P: AsRef<Path>>(&self, path: P) -> PathBuf {
        items_key(&self.cache_root, &path)
    }
}

fn items_key<P: AsRef<Path>>(cache_root: &PathBuf, path: P) -> PathBuf {
    path.as_ref()
        .strip_prefix(cache_root)
        .log_error(format!(
            "path should be under prefix, but is not, returning empty, path: {:?}, prefix: {:?}",
            path.as_ref(),
            cache_root,
        ))
        .map(Path::to_path_buf)
        .unwrap_or_default()
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
        chunk_byte_indicies: &Vec<u32>,
        data: &[u8],
    ) -> Result<(), crate::error::ChunkCacheError> {
        self.put_impl(key, range, chunk_byte_indicies, data)
    }
}

fn key_to_subdir(key: &Key) -> String {
    BASE64_STANDARD_NO_PAD.encode(key.to_string().as_bytes())
}
