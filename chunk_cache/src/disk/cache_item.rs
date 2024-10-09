use super::BASE64_ENGINE;
use crate::error::ChunkCacheError;
use base64::Engine;
use blake3::Hash;
use cas_types::Range;
use std::{
    cmp::Ordering,
    io::{Cursor, Read, Write},
    mem::size_of,
};
use utils::serialization_utils::{read_u32, read_u64, write_u32, write_u64};

const CACHE_ITEM_FILE_NAME_BUF_SIZE: usize =
    size_of::<u32>() * 2 + size_of::<u64>() + blake3::OUT_LEN;

/// A CacheItem represents metadata for a single range in the cache
/// it contains the range of chunks the item is for
/// the length of the file on disk and the hash of the file contents
/// for validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CacheItem {
    pub(crate) range: Range,
    pub(crate) len: u64,
    pub(crate) hash: Hash,
}

impl std::fmt::Display for CacheItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CacheItem {{ range: {}, len: {}, hash: {} }}",
            self.range, self.len, self.hash,
        )
    }
}

// impl PartialOrd & Ord to sort initially by the range to enable binary search over
// sorted CacheItems using the range field to match a range for search
// see logic
impl Ord for CacheItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.range.cmp(&other.range)
    }
}

impl PartialOrd for CacheItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl CacheItem {
    pub(crate) fn to_file_name(&self) -> Result<String, ChunkCacheError> {
        let mut buf = [0u8; CACHE_ITEM_FILE_NAME_BUF_SIZE];
        let mut w = Cursor::new(&mut buf[..]);
        write_u32(&mut w, self.range.start)?;
        write_u32(&mut w, self.range.end)?;
        write_u64(&mut w, self.len)?;
        write_hash(&mut w, &self.hash)?;
        Ok(BASE64_ENGINE.encode(buf))
    }

    pub(crate) fn parse(file_name: &[u8]) -> Result<CacheItem, ChunkCacheError> {
        let buf = BASE64_ENGINE.decode(file_name)?;
        let mut r = Cursor::new(buf);
        let start = read_u32(&mut r)?;
        let end = read_u32(&mut r)?;
        let len = read_u64(&mut r)?;
        let hash = read_hash(&mut r)?;
        if start >= end {
            return Err(ChunkCacheError::BadRange);
        }

        Ok(Self {
            range: Range { start, end },
            len,
            hash,
        })
    }
}

pub(super) fn range_contained_fn(
    range: &Range,
) -> impl FnMut(&CacheItem) -> std::cmp::Ordering + '_ {
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

pub fn write_hash(writer: &mut impl Write, hash: &blake3::Hash) -> Result<(), std::io::Error> {
    writer.write_all(hash.as_bytes())
}

pub fn read_hash(reader: &mut impl Read) -> Result<blake3::Hash, std::io::Error> {
    let mut m = [0u8; 32];
    reader.read_exact(&mut m)?;
    Ok(blake3::Hash::from_bytes(m))
}
