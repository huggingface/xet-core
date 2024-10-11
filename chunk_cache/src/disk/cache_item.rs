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
            "CacheItem {{ range: {:?}, len: {}, hash: {} }}",
            self.range, self.len, self.hash,
        )
    }
}

// impl PartialOrd & Ord to sort by the range to enable binary search over
// sorted CacheItems using the range field to match a range for search
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
    pub(crate) fn file_name(&self) -> Result<String, ChunkCacheError> {
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
        if buf.len() != CACHE_ITEM_FILE_NAME_BUF_SIZE {
            return Err(ChunkCacheError::parse(
                "decoded buf is not the right size for a cache item file name",
            ));
        }
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

#[cfg(test)]
mod tests {
    use base64::Engine;
    use blake3::OUT_LEN;
    use cas_types::Range;
    use sorted_vec::SortedVec;

    use crate::disk::{cache_item::CACHE_ITEM_FILE_NAME_BUF_SIZE, BASE64_ENGINE};

    use super::{range_contained_fn, CacheItem};

    impl Default for CacheItem {
        fn default() -> Self {
            Self {
                range: Default::default(),
                len: Default::default(),
                hash: blake3::Hash::from_bytes([0u8; OUT_LEN]),
            }
        }
    }

    #[test]
    fn test_to_file_name_len() {
        let cache_item = CacheItem {
            range: Range {
                start: 0,
                end: 1024,
            },
            len: 16 << 20,
            hash: blake3::hash(&(1..100).collect::<Vec<u8>>()),
        };

        let file_name = cache_item.file_name().unwrap();
        let decoded = BASE64_ENGINE.decode(file_name).unwrap();
        assert_eq!(decoded.len(), CACHE_ITEM_FILE_NAME_BUF_SIZE);
    }

    #[test]
    fn test_binary_search() {
        let range = |i: u32| Range {
            start: i * 100,
            end: (i + 1) * 100,
        };
        let sub_range = |i: u32| Range {
            start: i * 100 + 20,
            end: (i + 1) * 100 - 1,
        };

        let v = SortedVec::from(
            (1..=10)
                .map(|i| CacheItem {
                    range: range(i),
                    ..Default::default()
                })
                .collect::<Vec<_>>(),
        );

        for i in 1..=10 {
            let r = range(i);
            let sr = sub_range(i);

            let idx_result = v.binary_search_by(range_contained_fn(&r));
            assert!(idx_result.is_ok(), "{r} {idx_result:?}");
            let idx = idx_result.unwrap();
            assert_eq!(idx, (i - 1) as usize, "{r}, {i}");

            let sr_idx_result = v.binary_search_by(range_contained_fn(&sr));
            assert!(sr_idx_result.is_ok(), "{sr}, {sr_idx_result:?}");
            let sr_idx = sr_idx_result.unwrap();
            assert_eq!(sr_idx, (i - 1) as usize, "{sr}, {i}");
        }

        let out_range = range(100);
        assert!(v.binary_search_by(range_contained_fn(&out_range)).is_err());
    }
}
