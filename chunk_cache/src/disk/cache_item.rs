use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::mem::size_of;
use std::ops::Deref;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use base64::Engine;
use cas_types::ChunkRange;
use utils::serialization_utils::{read_u32, read_u64, write_u32, write_u64};

use super::BASE64_ENGINE;
use crate::error::ChunkCacheError;

#[derive(Debug, Clone)]
pub(crate) struct VerificationCell<T> {
    inner: T,
    verification: Arc<AtomicBool>,
}

impl<T: std::fmt::Display> std::fmt::Display for VerificationCell<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({})",
            self.inner,
            if self.verification.load(std::sync::atomic::Ordering::Relaxed) {
                "verified"
            } else {
                "unverified"
            }
        )
    }
}

impl<T> AsRef<T> for VerificationCell<T> {
    fn as_ref(&self) -> &T {
        &self.inner
    }
}

impl<T> Deref for VerificationCell<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T: Hash> Hash for VerificationCell<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state)
    }
}

impl<T: PartialEq> PartialEq for VerificationCell<T> {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(other)
    }
}

impl<T: Debug + Clone> VerificationCell<T> {
    pub fn new_unverified(inner: T) -> Self {
        Self::new(inner, false)
    }

    pub fn new_verified(inner: T) -> Self {
        Self::new(inner, true)
    }

    #[inline]
    fn new(inner: T, verified: bool) -> Self {
        Self {
            inner,
            verification: Arc::new(AtomicBool::new(verified)),
        }
    }

    pub fn verify(&self) {
        self.verification.store(true, std::sync::atomic::Ordering::Release)
    }

    pub fn is_verified(&self) -> bool {
        self.verification.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl<T: PartialEq> PartialEq<T> for VerificationCell<T> {
    fn eq(&self, other: &T) -> bool {
        self.inner.eq(other)
    }
}

// range start, range end, length, and checksum
const CACHE_ITEM_FILE_NAME_BUF_SIZE: usize = size_of::<u32>() * 2 + size_of::<u64>() + size_of::<u32>();

/// A CacheItem represents metadata for a single range in the cache
/// it contains the range of chunks the item is for
/// the length of the file on disk and the hash of the file contents
/// for validation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CacheItem {
    pub(crate) range: ChunkRange,
    pub(crate) len: u64,
    pub(crate) checksum: u32,
}

impl std::fmt::Display for CacheItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CacheItem {{ range: {:?}, len: {}, checksum: {} }}", self.range, self.len, self.checksum,)
    }
}

// impl PartialOrd & Ord to sort by the range to enable binary search over
// sorted CacheItems using the range field to match a range for search
impl Ord for CacheItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.range.cmp(&other.range)
    }
}

impl PartialOrd for CacheItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// CacheItem is represented on disk as the file name of a cache file
/// the file name is created by base64 encoding a buffer that concatenates
/// all attributes of the CacheItem, numbers being written in little endian order
impl CacheItem {
    pub(crate) fn file_name(&self) -> Result<String, ChunkCacheError> {
        let mut buf = [0u8; CACHE_ITEM_FILE_NAME_BUF_SIZE];
        let mut w = Cursor::new(&mut buf[..]);
        write_u32(&mut w, self.range.start)?;
        write_u32(&mut w, self.range.end)?;
        write_u64(&mut w, self.len)?;
        write_u32(&mut w, self.checksum)?;
        Ok(BASE64_ENGINE.encode(buf))
    }

    pub(crate) fn parse(file_name: &[u8]) -> Result<CacheItem, ChunkCacheError> {
        let buf = BASE64_ENGINE.decode(file_name)?;
        if buf.len() != CACHE_ITEM_FILE_NAME_BUF_SIZE {
            return Err(ChunkCacheError::parse("decoded buf is not the right size for a cache item file name"));
        }
        let mut r = Cursor::new(buf);
        let start = read_u32(&mut r)?;
        let end = read_u32(&mut r)?;
        let len = read_u64(&mut r)?;
        let checksum = read_u32(&mut r)?;
        if start >= end {
            return Err(ChunkCacheError::BadRange);
        }

        Ok(Self {
            range: ChunkRange::new(start, end),
            len,
            checksum,
        })
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use cas_types::ChunkRange;

    use crate::disk::cache_item::CACHE_ITEM_FILE_NAME_BUF_SIZE;
    use crate::disk::{CacheItem, BASE64_ENGINE};

    impl Default for CacheItem {
        fn default() -> Self {
            Self {
                range: Default::default(),
                len: Default::default(),
                checksum: Default::default(),
            }
        }
    }

    #[test]
    fn test_to_file_name_len() {
        let cache_item = CacheItem {
            range: ChunkRange::new(0, 1024),
            len: 16 << 20,
            checksum: 10000,
        };

        let file_name = cache_item.file_name().unwrap();
        let decoded = BASE64_ENGINE.decode(file_name).unwrap();
        assert_eq!(decoded.len(), CACHE_ITEM_FILE_NAME_BUF_SIZE);
    }
}
