//! Complete reconstructed files, named by Xet file hash, in the repository's common Git directory.
//!
//! Linked worktrees share the cache. An entry appears only when complete: it is written to a hidden
//! temporary file in the cache directory and renamed into place, so an interrupted write is never
//! mistaken for a cached file. Published entries are never written again; concurrent writers of
//! one hash publish interchangeable content. Entries are independent of upload staging and may be
//! removed at any time.
use std::path::{Path, PathBuf};

use tempfile::TempPath;
use xet_core_structures::merklehash::MerkleHash;

use crate::{Result, XetPointer};

pub(crate) struct FileCache {
    root: PathBuf,
}

impl FileCache {
    /// Opens the cache for the repository whose common Git directory is `common_dir`.
    pub(crate) fn new(common_dir: &Path) -> Result<Self> {
        let root = common_dir.join("xet").join("files");
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    pub(crate) fn path(&self, hash: &MerkleHash) -> PathBuf {
        self.root.join(hash.hex())
    }

    /// Whether the pointer's file is cached. The size check rejects a foreign file of the wrong length.
    pub(crate) fn contains(&self, pointer: &XetPointer) -> bool {
        std::fs::metadata(self.path(&pointer.hash)).is_ok_and(|m| m.is_file() && m.len() == pointer.size)
    }

    /// A temporary file in the cache directory, removed on drop unless published.
    pub(crate) fn temp_path(&self) -> Result<TempPath> {
        Ok(tempfile::Builder::new()
            .prefix(".")
            .suffix(".tmp")
            .tempfile_in(&self.root)?
            .into_temp_path())
    }

    /// Publishes a complete temporary file as the entry for `hash`.
    pub(crate) fn publish(&self, temp: TempPath, hash: &MerkleHash) -> Result<()> {
        temp.persist(self.path(hash)).map_err(|error| error.error)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publishes_complete_entries_only() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let cache = FileCache::new(dir.path())?;
        let pointer = XetPointer::new(MerkleHash::from([1, 2, 3, 4]), 5);
        assert!(!cache.contains(&pointer));

        // An abandoned temporary file is removed and never becomes an entry.
        let temp = cache.temp_path()?;
        std::fs::write(&temp, b"hello")?;
        drop(temp);
        assert!(!cache.contains(&pointer));
        assert_eq!(std::fs::read_dir(dir.path().join("xet/files"))?.count(), 0);

        let temp = cache.temp_path()?;
        std::fs::write(&temp, b"hello")?;
        cache.publish(temp, &pointer.hash)?;
        assert!(cache.contains(&pointer));
        assert!(!cache.contains(&XetPointer::new(pointer.hash, 6)));

        // A second publisher of the same hash replaces the entry with identical content.
        let temp = cache.temp_path()?;
        std::fs::write(&temp, b"hello")?;
        cache.publish(temp, &pointer.hash)?;
        assert_eq!(std::fs::read(cache.path(&pointer.hash))?, b"hello");
        Ok(())
    }
}
