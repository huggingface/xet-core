use std::hash::{BuildHasher, Hash};
use std::sync::Arc;

use super::parallel_hash_map_impl::ParallelHashMapInner;

/// A thread-safe hash set using submap-based partitioning.
///
/// This is a thin wrapper around `ParallelHashMapInner` with `()` as the value type,
/// providing set-like semantics.
///
/// # Type Parameters
/// - `Key`: The key type (must implement `Hash + Eq`)
/// - `S`: The hasher builder (default: `std::collections::hash_map::RandomState`)
/// - `FIRST_LEVEL_SIZE`: Number of submaps (must be a power of 2, default: 64)
pub struct ParallelHashSet<Key, S = std::collections::hash_map::RandomState, const FIRST_LEVEL_SIZE: usize = 64> {
    pub(super) inner: Arc<ParallelHashMapInner<Key, (), S, FIRST_LEVEL_SIZE>>,
}

impl<Key, S, const FIRST_LEVEL_SIZE: usize> ParallelHashSet<Key, S, FIRST_LEVEL_SIZE>
where
    Key: Hash + Eq,
    S: BuildHasher + Default + Clone,
{
    /// Creates a new empty `ParallelHashSet`.
    pub fn new() -> Self {
        Self::with_hasher(S::default())
    }

    /// Creates a new empty `ParallelHashSet` with the given hasher.
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            inner: Arc::new(ParallelHashMapInner::new(hasher)),
        }
    }

    /// Returns the number of submaps.
    pub const fn num_submaps(&self) -> usize {
        FIRST_LEVEL_SIZE
    }

    /// Checks if the set contains the key.
    pub fn contains(&self, key: &Key) -> bool {
        self.inner.contains_key(key)
    }

    /// Inserts a key into the set. Returns `true` if the key was newly inserted.
    pub fn insert(&self, key: Key) -> bool {
        self.inner.insert(key, ()).is_none()
    }

    /// Removes a key from the set. Returns `true` if the key was present.
    pub fn remove(&self, key: &Key) -> bool {
        self.inner.remove(key).is_some()
    }
}

impl<Key, S, const FIRST_LEVEL_SIZE: usize> Default for ParallelHashSet<Key, S, FIRST_LEVEL_SIZE>
where
    Key: Hash + Eq,
    S: BuildHasher + Default + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(target_family = "wasm"))]
impl<Key, S, const FIRST_LEVEL_SIZE: usize> ParallelHashSet<Key, S, FIRST_LEVEL_SIZE>
where
    Key: Hash + Eq + Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    S: BuildHasher + Default + Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
{
    /// Serializes to an archive directory using the default write concurrency (4 parallel files).
    pub async fn serialize_to_archive(&self, dir: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        self.serialize_to_archive_with_concurrency(dir, super::parallel_hash_map::DEFAULT_WRITE_CONCURRENCY)
            .await
    }

    /// Serializes to an archive directory using a specified number of parallel data files.
    ///
    /// # Arguments
    /// * `dir` - The directory to write the archive to
    /// * `write_concurrency` - Number of data files to create (parallelism level for I/O)
    pub async fn serialize_to_archive_with_concurrency(
        &self,
        dir: impl AsRef<std::path::Path>,
        write_concurrency: usize,
    ) -> std::io::Result<()> {
        self.inner.clone().serialize_to_archive(dir, write_concurrency).await
    }

    /// Deserializes from an archive directory using the default read concurrency.
    pub async fn deserialize_from_archive(dir: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        Self::deserialize_from_archive_with_concurrency(dir, super::parallel_hash_map_impl::DEFAULT_READ_CONCURRENCY)
            .await
    }

    /// Deserializes from an archive directory using a specified read concurrency.
    ///
    /// # Arguments
    /// * `dir` - The directory to read from
    /// * `read_concurrency` - Number of parallel readers (can exceed number of files)
    pub async fn deserialize_from_archive_with_concurrency(
        dir: impl AsRef<std::path::Path>,
        read_concurrency: usize,
    ) -> std::io::Result<Self> {
        let inner = Arc::new(
            super::parallel_hash_map_impl::ParallelHashMapInner::deserialize_from_archive(dir, read_concurrency)
                .await?,
        );
        Ok(Self { inner })
    }

    /// Loads and merges from an archive directory using the default read concurrency.
    pub async fn load_and_merge(&self, dir: impl AsRef<std::path::Path>) -> std::io::Result<usize> {
        self.load_and_merge_with_concurrency(dir, super::parallel_hash_map_impl::DEFAULT_READ_CONCURRENCY)
            .await
    }

    /// Loads and merges from an archive directory using a specified read concurrency.
    ///
    /// # Arguments
    /// * `dir` - The directory to read from
    /// * `read_concurrency` - Number of parallel readers
    pub async fn load_and_merge_with_concurrency(
        &self,
        dir: impl AsRef<std::path::Path>,
        read_concurrency: usize,
    ) -> std::io::Result<usize> {
        // For sets, always use overwrite=true since there's no value to preserve
        self.inner
            .clone()
            .load_and_merge_from_archive(dir, read_concurrency, true)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::collections::hash_map::RandomState;
    use std::sync::Arc;

    use merklehash::{DataHash, compute_data_hash};

    use super::*;
    use crate::data_structures::passthrough_hasher::U64DirectHasher;

    type ParallelU64HashSet<const FIRST_LEVEL_SIZE: usize = 64> =
        ParallelHashSet<u64, U64DirectHasher<u64>, FIRST_LEVEL_SIZE>;

    type ParallelMerkleHashSet<const FIRST_LEVEL_SIZE: usize = 64> =
        ParallelHashSet<DataHash, U64DirectHasher<DataHash>, FIRST_LEVEL_SIZE>;

    fn test_set_correctness<const SIZE: usize>() {
        let set: ParallelHashSet<String, RandomState, SIZE> = ParallelHashSet::new();

        // Test construction
        assert_eq!(set.num_submaps(), SIZE);

        // Test default
        let set2: ParallelHashSet<String, RandomState, SIZE> = ParallelHashSet::default();
        assert_eq!(set2.num_submaps(), SIZE);

        // Generate distinct keys
        let num_keys = 500;
        let keys: Vec<String> = (0..num_keys).map(|i| format!("set_key_{i}")).collect();

        // Verify all keys are distinct
        let unique: HashSet<_> = keys.iter().collect();
        assert_eq!(unique.len(), num_keys);

        // Test insert and contains
        for key in keys.iter() {
            assert!(!set.contains(key), "Key should not exist before insert");

            let inserted = set.insert(key.clone());
            assert!(inserted, "First insert should return true");

            assert!(set.contains(key), "Key should exist after insert");
        }

        // Test insert duplicate (should return false)
        for key in keys.iter() {
            let inserted = set.insert(key.clone());
            assert!(!inserted, "Duplicate insert should return false");
            assert!(set.contains(key));
        }

        // Test remove
        for key in keys.iter() {
            let removed = set.remove(key);
            assert!(removed, "Remove should return true for existing key");

            assert!(!set.contains(key), "Key should not exist after remove");

            // Double remove should return false
            assert!(!set.remove(key));
        }

        // Re-insert after removal
        for key in keys.iter() {
            let inserted = set.insert(key.clone());
            assert!(inserted, "Insert after removal should return true");
            assert!(set.contains(key));
        }
    }

    #[test]
    fn test_set_correctness_2_submaps() {
        test_set_correctness::<2>();
    }

    #[test]
    fn test_set_correctness_4_submaps() {
        test_set_correctness::<4>();
    }

    #[test]
    fn test_set_correctness_32_submaps() {
        test_set_correctness::<32>();
    }

    #[test]
    fn test_set_correctness_default_submaps() {
        let set: ParallelHashSet<String> = ParallelHashSet::new();
        assert_eq!(set.num_submaps(), 64);
    }

    #[test]
    fn test_set_correctness_1024_submaps() {
        test_set_correctness::<1024>();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_set_parallel_operations() {
        const SIZE: usize = 64;
        const NUM_TASKS: usize = 16;
        const OPS_PER_TASK: usize = 5000;

        let set: Arc<ParallelHashSet<String, RandomState, SIZE>> = Arc::new(ParallelHashSet::new());

        // Pre-generate all keys to ensure they're distinct
        let all_keys: Vec<Vec<String>> = (0..NUM_TASKS)
            .map(|task_id| (0..OPS_PER_TASK).map(|i| format!("set_parallel_t{task_id}_k{i}")).collect())
            .collect();

        // Verify all keys across all tasks are distinct
        let all_flat: Vec<_> = all_keys.iter().flatten().collect();
        let unique: HashSet<_> = all_flat.iter().collect();
        assert_eq!(unique.len(), NUM_TASKS * OPS_PER_TASK);

        let handles: Vec<_> = (0..NUM_TASKS)
            .map(|task_id| {
                let set = Arc::clone(&set);
                let keys = all_keys[task_id].clone();
                tokio::spawn(async move {
                    // Insert all
                    for key in keys.iter() {
                        let inserted = set.insert(key.clone());
                        assert!(inserted);
                    }

                    // Verify all inserted correctly
                    for key in keys.iter() {
                        assert!(set.contains(key));
                    }

                    // Remove half
                    for key in keys.iter().take(OPS_PER_TASK / 2) {
                        set.remove(key);
                    }

                    // Verify removals
                    for (i, key) in keys.iter().enumerate() {
                        if i < OPS_PER_TASK / 2 {
                            assert!(!set.contains(key));
                        } else {
                            assert!(set.contains(key));
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.await.expect("Task should not panic");
        }
    }

    #[test]
    fn test_set_all_submaps_used() {
        const SIZE: usize = 32;
        const NUM_KEYS: usize = 500;

        let set: ParallelHashSet<String, RandomState, SIZE> = ParallelHashSet::new();

        // Insert keys
        for i in 0..NUM_KEYS {
            let key = format!("set_dist_key_{i}");
            set.insert(key);
        }

        // Verify every submap has at least one entry
        for (i, submap) in set.inner.submaps.iter().enumerate() {
            let count = submap.lock().unwrap().len();
            assert!(count >= 1, "Submap {i} is empty - distribution is broken");
        }

        // Verify total count
        let total: usize = set.inner.submaps.iter().map(|s| s.lock().unwrap().len()).sum();
        assert_eq!(total, NUM_KEYS);
    }

    fn test_set_passthrough_correctness<const SIZE: usize>() {
        let set: ParallelMerkleHashSet<SIZE> = ParallelMerkleHashSet::new();

        // Test construction
        assert_eq!(set.num_submaps(), SIZE);

        // Generate distinct hashes
        let num_keys = 500;
        let hashes: Vec<_> = (0..num_keys)
            .map(|i| compute_data_hash(format!("set_passthrough_key_{i}").as_bytes()))
            .collect();

        // Verify all hashes are distinct
        let unique: HashSet<_> = hashes.iter().collect();
        assert_eq!(unique.len(), num_keys);

        // Test insert and contains
        for hash in hashes.iter() {
            assert!(!set.contains(hash));

            let inserted = set.insert(*hash);
            assert!(inserted);

            assert!(set.contains(hash));
        }

        // Test insert duplicate
        for hash in hashes.iter() {
            let inserted = set.insert(*hash);
            assert!(!inserted);
        }

        // Test remove
        for hash in hashes.iter() {
            let removed = set.remove(hash);
            assert!(removed);

            assert!(!set.contains(hash));
            assert!(!set.remove(hash));
        }
    }

    #[test]
    fn test_set_passthrough_correctness_2_submaps() {
        test_set_passthrough_correctness::<2>();
    }

    #[test]
    fn test_set_passthrough_correctness_32_submaps() {
        test_set_passthrough_correctness::<32>();
    }

    #[test]
    fn test_set_passthrough_correctness_256_submaps() {
        test_set_passthrough_correctness::<256>();
    }

    #[tokio::test]
    async fn test_set_archive_serialize_deserialize() {
        let temp_dir = tempfile::tempdir().unwrap();
        let archive_path = temp_dir.path().join("set_archive");

        const SIZE: usize = 16;
        let set: ParallelU64HashSet<SIZE> = ParallelU64HashSet::new();

        // Insert keys
        for i in 0u64..100 {
            set.insert(i);
        }

        // Serialize
        set.serialize_to_archive(&archive_path).await.unwrap();

        // Verify files exist
        assert!(archive_path.join("metadata.bin").exists());
        assert!(archive_path.join("hasher.bin").exists());
        // 4 parallel data files
        for i in 0..4 {
            assert!(archive_path.join(format!("data_{i}.bin")).exists());
        }

        // Deserialize
        let loaded: ParallelU64HashSet<SIZE> =
            ParallelU64HashSet::deserialize_from_archive(&archive_path).await.unwrap();

        // Verify all entries
        for i in 0u64..100 {
            assert!(loaded.contains(&i));
        }

        // Verify non-existent keys
        for i in 100u64..200 {
            assert!(!loaded.contains(&i));
        }
    }

    #[tokio::test]
    async fn test_set_archive_different_submaps() {
        let temp_dir = tempfile::tempdir().unwrap();
        let archive_path = temp_dir.path().join("set_archive");

        // Create set with 16 submaps
        const SOURCE_SIZE: usize = 16;
        let source_set: ParallelU64HashSet<SOURCE_SIZE> = ParallelU64HashSet::new();

        for i in 0u64..100 {
            source_set.insert(i);
        }

        source_set.serialize_to_archive(&archive_path).await.unwrap();

        // Deserialize into set with different submap count
        const TARGET_SIZE: usize = 64;
        let loaded: ParallelU64HashSet<TARGET_SIZE> =
            ParallelU64HashSet::deserialize_from_archive(&archive_path).await.unwrap();

        assert_ne!(source_set.num_submaps(), loaded.num_submaps());

        // Verify all entries
        for i in 0u64..100 {
            assert!(loaded.contains(&i));
        }
    }

    #[tokio::test]
    async fn test_set_archive_load_and_merge() {
        let temp_dir = tempfile::tempdir().unwrap();
        let archive_path = temp_dir.path().join("set_archive");

        const SIZE: usize = 16;
        let set1: ParallelU64HashSet<SIZE> = ParallelU64HashSet::new();

        // Insert 0..50
        for i in 0u64..50 {
            set1.insert(i);
        }

        set1.serialize_to_archive(&archive_path).await.unwrap();

        // Create another set with overlapping and new keys
        let set2: ParallelU64HashSet<SIZE> = ParallelU64HashSet::new();
        for i in 25u64..75 {
            set2.insert(i);
        }

        // Merge archived set into set2
        let count = set2.load_and_merge(&archive_path).await.unwrap();
        assert_eq!(count, 50); // All 50 from archive are "inserted" (with overwrite=true)

        // Verify set2 now has union of both
        for i in 0u64..75 {
            assert!(set2.contains(&i), "Set should contain {i}");
        }
    }

    #[tokio::test]
    async fn test_set_archive_with_merkle_hash_keys() {
        let temp_dir = tempfile::tempdir().unwrap();
        let archive_path = temp_dir.path().join("set_archive");

        const SIZE: usize = 16;
        let set: ParallelMerkleHashSet<SIZE> = ParallelMerkleHashSet::new();

        let hashes: Vec<_> = (0..50).map(|i| compute_data_hash(format!("set_data_{i}").as_bytes())).collect();

        for hash in hashes.iter() {
            set.insert(*hash);
        }

        // Serialize
        set.serialize_to_archive(&archive_path).await.unwrap();

        // Deserialize
        let loaded: ParallelMerkleHashSet<SIZE> =
            ParallelMerkleHashSet::deserialize_from_archive(&archive_path).await.unwrap();

        // Verify all entries
        for hash in hashes.iter() {
            assert!(loaded.contains(hash));
        }
    }
}
