use std::hash::{BuildHasher, Hash};
use std::sync::Arc;

use super::parallel_hash_map_impl::ParallelHashMapInner;
pub use super::parallel_hash_map_impl::{DEFAULT_READ_CONCURRENCY, DEFAULT_WRITE_CONCURRENCY};

/// A thread-safe hash map using submap-based partitioning.
///
/// This structure uses a two-level hash table with a fixed-size first layer
/// determined by the `FIRST_LEVEL_SIZE` const generic parameter. Each submap contains
/// a `HashMap` protected by a `std::sync::Mutex`.
///
/// Submap selection uses the HIGH bits of the hash, while HashMap internally uses
/// LOW bits for bucket selection. This avoids correlation and ensures good distribution
/// across submaps.
///
/// # Type Parameters
/// - `Key`: The key type (must implement `Hash + Eq`)
/// - `Value`: The value type
/// - `S`: The hasher builder for both submap selection and internal HashMaps (default:
///   `std::collections::hash_map::RandomState`)
/// - `FIRST_LEVEL_SIZE`: Number of submaps (must be a power of 2, default: 256)
///
/// # Example
/// ```ignore
/// // Create a map with 256 submaps (the default)
/// let map: ParallelHashMap<String, i32> = ParallelHashMap::new();
/// map.insert("key".to_string(), 42);
///
/// // Create a map with 16 submaps
/// let map: ParallelHashMap<String, i32, _, 16> = ParallelHashMap::new();
/// ```
pub struct ParallelHashMap<Key, Value, S = std::collections::hash_map::RandomState, const FIRST_LEVEL_SIZE: usize = 64>
{
    pub(super) inner: Arc<ParallelHashMapInner<Key, Value, S, FIRST_LEVEL_SIZE>>,
}

impl<Key, Value, S, const FIRST_LEVEL_SIZE: usize> ParallelHashMap<Key, Value, S, FIRST_LEVEL_SIZE>
where
    Key: Hash + Eq,
    Value: Clone,
    S: BuildHasher + Default + Clone,
{
    /// Creates a new empty `ParallelHashMap`.
    pub fn new() -> Self {
        Self::with_hasher(S::default())
    }

    /// Creates a new empty `ParallelHashMap` with the given hasher.
    pub fn with_hasher(hasher: S) -> Self {
        Self {
            inner: Arc::new(ParallelHashMapInner::new(hasher)),
        }
    }

    /// Returns the number of submaps.
    pub const fn num_submaps(&self) -> usize {
        FIRST_LEVEL_SIZE
    }

    /// Checks if a key exists in the map.
    pub fn contains_key(&self, key: &Key) -> bool {
        self.inner.contains_key(key)
    }

    /// Inserts a key-value pair into the map.
    pub fn insert(&self, key: Key, value: Value) -> Option<Value> {
        self.inner.insert(key, value)
    }

    /// Removes a key from the map.
    pub fn remove(&self, key: &Key) -> Option<Value> {
        self.inner.remove(key)
    }

    /// Gets a clone of the value associated with the key.
    pub fn get(&self, key: &Key) -> Option<Value> {
        self.inner.get(key)
    }

    /// Gets a clone of the value, or inserts the provided value if the key doesn't exist.
    pub fn get_or_insert(&self, key: Key, value: Value) -> Value {
        self.inner.get_or_insert(key, value)
    }
}

impl<Key, Value, S, const FIRST_LEVEL_SIZE: usize> Default for ParallelHashMap<Key, Value, S, FIRST_LEVEL_SIZE>
where
    Key: Hash + Eq,
    Value: Clone,
    S: BuildHasher + Default + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(target_family = "wasm"))]
impl<Key, Value, S, const FIRST_LEVEL_SIZE: usize> ParallelHashMap<Key, Value, S, FIRST_LEVEL_SIZE>
where
    Key: Hash + Eq + Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    Value: Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
    S: BuildHasher + Default + Clone + serde::Serialize + for<'de> serde::Deserialize<'de> + Send + Sync + 'static,
{
    /// Serializes to an archive directory using the default write concurrency (4 parallel files).
    pub async fn serialize_to_archive(&self, dir: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        self.serialize_to_archive_with_concurrency(dir, DEFAULT_WRITE_CONCURRENCY).await
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
        Self::deserialize_from_archive_with_concurrency(dir, DEFAULT_READ_CONCURRENCY).await
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
    pub async fn load_and_merge(&self, dir: impl AsRef<std::path::Path>, overwrite: bool) -> std::io::Result<usize> {
        self.load_and_merge_with_concurrency(dir, DEFAULT_READ_CONCURRENCY, overwrite)
            .await
    }

    /// Loads and merges from an archive directory using a specified read concurrency.
    ///
    /// # Arguments
    /// * `dir` - The directory to read from
    /// * `read_concurrency` - Number of parallel readers
    /// * `overwrite` - Whether to overwrite existing entries
    pub async fn load_and_merge_with_concurrency(
        &self,
        dir: impl AsRef<std::path::Path>,
        read_concurrency: usize,
        overwrite: bool,
    ) -> std::io::Result<usize> {
        self.inner
            .clone()
            .load_and_merge_from_archive(dir, read_concurrency, overwrite)
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
    use crate::data_structures::parallel_hash_map_impl::ParallelHashMapInner;
    use crate::data_structures::passthrough_hasher::U64DirectHasher;

    type ParallelMerkleHashMap<Value, const FIRST_LEVEL_SIZE: usize = 64> =
        ParallelHashMap<DataHash, Value, U64DirectHasher<DataHash>, FIRST_LEVEL_SIZE>;

    type ParallelU64HashMap<Value, const FIRST_LEVEL_SIZE: usize = 256> =
        ParallelHashMap<u64, Value, U64DirectHasher<u64>, FIRST_LEVEL_SIZE>;

    type ParallelU64HashMapInner<Value, const FIRST_LEVEL_SIZE: usize = 256> =
        ParallelHashMapInner<u64, Value, U64DirectHasher<u64>, FIRST_LEVEL_SIZE>;

    // ===========================================
    // Generic correctness test for all operations
    // ===========================================

    fn test_correctness<const SIZE: usize>() {
        let map: ParallelHashMap<String, i32, RandomState, SIZE> = ParallelHashMap::new();

        // Test construction
        assert_eq!(map.num_submaps(), SIZE);

        // Test default
        let map2: ParallelHashMap<String, i32, RandomState, SIZE> = ParallelHashMap::default();
        assert_eq!(map2.num_submaps(), SIZE);

        // Generate distinct keys
        let num_keys = 500;
        let keys: Vec<String> = (0..num_keys).map(|i| format!("correctness_key_{i}")).collect();

        // Verify all keys are distinct
        let unique: HashSet<_> = keys.iter().collect();
        assert_eq!(unique.len(), num_keys);

        // Test insert and get
        for (i, key) in keys.iter().enumerate() {
            assert!(!map.contains_key(key), "Key should not exist before insert");
            assert_eq!(map.get(key), None);

            let prev = map.insert(key.clone(), i as i32);
            assert_eq!(prev, None, "First insert should return None");

            assert!(map.contains_key(key), "Key should exist after insert");
            assert_eq!(map.get(key), Some(i as i32));
        }

        // Test insert overwrite
        for (i, key) in keys.iter().enumerate() {
            let new_value = (i as i32) + 1000;
            let prev = map.insert(key.clone(), new_value);
            assert_eq!(prev, Some(i as i32), "Overwrite should return previous value");
            assert_eq!(map.get(key), Some(new_value));
        }

        // Test get_or_insert (should return existing values)
        for (i, key) in keys.iter().enumerate() {
            let expected = (i as i32) + 1000;
            let result = map.get_or_insert(key.clone(), 9999);
            assert_eq!(result, expected, "get_or_insert should return existing value");
        }

        // Test remove
        for (i, key) in keys.iter().enumerate() {
            let expected = (i as i32) + 1000;
            let removed = map.remove(key);
            assert_eq!(removed, Some(expected), "Remove should return the value");

            assert!(!map.contains_key(key), "Key should not exist after remove");
            assert_eq!(map.get(key), None);

            // Double remove should return None
            assert_eq!(map.remove(key), None);
        }

        // Test get_or_insert on empty map (should insert)
        for (i, key) in keys.iter().enumerate() {
            let value = (i as i32) + 2000;
            let result = map.get_or_insert(key.clone(), value);
            assert_eq!(result, value, "get_or_insert on missing key should insert and return value");
            assert_eq!(map.get(key), Some(value));
        }
    }

    #[test]
    fn test_correctness_2_submaps() {
        test_correctness::<2>();
    }

    #[test]
    fn test_correctness_4_submaps() {
        test_correctness::<4>();
    }

    #[test]
    fn test_correctness_32_submaps() {
        test_correctness::<32>();
    }

    #[test]
    fn test_correctness_default_submaps() {
        // Test with default (64 submaps)
        let map: ParallelHashMap<String, i32> = ParallelHashMap::new();
        assert_eq!(map.num_submaps(), 64);
    }

    #[test]
    fn test_correctness_1024_submaps() {
        test_correctness::<1024>();
    }

    // ===========================================
    // Large parallel random insert/retrieve test
    // ===========================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_parallel_random_operations() {
        const SIZE: usize = 64;
        const NUM_TASKS: usize = 16;
        const OPS_PER_TASK: usize = 5000;

        let map: Arc<ParallelHashMap<String, u64, RandomState, SIZE>> = Arc::new(ParallelHashMap::new());

        // Pre-generate all keys to ensure they're distinct
        let all_keys: Vec<Vec<String>> = (0..NUM_TASKS)
            .map(|task_id| (0..OPS_PER_TASK).map(|i| format!("parallel_t{task_id}_k{i}")).collect())
            .collect();

        // Verify all keys across all tasks are distinct
        let all_flat: Vec<_> = all_keys.iter().flatten().collect();
        let unique: HashSet<_> = all_flat.iter().collect();
        assert_eq!(unique.len(), NUM_TASKS * OPS_PER_TASK, "All keys must be distinct");

        let handles: Vec<_> = (0..NUM_TASKS)
            .map(|task_id| {
                let map = Arc::clone(&map);
                let keys = all_keys[task_id].clone();
                tokio::spawn(async move {
                    // Insert all
                    for (i, key) in keys.iter().enumerate() {
                        let value = (task_id * OPS_PER_TASK + i) as u64;
                        map.insert(key.clone(), value);
                    }

                    // Verify all inserted correctly
                    for (i, key) in keys.iter().enumerate() {
                        let expected = (task_id * OPS_PER_TASK + i) as u64;
                        assert!(map.contains_key(key));
                        assert_eq!(map.get(key), Some(expected));
                    }

                    // Remove half
                    for key in keys.iter().take(OPS_PER_TASK / 2) {
                        map.remove(key);
                    }

                    // Verify removals
                    for (i, key) in keys.iter().enumerate() {
                        if i < OPS_PER_TASK / 2 {
                            assert!(!map.contains_key(key));
                        } else {
                            assert!(map.contains_key(key));
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.await.expect("Task should not panic");
        }
    }

    // ===========================================
    // Distribution test - verify all submaps are used
    // ===========================================

    #[test]
    fn test_all_submaps_used() {
        const SIZE: usize = 32;
        const NUM_KEYS: usize = 500;

        let map: ParallelHashMap<String, i32, RandomState, SIZE> = ParallelHashMap::new();

        // Insert keys
        for i in 0..NUM_KEYS {
            let key = format!("dist_key_{i}");
            map.insert(key, i as i32);
        }

        // Verify every submap has at least one entry
        for (i, submap) in map.inner.submaps.iter().enumerate() {
            let count = submap.lock().unwrap().len();
            assert!(count >= 1, "Submap {i} is empty - distribution is broken");
        }

        // Verify total count
        let total: usize = map.inner.submaps.iter().map(|s| s.lock().unwrap().len()).sum();
        assert_eq!(total, NUM_KEYS);
    }

    // ===========================================
    // Passthrough hasher tests (MerkleHash keys)
    // ===========================================

    fn test_passthrough_correctness<const SIZE: usize>() {
        let map: ParallelMerkleHashMap<i32, SIZE> = ParallelMerkleHashMap::new();

        // Test construction
        assert_eq!(map.num_submaps(), SIZE);

        // Test default
        let map2: ParallelMerkleHashMap<i32, SIZE> = ParallelMerkleHashMap::default();
        assert_eq!(map2.num_submaps(), SIZE);

        // Generate distinct hashes
        let num_keys = 500;
        let hashes: Vec<_> = (0..num_keys)
            .map(|i| compute_data_hash(format!("correctness_key_{i}").as_bytes()))
            .collect();

        // Verify all hashes are distinct
        let unique: HashSet<_> = hashes.iter().collect();
        assert_eq!(unique.len(), num_keys);

        // Test insert and get
        for (i, hash) in hashes.iter().enumerate() {
            assert!(!map.contains_key(hash), "Key should not exist before insert");
            assert_eq!(map.get(hash), None);

            let prev = map.insert(*hash, i as i32);
            assert_eq!(prev, None, "First insert should return None");

            assert!(map.contains_key(hash), "Key should exist after insert");
            assert_eq!(map.get(hash), Some(i as i32));
        }

        // Test insert overwrite
        for (i, hash) in hashes.iter().enumerate() {
            let new_value = (i as i32) + 1000;
            let prev = map.insert(*hash, new_value);
            assert_eq!(prev, Some(i as i32), "Overwrite should return previous value");
            assert_eq!(map.get(hash), Some(new_value));
        }

        // Test get_or_insert (should return existing values)
        for (i, hash) in hashes.iter().enumerate() {
            let expected = (i as i32) + 1000;
            let result = map.get_or_insert(*hash, 9999);
            assert_eq!(result, expected, "get_or_insert should return existing value");
        }

        // Test remove
        for (i, hash) in hashes.iter().enumerate() {
            let expected = (i as i32) + 1000;
            let removed = map.remove(hash);
            assert_eq!(removed, Some(expected), "Remove should return the value");

            assert!(!map.contains_key(hash), "Key should not exist after remove");
            assert_eq!(map.get(hash), None);

            // Double remove should return None
            assert_eq!(map.remove(hash), None);
        }

        // Test get_or_insert on empty map (should insert)
        for (i, hash) in hashes.iter().enumerate() {
            let value = (i as i32) + 2000;
            let result = map.get_or_insert(*hash, value);
            assert_eq!(result, value, "get_or_insert on missing key should insert and return value");
            assert_eq!(map.get(hash), Some(value));
        }
    }

    #[test]
    fn test_passthrough_correctness_2_submaps() {
        test_passthrough_correctness::<2>();
    }

    #[test]
    fn test_passthrough_correctness_4_submaps() {
        test_passthrough_correctness::<4>();
    }

    #[test]
    fn test_passthrough_correctness_32_submaps() {
        test_passthrough_correctness::<32>();
    }

    #[test]
    fn test_passthrough_correctness_default_submaps() {
        // Test with default (64 submaps)
        let map: ParallelMerkleHashMap<i32> = ParallelMerkleHashMap::new();
        assert_eq!(map.num_submaps(), 64);
    }

    #[test]
    fn test_passthrough_correctness_1024_submaps() {
        test_passthrough_correctness::<1024>();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_passthrough_parallel_random_operations() {
        const SIZE: usize = 64;
        const NUM_TASKS: usize = 16;
        const OPS_PER_TASK: usize = 5000;

        let map: Arc<ParallelMerkleHashMap<u64, SIZE>> = Arc::new(ParallelMerkleHashMap::new());

        // Pre-generate all hashes to ensure they're distinct
        let all_hashes: Vec<Vec<DataHash>> = (0..NUM_TASKS)
            .map(|task_id| {
                (0..OPS_PER_TASK)
                    .map(|i| compute_data_hash(format!("parallel_t{task_id}_k{i}").as_bytes()))
                    .collect()
            })
            .collect();

        // Verify all hashes across all tasks are distinct
        let all_flat: Vec<_> = all_hashes.iter().flatten().collect();
        let unique: HashSet<_> = all_flat.iter().collect();
        assert_eq!(unique.len(), NUM_TASKS * OPS_PER_TASK, "All hashes must be distinct");

        let handles: Vec<_> = (0..NUM_TASKS)
            .map(|task_id| {
                let map = Arc::clone(&map);
                let hashes = all_hashes[task_id].clone();
                tokio::spawn(async move {
                    // Insert all
                    for (i, hash) in hashes.iter().enumerate() {
                        let value = (task_id * OPS_PER_TASK + i) as u64;
                        map.insert(*hash, value);
                    }

                    // Verify all inserted correctly
                    for (i, hash) in hashes.iter().enumerate() {
                        let expected = (task_id * OPS_PER_TASK + i) as u64;
                        assert!(map.contains_key(hash));
                        assert_eq!(map.get(hash), Some(expected));
                    }

                    // Remove half
                    for hash in hashes.iter().take(OPS_PER_TASK / 2) {
                        map.remove(hash);
                    }

                    // Verify removals
                    for (i, hash) in hashes.iter().enumerate() {
                        if i < OPS_PER_TASK / 2 {
                            assert!(!map.contains_key(hash));
                        } else {
                            assert!(map.contains_key(hash));
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
    fn test_u64_key_parallel_hashmap() {
        type ParallelTruncatedHashMap<Value, const SIZE: usize = 256> =
            ParallelHashMap<u64, Value, U64DirectHasher<u64>, SIZE>;

        let map: ParallelTruncatedHashMap<String> = ParallelTruncatedHashMap::new();
        map.insert(12345, "value1".to_string());
        map.insert(67890, "value2".to_string());

        assert_eq!(map.get(&12345), Some("value1".to_string()));
        assert_eq!(map.get(&67890), Some("value2".to_string()));
        assert!(map.contains_key(&12345));
        assert!(!map.contains_key(&99999));
    }

    // ===========================================
    // Archive serialization/deserialization tests
    // ===========================================

    #[tokio::test]
    async fn test_archive_serialize_deserialize_same_submaps() {
        let temp_dir = tempfile::tempdir().unwrap();
        let archive_path = temp_dir.path().join("archive");

        // Create and populate a map using u64 keys with passthrough hasher
        const SIZE: usize = 16;
        let map: ParallelU64HashMap<i32, SIZE> = ParallelU64HashMap::new();

        for i in 0..100u64 {
            map.insert(i, i as i32);
        }

        // Serialize to archive
        map.serialize_to_archive(&archive_path).await.unwrap();

        // Verify metadata file exists
        assert!(archive_path.join("metadata.bin").exists());

        // Verify hasher file exists
        assert!(archive_path.join("hasher.bin").exists());

        // Verify data files exist (4 parallel files)
        for i in 0..4 {
            assert!(archive_path.join(format!("data_{i}.bin")).exists());
        }

        // Read metadata
        let metadata = ParallelU64HashMapInner::<i32, SIZE>::read_archive_metadata(&archive_path)
            .await
            .unwrap();
        assert_eq!(metadata.num_submaps, SIZE);
        assert_eq!(metadata.total_entries, 100);

        // Deserialize from archive
        let loaded: ParallelU64HashMap<i32, SIZE> =
            ParallelU64HashMap::deserialize_from_archive(&archive_path).await.unwrap();

        // Verify all entries are present
        for i in 0..100u64 {
            assert_eq!(loaded.get(&i), Some(i as i32));
        }
    }

    #[tokio::test]
    async fn test_archive_serialize_deserialize_different_submaps() {
        let temp_dir = tempfile::tempdir().unwrap();
        let archive_path = temp_dir.path().join("archive");

        // Create and populate a map with 16 submaps
        const SOURCE_SIZE: usize = 16;
        let source_map: ParallelU64HashMap<i32, SOURCE_SIZE> = ParallelU64HashMap::new();

        for i in 0..100u64 {
            source_map.insert(i, i as i32);
        }

        // Serialize to archive
        source_map.serialize_to_archive(&archive_path).await.unwrap();

        // Deserialize into a map with different submap count (256 submaps)
        const TARGET_SIZE: usize = 256;
        let loaded: ParallelU64HashMap<i32, TARGET_SIZE> =
            ParallelU64HashMap::deserialize_from_archive(&archive_path).await.unwrap();

        // Verify submap count is different
        assert_ne!(source_map.num_submaps(), loaded.num_submaps());

        // Verify all entries are present (redistributed)
        for i in 0..100u64 {
            assert_eq!(loaded.get(&i), Some(i as i32));
        }
    }

    #[tokio::test]
    async fn test_archive_empty_map() {
        let temp_dir = tempfile::tempdir().unwrap();
        let archive_path = temp_dir.path().join("archive");

        // Create empty map
        const SIZE: usize = 8;
        let map: ParallelU64HashMap<i32, SIZE> = ParallelU64HashMap::new();

        // Serialize empty map
        map.serialize_to_archive(&archive_path).await.unwrap();

        // Read metadata
        let metadata = ParallelU64HashMapInner::<i32, SIZE>::read_archive_metadata(&archive_path)
            .await
            .unwrap();
        assert_eq!(metadata.total_entries, 0);

        // Deserialize
        let loaded: ParallelU64HashMap<i32, SIZE> =
            ParallelU64HashMap::deserialize_from_archive(&archive_path).await.unwrap();
        assert!(!loaded.contains_key(&999));
    }

    #[tokio::test]
    async fn test_archive_deserialize_union_overwrite() {
        let temp_dir = tempfile::tempdir().unwrap();
        let archive_path = temp_dir.path().join("archive");

        const SIZE: usize = 16;

        // Create and serialize an archive with some entries
        let archive_map: ParallelU64HashMap<i32, SIZE> = ParallelU64HashMap::new();
        archive_map.insert(1, 1);
        archive_map.insert(2, 2);
        archive_map.insert(3, 3);
        archive_map.serialize_to_archive(&archive_path).await.unwrap();

        // Create an in-memory map with overlapping and non-overlapping keys
        let memory_map: ParallelU64HashMap<i32, SIZE> = ParallelU64HashMap::new();
        memory_map.insert(2, 200); // overlaps with archive
        memory_map.insert(3, 300); // overlaps with archive
        memory_map.insert(4, 4); // new key

        // Union with overwrite=true: archive values should replace in-memory values
        let count = memory_map.load_and_merge(&archive_path, true).await.unwrap();
        assert_eq!(count, 3); // All 3 archive entries are processed

        assert_eq!(memory_map.get(&1), Some(1)); // from archive
        assert_eq!(memory_map.get(&2), Some(2)); // overwritten by archive
        assert_eq!(memory_map.get(&3), Some(3)); // overwritten by archive
        assert_eq!(memory_map.get(&4), Some(4)); // kept from memory
    }

    #[tokio::test]
    async fn test_archive_deserialize_union_no_overwrite() {
        let temp_dir = tempfile::tempdir().unwrap();
        let archive_path = temp_dir.path().join("archive");

        const SIZE: usize = 16;

        // Create and serialize an archive with some entries
        let archive_map: ParallelU64HashMap<i32, SIZE> = ParallelU64HashMap::new();
        archive_map.insert(1, 1);
        archive_map.insert(2, 2);
        archive_map.insert(3, 3);
        archive_map.serialize_to_archive(&archive_path).await.unwrap();

        // Create an in-memory map with overlapping and non-overlapping keys
        let memory_map: ParallelU64HashMap<i32, SIZE> = ParallelU64HashMap::new();
        memory_map.insert(2, 200); // overlaps with archive
        memory_map.insert(3, 300); // overlaps with archive
        memory_map.insert(4, 4); // new key

        // Union with overwrite=false: in-memory values should be preserved
        let count = memory_map.load_and_merge(&archive_path, false).await.unwrap();
        assert_eq!(count, 1); // Only key 1 was added (2 and 3 already existed)

        assert_eq!(memory_map.get(&1), Some(1)); // from archive
        assert_eq!(memory_map.get(&2), Some(200)); // kept from memory
        assert_eq!(memory_map.get(&3), Some(300)); // kept from memory
        assert_eq!(memory_map.get(&4), Some(4)); // kept from memory
    }

    #[tokio::test]
    async fn test_archive_large_dataset() {
        let temp_dir = tempfile::tempdir().unwrap();
        let archive_path = temp_dir.path().join("archive");

        // Create map with many entries using u64 keys
        const SIZE: usize = 64;
        let map: ParallelU64HashMap<i64, SIZE> = ParallelU64HashMap::new();

        const NUM_ENTRIES: u64 = 10000;
        for i in 0..NUM_ENTRIES {
            map.insert(i, i as i64);
        }

        // Serialize
        map.serialize_to_archive(&archive_path).await.unwrap();

        // Read metadata
        let metadata = ParallelU64HashMapInner::<i64, SIZE>::read_archive_metadata(&archive_path)
            .await
            .unwrap();
        assert_eq!(metadata.total_entries, NUM_ENTRIES as usize);
        assert_eq!(metadata.num_submaps, SIZE);

        // Deserialize
        let loaded: ParallelU64HashMap<i64, SIZE> =
            ParallelU64HashMap::deserialize_from_archive(&archive_path).await.unwrap();

        // Verify all entries
        for i in 0..NUM_ENTRIES {
            assert_eq!(loaded.get(&i), Some(i as i64));
        }
    }

    #[tokio::test]
    async fn test_archive_with_merkle_hash_keys() {
        let temp_dir = tempfile::tempdir().unwrap();
        let archive_path = temp_dir.path().join("archive");

        // Create map with MerkleHash keys
        const SIZE: usize = 16;
        let map: ParallelMerkleHashMap<String, SIZE> = ParallelMerkleHashMap::new();

        let hashes: Vec<_> = (0..50).map(|i| compute_data_hash(format!("data_{i}").as_bytes())).collect();

        for (i, hash) in hashes.iter().enumerate() {
            map.insert(*hash, format!("value_{i}"));
        }

        // Serialize
        map.serialize_to_archive(&archive_path).await.unwrap();

        // Deserialize
        let loaded: ParallelMerkleHashMap<String, SIZE> =
            ParallelMerkleHashMap::deserialize_from_archive(&archive_path).await.unwrap();

        // Verify all entries
        for (i, hash) in hashes.iter().enumerate() {
            assert_eq!(loaded.get(hash), Some(format!("value_{i}")));
        }
    }

    #[tokio::test]
    async fn test_archive_with_different_file_counts() {
        const SIZE: usize = 64;
        const NUM_ENTRIES: u64 = 1000;

        // Test with different numbers of data files
        for num_files in [1, 2, 4, 8, 16, 64] {
            let temp_dir = tempfile::tempdir().unwrap();
            let archive_path = temp_dir.path().join(format!("archive_{num_files}"));

            let map: ParallelU64HashMap<i32, SIZE> = ParallelU64HashMap::new();
            for i in 0..NUM_ENTRIES {
                map.insert(i, i as i32);
            }

            // Serialize with specified number of files
            map.serialize_to_archive_with_concurrency(&archive_path, num_files)
                .await
                .unwrap();

            // Verify metadata
            let metadata = ParallelU64HashMapInner::<i32, SIZE>::read_archive_metadata(&archive_path)
                .await
                .unwrap();
            assert_eq!(metadata.num_files, num_files.min(SIZE));
            assert_eq!(metadata.total_entries, NUM_ENTRIES as usize);

            // Verify correct number of data files exist
            let expected_files = num_files.min(SIZE);
            for file_idx in 0..expected_files {
                let file_path = archive_path.join(format!("data_{file_idx}.bin"));
                assert!(file_path.exists(), "data_{file_idx}.bin should exist for num_files={num_files}");
            }

            // Deserialize and verify all entries
            let loaded: ParallelU64HashMap<i32, SIZE> =
                ParallelU64HashMap::deserialize_from_archive(&archive_path).await.unwrap();

            for i in 0..NUM_ENTRIES {
                assert_eq!(loaded.get(&i), Some(i as i32), "Entry {i} missing for num_files={num_files}");
            }
        }
    }

    #[tokio::test]
    async fn test_archive_cross_file_count_compatibility() {
        // Serialize with 1 file, deserialize into maps with various submap counts
        let temp_dir = tempfile::tempdir().unwrap();
        let archive_path = temp_dir.path().join("archive");

        const SOURCE_SIZE: usize = 32;
        const NUM_ENTRIES: u64 = 500;

        let source_map: ParallelU64HashMap<i32, SOURCE_SIZE> = ParallelU64HashMap::new();
        for i in 0..NUM_ENTRIES {
            source_map.insert(i, i as i32);
        }

        // Serialize with just 1 file
        source_map
            .serialize_to_archive_with_concurrency(&archive_path, 1)
            .await
            .unwrap();

        // Verify only 1 data file exists
        assert!(archive_path.join("data_0.bin").exists());
        assert!(!archive_path.join("data_1.bin").exists());

        // Deserialize into map with different submap count
        const TARGET_SIZE: usize = 128;
        let loaded: ParallelU64HashMap<i32, TARGET_SIZE> =
            ParallelU64HashMap::deserialize_from_archive(&archive_path).await.unwrap();

        // Verify all entries
        for i in 0..NUM_ENTRIES {
            assert_eq!(loaded.get(&i), Some(i as i32));
        }
    }
}
