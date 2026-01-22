use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::sync::Mutex;

use more_asserts::debug_assert_lt;

pub const DEFAULT_WRITE_CONCURRENCY: usize = 4;
pub const DEFAULT_READ_CONCURRENCY: usize = 16;

pub(super) struct ParallelHashMapInner<Key, Value, S, const FIRST_LEVEL_SIZE: usize> {
    pub(super) hasher: S,
    pub(super) submaps: [Mutex<HashMap<Key, Value, S>>; FIRST_LEVEL_SIZE],
}

impl<Key, Value, S, const FIRST_LEVEL_SIZE: usize> ParallelHashMapInner<Key, Value, S, FIRST_LEVEL_SIZE>
where
    Key: Hash + Eq,
    Value: Clone,
    S: BuildHasher + Default + Clone,
{
    pub(super) const FIRST_LEVEL_BITS: usize = {
        assert!(FIRST_LEVEL_SIZE >= 2);
        assert!(FIRST_LEVEL_SIZE.is_power_of_two());
        FIRST_LEVEL_SIZE.trailing_zeros() as usize
    };

    pub(super) fn new(hasher: S) -> Self {
        let submaps = std::array::from_fn(|_| Mutex::new(HashMap::with_hasher(hasher.clone())));
        Self { hasher, submaps }
    }

    /// Creates a new instance with pre-allocated capacity for each submap.
    #[cfg(not(target_family = "wasm"))]
    fn with_submap_capacities(hasher: &S, capacities: &[usize]) -> Self {
        let submaps = std::array::from_fn(|i| {
            let capacity = capacities.get(i).copied().unwrap_or(0);
            Mutex::new(HashMap::with_capacity_and_hasher(capacity, hasher.clone()))
        });
        Self {
            hasher: hasher.clone(),
            submaps,
        }
    }

    #[inline]
    pub(super) fn submap_index(&self, key: &Key) -> usize {
        let hash = self.hasher.hash_one(key);
        let submap_bits = (hash >> (64 - Self::FIRST_LEVEL_BITS)) as usize;
        debug_assert_lt!(submap_bits, FIRST_LEVEL_SIZE);
        submap_bits
    }

    pub(super) fn contains_key(&self, key: &Key) -> bool {
        let index = self.submap_index(key);
        let guard = self.submaps[index].lock().unwrap();
        guard.contains_key(key)
    }

    pub(super) fn insert(&self, key: Key, value: Value) -> Option<Value> {
        let index = self.submap_index(&key);
        let mut guard = self.submaps[index].lock().unwrap();
        guard.insert(key, value)
    }

    pub(super) fn remove(&self, key: &Key) -> Option<Value> {
        let index = self.submap_index(key);
        let mut guard = self.submaps[index].lock().unwrap();
        guard.remove(key)
    }

    pub(super) fn get(&self, key: &Key) -> Option<Value> {
        let index = self.submap_index(key);
        let guard = self.submaps[index].lock().unwrap();
        guard.get(key).cloned()
    }

    pub(super) fn get_or_insert(&self, key: Key, value: Value) -> Value {
        let index = self.submap_index(&key);
        let mut guard = self.submaps[index].lock().unwrap();
        guard.entry(key).or_insert(value).clone()
    }
}

/// Archive serialization/deserialization support for non-WASM targets.
#[cfg(not(target_family = "wasm"))]
pub(super) mod archive {
    use std::collections::hash_map::Entry;
    use std::fs::{self, File};
    use std::hash::{BuildHasher, Hash};
    use std::io::{self, BufReader, BufWriter, ErrorKind, Seek};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};
    use tokio::task::JoinSet;

    use super::ParallelHashMapInner;

    const METADATA_FILENAME: &str = "metadata.bin";
    const HASHER_FILENAME: &str = "hasher.bin";
    const WRITE_BUFFER_SIZE: usize = 1024 * 1024; // 1MB buffer for optimal disk throughput
    const READ_BUFFER_SIZE: usize = 1024 * 1024; // 1MB buffer for optimal disk throughput

    // Magic number: "PHASMAP\0" in little-endian
    const FILE_MAGIC: u64 = 0x0050_414D_5341_4850;
    const FILE_VERSION: u32 = 1;

    /// Location of a submap within the archive files.
    #[derive(Serialize, Deserialize, Debug, Clone, Default)]
    pub struct SubmapLocation {
        pub file_index: usize,
        pub byte_offset: u64,
        pub entry_count: usize,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct ArchiveMetadata {
        pub first_level_bits: usize,
        pub num_submaps: usize,
        pub total_entries: usize,
        pub num_files: usize,
        /// Location of each submap within the data files.
        pub submap_locations: Vec<SubmapLocation>,
    }

    fn metadata_path(dir: &Path) -> PathBuf {
        dir.join(METADATA_FILENAME)
    }

    fn hasher_path(dir: &Path) -> PathBuf {
        dir.join(HASHER_FILENAME)
    }

    fn data_file_path(dir: &Path, file_index: usize) -> PathBuf {
        dir.join(format!("data_{file_index}.bin"))
    }

    fn invalid_data_error(msg: impl Into<String>) -> io::Error {
        io::Error::new(ErrorKind::InvalidData, msg.into())
    }

    /// Reads hasher from archive directory.
    fn read_hasher<S>(dir: &Path) -> io::Result<S>
    where
        S: for<'de> Deserialize<'de>,
    {
        let hasher_bytes = fs::read(hasher_path(dir))?;
        let hasher: S = bincode::deserialize(&hasher_bytes).map_err(|e| invalid_data_error(e.to_string()))?;
        Ok(hasher)
    }

    pub fn read_metadata<S>(dir: &Path) -> io::Result<(ArchiveMetadata, S)>
    where
        S: for<'de> Deserialize<'de>,
    {
        let metadata_bytes = fs::read(metadata_path(dir))?;
        let metadata: ArchiveMetadata =
            bincode::deserialize(&metadata_bytes).map_err(|e| invalid_data_error(e.to_string()))?;
        let hasher = read_hasher(dir)?;
        Ok((metadata, hasher))
    }

    /// Returns the range of submap indices for a given file.
    fn submaps_for_file(file_index: usize, num_submaps: usize, num_files: usize) -> std::ops::Range<usize> {
        if num_files == 0 || num_submaps == 0 {
            return 0..0;
        }

        let submaps_per_file = num_submaps / num_files;
        if submaps_per_file == 0 {
            // Less submaps than files: each file gets at most one
            let start = file_index.min(num_submaps);
            let end = (file_index + 1).min(num_submaps);
            start..end
        } else {
            let start = file_index * submaps_per_file;
            let end = if file_index == num_files - 1 {
                num_submaps // Last file gets any remainder
            } else {
                (file_index + 1) * submaps_per_file
            };
            start..end
        }
    }

    /// Header written at the start of each data file.
    #[derive(Debug)]
    struct DataFileHeader {
        magic: u64,
        version: u32,
        file_index: u32,
        num_submaps: u64,
    }

    impl DataFileHeader {
        fn write_to<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
            bincode::serialize_into(&mut *writer, &self.magic).map_err(|e| invalid_data_error(e.to_string()))?;
            bincode::serialize_into(&mut *writer, &self.version).map_err(|e| invalid_data_error(e.to_string()))?;
            bincode::serialize_into(&mut *writer, &self.file_index).map_err(|e| invalid_data_error(e.to_string()))?;
            bincode::serialize_into(&mut *writer, &self.num_submaps).map_err(|e| invalid_data_error(e.to_string()))?;
            Ok(())
        }

        #[allow(dead_code)]
        fn read_from<R: io::Read>(reader: &mut R) -> io::Result<Self> {
            let magic: u64 = bincode::deserialize_from(&mut *reader).map_err(|e| invalid_data_error(e.to_string()))?;
            if magic != FILE_MAGIC {
                return Err(invalid_data_error("Invalid file magic number"));
            }
            let version: u32 =
                bincode::deserialize_from(&mut *reader).map_err(|e| invalid_data_error(e.to_string()))?;
            if version != FILE_VERSION {
                return Err(invalid_data_error(format!("Unsupported file version: {version}")));
            }
            let file_index: u32 =
                bincode::deserialize_from(&mut *reader).map_err(|e| invalid_data_error(e.to_string()))?;
            let num_submaps: u64 =
                bincode::deserialize_from(&mut *reader).map_err(|e| invalid_data_error(e.to_string()))?;
            Ok(Self {
                magic,
                version,
                file_index,
                num_submaps,
            })
        }
    }

    // Inner type: actual archive implementation with self: Arc<Self> methods
    impl<Key, Value, S, const FIRST_LEVEL_SIZE: usize> ParallelHashMapInner<Key, Value, S, FIRST_LEVEL_SIZE>
    where
        Key: Hash + Eq + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        Value: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
        S: BuildHasher + Default + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    {
        /// Serializes to an archive directory using a specified number of parallel data files.
        /// Each file is self-describing and can be read independently.
        ///
        /// # Arguments
        /// * `dir` - The directory to write the archive to
        /// * `write_concurrency` - Number of data files to create (parallelism level for I/O)
        pub async fn serialize_to_archive(
            self: Arc<Self>,
            dir: impl AsRef<Path>,
            write_concurrency: usize,
        ) -> io::Result<()> {
            let dir = dir.as_ref().to_path_buf();
            fs::create_dir_all(&dir)?;

            // Ensure at least 1 file, and cap at number of submaps
            let num_files = write_concurrency.max(1).min(FIRST_LEVEL_SIZE);

            let mut join_set: JoinSet<io::Result<Vec<SubmapLocation>>> = JoinSet::new();

            // Spawn parallel tasks, one for each data file
            for file_idx in 0..num_files {
                let inner = self.clone();
                let dir = dir.clone();

                join_set.spawn(async move {
                    tokio::task::spawn_blocking(move || {
                        let submap_range = submaps_for_file(file_idx, FIRST_LEVEL_SIZE, num_files);
                        if submap_range.is_empty() {
                            return Ok(Vec::new());
                        }

                        let path = data_file_path(&dir, file_idx);
                        let file = File::create(&path)?;
                        let mut writer = BufWriter::with_capacity(WRITE_BUFFER_SIZE, file);

                        // Write file header
                        let header = DataFileHeader {
                            magic: FILE_MAGIC,
                            version: FILE_VERSION,
                            file_index: file_idx as u32,
                            num_submaps: submap_range.len() as u64,
                        };
                        header.write_to(&mut writer)?;

                        let mut locations = Vec::with_capacity(submap_range.len());

                        // Write each submap
                        for submap_idx in submap_range {
                            // Record byte offset before writing this submap
                            let byte_offset = writer.stream_position()?;

                            let guard = inner.submaps[submap_idx].lock().unwrap();
                            let count = guard.len() as u64;

                            // Write submap index and count
                            bincode::serialize_into(&mut writer, &(submap_idx as u64))
                                .map_err(|e| invalid_data_error(e.to_string()))?;
                            bincode::serialize_into(&mut writer, &count)
                                .map_err(|e| invalid_data_error(e.to_string()))?;

                            // Stream entries
                            for (k, v) in guard.iter() {
                                bincode::serialize_into(&mut writer, k)
                                    .map_err(|e| invalid_data_error(e.to_string()))?;
                                bincode::serialize_into(&mut writer, v)
                                    .map_err(|e| invalid_data_error(e.to_string()))?;
                            }

                            locations.push(SubmapLocation {
                                file_index: file_idx,
                                byte_offset,
                                entry_count: count as usize,
                            });
                        }

                        drop(writer);
                        Ok(locations)
                    })
                    .await
                    .map_err(io::Error::other)?
                });
            }

            // Collect results and build metadata
            let mut all_locations: Vec<SubmapLocation> = vec![SubmapLocation::default(); FIRST_LEVEL_SIZE];
            let mut total_entries = 0usize;

            while let Some(result) = join_set.join_next().await {
                let locations = result.map_err(io::Error::other)??;
                for loc in locations {
                    // Determine the global submap index from file_index and position
                    let submap_range = submaps_for_file(loc.file_index, FIRST_LEVEL_SIZE, num_files);
                    let local_idx = all_locations[submap_range.start..submap_range.end]
                        .iter()
                        .position(|l| l.entry_count == 0 && l.file_index == 0 && l.byte_offset == 0)
                        .unwrap_or(0);
                    let global_idx = submap_range.start + local_idx;
                    total_entries += loc.entry_count;
                    all_locations[global_idx] = loc;
                }
            }

            let metadata = ArchiveMetadata {
                first_level_bits: Self::FIRST_LEVEL_BITS,
                num_submaps: FIRST_LEVEL_SIZE,
                total_entries,
                num_files,
                submap_locations: all_locations,
            };

            let metadata_bytes = bincode::serialize(&metadata).map_err(|e| invalid_data_error(e.to_string()))?;
            fs::write(metadata_path(&dir), metadata_bytes)?;

            let hasher_bytes = bincode::serialize(&self.hasher).map_err(|e| invalid_data_error(e.to_string()))?;
            fs::write(hasher_path(&dir), hasher_bytes)?;

            Ok(())
        }

        /// Deserializes from an archive directory into a new instance.
        /// Uses parallel readers based on submap locations in metadata.
        ///
        /// # Arguments
        /// * `dir` - Archive directory
        /// * `read_concurrency` - Number of parallel readers (each opens the file and seeks to its submap)
        pub async fn deserialize_from_archive(dir: impl AsRef<Path>, read_concurrency: usize) -> io::Result<Self> {
            let dir = dir.as_ref();

            // Read metadata for pre-sizing and submap locations
            let (metadata, hasher): (ArchiveMetadata, S) = read_metadata(dir)?;

            let capacities: Vec<usize> = metadata.submap_locations.iter().map(|l| l.entry_count).collect();

            let inner = if capacities.len() == FIRST_LEVEL_SIZE {
                Arc::new(Self::with_submap_capacities(&hasher, &capacities))
            } else {
                Arc::new(Self::new(hasher))
            };

            inner
                .clone()
                .load_with_parallel_readers(dir, &metadata, read_concurrency, true)
                .await?;

            Ok(Arc::into_inner(inner).expect("No other references should exist"))
        }

        /// Loads and merges from an archive directory into the current map.
        ///
        /// # Arguments
        /// * `dir` - Archive directory
        /// * `read_concurrency` - Number of parallel readers
        /// * `overwrite` - Whether to overwrite existing entries
        pub async fn load_and_merge_from_archive(
            self: Arc<Self>,
            dir: impl AsRef<Path>,
            read_concurrency: usize,
            overwrite: bool,
        ) -> io::Result<usize> {
            let dir = dir.as_ref();
            let (metadata, _): (ArchiveMetadata, S) = read_metadata(dir)?;
            self.load_with_parallel_readers(dir, &metadata, read_concurrency, overwrite)
                .await
        }

        #[cfg(test)]
        pub async fn read_archive_metadata(dir: impl AsRef<Path>) -> io::Result<ArchiveMetadata> {
            let (metadata, _): (ArchiveMetadata, S) = read_metadata(dir.as_ref())?;
            Ok(metadata)
        }

        /// Loads entries using parallel readers that can read from the same file simultaneously.
        /// Each reader opens its own file handle and seeks to the submap's byte offset.
        ///
        /// # Arguments
        /// * `dir` - Archive directory
        /// * `metadata` - Archive metadata containing submap locations
        /// * `read_concurrency` - Maximum number of parallel readers
        /// * `overwrite` - Whether to overwrite existing entries
        async fn load_with_parallel_readers(
            self: Arc<Self>,
            dir: &Path,
            metadata: &ArchiveMetadata,
            read_concurrency: usize,
            overwrite: bool,
        ) -> io::Result<usize> {
            use tokio::sync::Semaphore;

            let read_concurrency = read_concurrency.max(1);
            let semaphore = Arc::new(Semaphore::new(read_concurrency));
            let mut join_set: JoinSet<io::Result<usize>> = JoinSet::new();

            // Spawn a task for each submap, using its byte offset to seek directly
            for location in metadata.submap_locations.iter() {
                // Skip empty submaps
                if location.entry_count == 0 {
                    continue;
                }

                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let inner = self.clone();
                let path = data_file_path(dir, location.file_index);
                let byte_offset = location.byte_offset;
                let entry_count = location.entry_count;
                let num_disk_submaps = metadata.num_submaps;

                join_set.spawn(async move {
                    tokio::task::spawn_blocking(move || {
                        let file = File::open(&path)?;
                        let mut reader = BufReader::with_capacity(READ_BUFFER_SIZE, file);

                        // Seek to the submap's byte offset
                        reader.seek(io::SeekFrom::Start(byte_offset))?;

                        // Read submap index and count
                        let disk_submap_idx: u64 =
                            bincode::deserialize_from(&mut reader).map_err(|e| invalid_data_error(e.to_string()))?;
                        let count: u64 =
                            bincode::deserialize_from(&mut reader).map_err(|e| invalid_data_error(e.to_string()))?;

                        // Sanity check
                        if count as usize != entry_count {
                            return Err(invalid_data_error(format!(
                                "Entry count mismatch: metadata says {}, file says {}",
                                entry_count, count
                            )));
                        }

                        let total_inserted = if num_disk_submaps == FIRST_LEVEL_SIZE {
                            // Direct load: all entries go to the same submap, acquire lock once
                            let target_idx = disk_submap_idx as usize;
                            if target_idx >= FIRST_LEVEL_SIZE {
                                return Err(invalid_data_error(format!("Invalid submap index: {target_idx}")));
                            }

                            let mut guard = inner.submaps[target_idx].lock().unwrap();
                            let mut inserted = 0usize;

                            for _ in 0..count {
                                let k: Key = bincode::deserialize_from(&mut reader)
                                    .map_err(|e| invalid_data_error(e.to_string()))?;
                                let v: Value = bincode::deserialize_from(&mut reader)
                                    .map_err(|e| invalid_data_error(e.to_string()))?;

                                if overwrite {
                                    guard.insert(k, v);
                                    inserted += 1;
                                } else if let Entry::Vacant(e) = guard.entry(k) {
                                    e.insert(v);
                                    inserted += 1;
                                }
                            }
                            inserted
                        } else {
                            // Redistribute: entries may go to different submaps, rehash each key
                            let mut inserted = 0usize;

                            for _ in 0..count {
                                let k: Key = bincode::deserialize_from(&mut reader)
                                    .map_err(|e| invalid_data_error(e.to_string()))?;
                                let v: Value = bincode::deserialize_from(&mut reader)
                                    .map_err(|e| invalid_data_error(e.to_string()))?;

                                let target_idx = inner.submap_index(&k);

                                let mut guard = inner.submaps[target_idx].lock().unwrap();
                                if overwrite {
                                    guard.insert(k, v);
                                    inserted += 1;
                                } else if let Entry::Vacant(e) = guard.entry(k) {
                                    e.insert(v);
                                    inserted += 1;
                                }
                            }
                            inserted
                        };

                        drop(permit);
                        Ok(total_inserted)
                    })
                    .await
                    .map_err(io::Error::other)?
                });
            }

            let mut total = 0;
            while let Some(result) = join_set.join_next().await {
                total += result.map_err(io::Error::other)??;
            }
            Ok(total)
        }
    }
}
