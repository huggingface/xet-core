use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Cursor, ErrorKind, Read, Seek, Write};
use std::ops::Add;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use heapify::{make_heap_with, pop_heap_with};
use merklehash::{compute_data_hash, HMACKey, HashedWrite, MerkleHash};
use tracing::{debug, error, info, warn};

use crate::cas_structs::CASChunkSequenceHeader;
use crate::constants::MDB_SHARD_EXPIRATION_BUFFER_SECS;
use crate::error::{MDBShardError, Result};
use crate::file_structs::{FileDataSequenceEntry, MDBFileInfo};
use crate::shard_file::current_timestamp;
use crate::shard_format::MDBShardInfo;
use crate::utils::{parse_shard_filename, shard_file_name, temp_shard_file_name, truncate_hash};
use crate::MDBShardFileFooter;

/// When a specific implementation of the  
#[derive(Debug)]
pub struct MDBShardFile {
    pub shard_hash: MerkleHash,
    pub path: PathBuf,
    pub shard: MDBShardInfo,
    pub last_modified_time: SystemTime,

    // On occation, to test a corrupt shard, we need to disable the verification process.
    #[cfg(debug_assertions)]
    pub disable_verifications: std::sync::atomic::AtomicBool,
}

impl Default for MDBShardFile {
    fn default() -> Self {
        Self {
            shard_hash: MerkleHash::default(),
            path: PathBuf::default(),
            shard: MDBShardInfo::default(),
            last_modified_time: SystemTime::UNIX_EPOCH,
            #[cfg(debug_assertions)]
            disable_verifications: false.into(),
        }
    }
}

lazy_static::lazy_static! {
    static ref MDB_SHARD_FILE_CACHE: RwLock<HashMap<PathBuf, Arc<MDBShardFile>>> = RwLock::new(HashMap::default());
}

impl MDBShardFile {
    pub fn new(shard_hash: MerkleHash, path: PathBuf, shard: MDBShardInfo) -> Result<Arc<Self>> {
        let s = Arc::new(Self {
            last_modified_time: std::fs::metadata(&path)?.modified()?,
            shard_hash,
            path,
            shard,
            #[cfg(debug_assertions)]
            disable_verifications: false.into(),
        });

        s.verify_shard_integrity_debug_only();
        Ok(s)
    }

    pub fn copy_into_target_directory(&self, target_directory: impl AsRef<Path>) -> Result<Arc<Self>> {
        Self::write_out_from_reader(target_directory, &mut self.get_reader()?)
    }

    pub fn write_out_from_reader<R: Read>(target_directory: impl AsRef<Path>, reader: &mut R) -> Result<Arc<Self>> {
        let target_directory = target_directory.as_ref();

        let mut hashed_write; // Need to access after file is closed.

        let temp_file_name = target_directory.join(temp_shard_file_name());

        {
            // Scoped so that file is closed and flushed before name is changed.

            let out_file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp_file_name)?;

            hashed_write = HashedWrite::new(out_file);

            std::io::copy(reader, &mut hashed_write)?;
            hashed_write.flush()?;
        }

        // Get the hash
        let shard_hash = hashed_write.hash();

        let full_file_name = target_directory.join(shard_file_name(&shard_hash));

        std::fs::rename(&temp_file_name, &full_file_name)?;

        Self::load_from_hash_and_path(shard_hash, &full_file_name)
    }

    pub fn export_with_expiration(
        &self,
        target_directory: impl AsRef<Path>,
        shard_valid_for: Duration,
    ) -> Result<Arc<Self>> {
        let now = SystemTime::now();
        self.export_with_specific_expiration(target_directory, now.add(shard_valid_for), now)
    }

    pub fn export_with_specific_expiration(
        &self,
        target_directory: impl AsRef<Path>,
        expiration: SystemTime,
        creation_time: SystemTime,
    ) -> Result<Arc<Self>> {
        // New footer with the proper expiration added.
        let mut out_footer = self.shard.metadata.clone();

        out_footer.shard_key_expiry = expiration.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
        out_footer.shard_creation_timestamp = creation_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut out_footer_bytes = Vec::<u8>::with_capacity(std::mem::size_of::<MDBShardFileFooter>());
        out_footer.serialize(&mut out_footer_bytes)?;

        let reader = File::open(&self.path)?;

        Self::write_out_from_reader(
            target_directory,
            &mut reader.take(out_footer.footer_offset).chain(Cursor::new(out_footer_bytes)),
        )
    }

    fn load_from_hash_and_path(shard_hash: MerkleHash, path: &Path) -> Result<Arc<Self>> {
        let path = std::path::absolute(path)?;

        // First see if it's in the shard file cache.
        {
            let lg = MDB_SHARD_FILE_CACHE.read().unwrap();
            if let Some(sf) = lg.get(&path) {
                return Ok(sf.clone());
            }
        }

        let mut f = std::fs::File::open(&path)?;

        let sf = Arc::new(Self {
            shard_hash,
            path: path.clone(),
            last_modified_time: f.metadata()?.modified()?,
            shard: MDBShardInfo::load_from_reader(&mut f)?,
            #[cfg(debug_assertions)]
            disable_verifications: false.into(),
        });

        MDB_SHARD_FILE_CACHE.write().unwrap().insert(path, sf.clone());

        Ok(sf)
    }

    fn drop_from_cache(self: Arc<Self>) {
        MDB_SHARD_FILE_CACHE.write().unwrap().remove_entry(&self.path);
    }

    pub fn purge_if_needed(self: Arc<Self>) {
        // If the file no longer exists or isn't the correct length, then purge it from the cache.
        if !self.path.exists() || self.shard.num_bytes() != self.path.metadata().map(|m| m.len()).unwrap_or(0) {
            info!("Purging shard file from cache: {:?}", self.path);
            self.drop_from_cache();
        }
    }

    /// Loads the MDBShardFile struct from a file path
    pub fn load_from_file(path: &Path) -> Result<Arc<Self>> {
        if let Some(shard_hash) = parse_shard_filename(path.to_str().unwrap()) {
            Self::load_from_hash_and_path(shard_hash, path)
        } else {
            Err(MDBShardError::BadFilename(format!("{path:?} not a valid MerkleDB filename.")))
        }
    }

    pub fn load_all_valid(path: impl AsRef<Path>) -> Result<Vec<Arc<Self>>> {
        Self::load_managed_directory(path, true, false, false, 0)
    }

    pub fn load_managed_directory(
        path: impl AsRef<Path>,
        skip_on_error: bool,
        load_expired: bool,
        prune_expired: bool,
        prune_dir_storage_to_size: u64,
    ) -> Result<Vec<Arc<Self>>> {
        let current_time = current_timestamp();
        let expiration_buffer = *MDB_SHARD_EXPIRATION_BUFFER_SECS;

        let mut ret: Vec<Arc<MDBShardFile>> = Vec::new();

        let mut total_size = 0;

        Self::scan_impl(path, skip_on_error, |s| {
            if load_expired || current_time <= s.shard.metadata.shard_key_expiry {
                total_size += s.shard.num_bytes();
                ret.push(s);
            } else if prune_expired
                && s.shard.metadata.shard_key_expiry.saturating_add(expiration_buffer) <= current_time
            {
                info!("Deleting expired shard {:?}", &s.path);
                let _ = std::fs::remove_file(&s.path);
                Self::drop_from_cache(s);
            }

            Ok(())
        })?;

        // Do we need to prune the directory to keep things down to size?
        if prune_dir_storage_to_size != 0 && total_size > prune_dir_storage_to_size {
            // Flush out the oldest ones first using a heap.

            let heap_predicate = |s1: &Arc<MDBShardFile>, s2: &Arc<MDBShardFile>| {
                // Compare in reverse so pop is done from earliest shard
                s2.shard
                    .metadata
                    .shard_creation_timestamp
                    .partial_cmp(&s1.shard.metadata.shard_creation_timestamp)
            };

            // Turn the return shards into a heap around the shard creation timestamp
            make_heap_with(&mut ret, heap_predicate);

            while total_size > prune_dir_storage_to_size {
                pop_heap_with(&mut ret, heap_predicate);
                let Some(s) = ret.pop() else {
                    break;
                };

                info!("Pruning shard to maintain cache size: {:?}", &s.path);
                total_size -= s.shard.num_bytes();
                let _ = std::fs::remove_file(&s.path);
                Self::drop_from_cache(s);
            }
        }

        Ok(ret)
    }

    pub fn clean_shard_cache(path: impl AsRef<Path>, expiration_buffer_secs: u64) -> Result<()> {
        let current_time = current_timestamp();

        Self::scan_impl(path, true, |s| {
            if s.shard.metadata.shard_key_expiry.saturating_add(expiration_buffer_secs) <= current_time {
                info!("Deleting expired shard {:?}", &s.path);
                let _ = std::fs::remove_file(&s.path);
            }

            Ok(())
        })?;

        Ok(())
    }

    // Attempts to read the entire thing into memory.
    pub fn read_into_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
        buffer.resize(self.shard.num_bytes() as usize, 0);

        std::fs::File::open(&self.path)?.read_exact(buffer)?;
        Ok(())
    }

    #[inline]
    fn scan_impl(
        path: impl AsRef<Path>,
        skip_on_error: bool,
        mut callback: impl FnMut(Arc<Self>) -> Result<()>,
    ) -> Result<()> {
        let path = path.as_ref();

        let mut load_file = |h: MerkleHash, file_name: &Path| -> Result<()> {
            let s_res = Self::load_from_hash_and_path(h, file_name);

            let s = match s_res {
                Ok(s) => s,
                Err(e) => {
                    if skip_on_error {
                        info!("Error loading shard {file_name:?}: {e}; skipping.");
                        return Ok(());
                    } else {
                        error!("Error reading shard {file_name:?}: {e}; skipping.");
                        return Err(e);
                    }
                },
            };

            s.verify_shard_integrity_debug_only();
            callback(s)?;
            debug!("Registerd shard file '{file_name:?}'.");
            Ok(())
        };

        if path.is_dir() {
            for entry in std::fs::read_dir(path)? {
                if entry.is_err() && skip_on_error {
                    continue;
                }

                let entry = entry?;

                if let Some(h) = entry.file_name().to_str().and_then(parse_shard_filename) {
                    load_file(h, &path.join(entry.file_name()))?;
                }
            }
        } else if let Some(h) = path.file_name().and_then(parse_shard_filename) {
            load_file(h, path)?;
        } else {
            return Err(MDBShardError::BadFilename(format!("Filename {path:?} not valid shard file name.")));
        }

        Ok(())
    }

    /// Write out the current shard, re-keyed with an hmac key, to the output directory in question, returning
    /// the full path to the new shard.
    pub fn export_as_keyed_shard(
        &self,
        target_directory: impl AsRef<Path>,
        hmac_key: HMACKey,
        key_valid_for: Duration,
        include_file_info: bool,
        include_cas_lookup_table: bool,
        include_chunk_lookup_table: bool,
    ) -> Result<Arc<Self>> {
        let mut output_bytes = Vec::<u8>::new();

        self.shard.export_as_keyed_shard(
            &mut self.get_reader()?,
            &mut output_bytes,
            hmac_key,
            key_valid_for,
            include_file_info,
            include_cas_lookup_table,
            include_chunk_lookup_table,
        )?;

        let written_out = Self::write_out_from_reader(target_directory, &mut Cursor::new(output_bytes))?;
        written_out.verify_shard_integrity_debug_only();

        Ok(written_out)
    }

    #[inline]
    pub fn read_all_cas_blocks(&self) -> Result<Vec<(CASChunkSequenceHeader, u64)>> {
        self.shard.read_all_cas_blocks(&mut self.get_reader()?)
    }

    pub fn get_reader(&self) -> Result<BufReader<std::fs::File>> {
        Ok(BufReader::with_capacity(2048, std::fs::File::open(&self.path)?))
    }

    // Helper function to swallow io::ErrorKind::NotFound errors. In the case of
    // a cached shard was registered but later deleted during the lifetime
    // of a shard file manager, queries to this shard should not fail hard.
    pub fn get_reader_if_present(&self) -> Result<Option<BufReader<std::fs::File>>> {
        match self.get_reader() {
            Ok(v) => Ok(Some(v)),
            Err(MDBShardError::IOError(e)) => {
                if e.kind() == ErrorKind::NotFound {
                    Ok(None)
                } else {
                    Err(MDBShardError::IOError(e))
                }
            },
            Err(other_err) => Err(other_err),
        }
    }

    #[inline]
    pub fn get_file_reconstruction_info(&self, file_hash: &MerkleHash) -> Result<Option<MDBFileInfo>> {
        let Some(mut reader) = self.get_reader_if_present()? else {
            return Ok(None);
        };

        self.shard.get_file_reconstruction_info(&mut reader, file_hash)
    }

    #[inline]
    pub fn chunk_hash_dedup_query(
        &self,
        query_hashes: &[MerkleHash],
    ) -> Result<Option<(usize, FileDataSequenceEntry)>> {
        let Some(mut reader) = self.get_reader_if_present()? else {
            return Ok(None);
        };

        self.shard.chunk_hash_dedup_query(&mut reader, query_hashes)
    }

    #[inline]
    pub fn chunk_hash_dedup_query_direct(
        &self,
        query_hashes: &[MerkleHash],
        cas_block_index: u32,
        cas_chunk_offset: u32,
    ) -> Result<Option<(usize, FileDataSequenceEntry)>> {
        let Some(mut reader) = self.get_reader_if_present()? else {
            return Ok(None);
        };

        self.shard
            .chunk_hash_dedup_query_direct(&mut reader, query_hashes, cas_block_index, cas_chunk_offset)
    }

    #[inline]
    pub fn chunk_hmac_key(&self) -> Option<HMACKey> {
        self.shard.chunk_hmac_key()
    }

    #[inline]
    pub fn read_all_truncated_hashes(&self) -> Result<Vec<(u64, (u32, u32))>> {
        self.shard.read_all_truncated_hashes(&mut self.get_reader()?)
    }

    #[inline]
    pub fn read_full_cas_lookup(&self) -> Result<Vec<(u64, u32)>> {
        self.shard.read_full_cas_lookup(&mut self.get_reader()?)
    }

    #[inline]
    pub fn read_all_file_info_sections(&self) -> Result<Vec<MDBFileInfo>> {
        self.shard.read_all_file_info_sections(&mut self.get_reader()?)
    }

    #[inline]
    pub fn verify_shard_integrity_debug_only(&self) {
        #[cfg(debug_assertions)]
        {
            if !self.disable_verifications.load(std::sync::atomic::Ordering::Relaxed) {
                self.verify_shard_integrity();
            }
        }
    }

    pub fn verify_shard_integrity(&self) {
        debug!("Verifying shard integrity for shard {:?}", &self.path);

        debug!("Header : {:?}", self.shard.header);
        debug!("Metadata : {:?}", self.shard.metadata);

        let mut reader = self
            .get_reader()
            .map_err(|e| {
                error!("Error getting reader: {e:?}");
                e
            })
            .unwrap();

        let mut data = Vec::with_capacity(self.shard.num_bytes() as usize);
        reader.read_to_end(&mut data).unwrap();

        // Check the hash
        let hash = compute_data_hash(&data[..]);
        assert_eq!(hash, self.shard_hash);

        // Check the parsed shard from the filename.
        let parsed_shard_hash = parse_shard_filename(&self.path).unwrap();
        assert_eq!(hash, parsed_shard_hash);

        reader.rewind().unwrap();

        // Check the parsed shard from the filename.
        if let Some(parsed_shard_hash) = parse_shard_filename(&self.path) {
            if hash != parsed_shard_hash {
                error!("Hash parsed from filename does not match the computed hash; hash from filename={parsed_shard_hash:?}, hash of file={hash:?}");
            }
        } else {
            warn!("Unable to obtain hash from filename.");
        }

        // Check the file info sections
        reader.rewind().unwrap();

        let fir = MDBShardInfo::read_file_info_ranges(&mut reader)
            .map_err(|e| {
                error!("Error reading file info ranges : {e:?}");
                e
            })
            .unwrap();

        if self.shard.metadata.file_lookup_num_entry != 0 {
            debug_assert_eq!(fir.len() as u64, self.shard.metadata.file_lookup_num_entry);
        }
        debug!("Integrity test passed for shard {:?}", &self.path);

        // Verify that the shard chunk lookup tables are correct.

        // Read from the lookup table section.
        let mut read_truncated_hashes = self.read_all_truncated_hashes().unwrap();

        let mut truncated_hashes = Vec::new();

        let cas_blocks = self.shard.read_all_cas_blocks_full(&mut self.get_reader().unwrap()).unwrap();

        // Read from the cas blocks
        let mut cas_index = 0;
        for ci in cas_blocks {
            for (i, chunk) in ci.chunks.iter().enumerate() {
                truncated_hashes.push((truncate_hash(&chunk.chunk_hash), (cas_index as u32, i as u32)));
            }
            cas_index += 1 + ci.chunks.len();
        }

        read_truncated_hashes.sort();
        truncated_hashes.sort();

        assert_eq!(read_truncated_hashes, truncated_hashes);
    }
}
