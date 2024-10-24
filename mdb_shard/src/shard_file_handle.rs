use std::io::{BufReader, Cursor, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use merklehash::{compute_data_hash, HMACKey, HashedWrite, MerkleHash};
use tracing::{debug, error, warn};

use crate::cas_structs::CASChunkSequenceHeader;
use crate::error::{MDBShardError, Result};
use crate::file_structs::{FileDataSequenceEntry, MDBFileInfo};
use crate::shard_format::MDBShardInfo;
use crate::utils::{parse_shard_filename, shard_file_name, temp_shard_file_name};

/// When a specific implementation of the  
#[derive(Debug, Clone, Default)]
pub struct MDBShardFile {
    pub shard_hash: MerkleHash,
    pub path: PathBuf,
    pub shard: MDBShardInfo,
}

impl MDBShardFile {
    pub fn new(shard_hash: MerkleHash, path: PathBuf, shard: MDBShardInfo) -> Result<Self> {
        let s = Self {
            shard_hash,
            path,
            shard,
        };

        s.verify_shard_integrity_debug_only();
        Ok(s)
    }

    pub fn write_out_from_reader<R: Read + Seek>(target_directory: impl AsRef<Path>, reader: &mut R) -> Result<Self> {
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
        }

        // Get the hash
        hashed_write.flush()?;
        let shard_hash = hashed_write.hash();

        let full_file_name = target_directory.join(shard_file_name(&shard_hash));

        std::fs::rename(&temp_file_name, &full_file_name)?;

        Self::new(shard_hash, full_file_name, MDBShardInfo::load_from_file(reader)?)
    }

    /// Loads the MDBShardFile struct from
    pub fn load_from_file(path: &Path) -> Result<Self> {
        if let Some(shard_hash) = parse_shard_filename(path.to_str().unwrap()) {
            let mut f = std::fs::File::open(path)?;
            Ok(Self::new(shard_hash, std::fs::canonicalize(path)?, MDBShardInfo::load_from_file(&mut f)?)?)
        } else {
            Err(MDBShardError::BadFilename(format!("{path:?} not a valid MerkleDB filename.")))
        }
    }

    pub fn load_all(path: &Path) -> Result<Vec<Self>> {
        let mut shards = Vec::new();

        if path.is_dir() {
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                if let Some(file_name) = entry.file_name().to_str() {
                    if let Some(h) = parse_shard_filename(file_name) {
                        shards.push((h, std::fs::canonicalize(entry.path())?));
                    }
                    debug!("Found shard file '{file_name:?}'.");
                }
            }
        } else if let Some(file_name) = path.to_str() {
            if let Some(h) = parse_shard_filename(file_name) {
                shards.push((h, std::fs::canonicalize(path)?));
                debug!("Registerd shard file '{file_name:?}'.");
            } else {
                return Err(MDBShardError::BadFilename(format!("Filename {file_name} not valid shard file name.")));
            }
        }

        let mut ret = Vec::with_capacity(shards.len());

        for (shard_hash, path) in shards {
            let mut f = std::fs::File::open(&path)?;

            ret.push(MDBShardFile {
                shard_hash,
                path,
                shard: MDBShardInfo::load_from_file(&mut f)?,
            });
        }

        #[cfg(debug_assertions)]
        {
            // In debug mode, verify all shards on loading to catch errors earlier.
            for s in ret.iter() {
                s.verify_shard_integrity_debug_only();
            }
        }

        Ok(ret)
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
    ) -> Result<Self> {
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

        Self::write_out_from_reader(target_directory, &mut Cursor::new(output_bytes))
    }

    #[inline]
    pub fn read_all_cas_blocks(&self) -> Result<Vec<(CASChunkSequenceHeader, u64)>> {
        self.shard.read_all_cas_blocks(&mut self.get_reader()?)
    }

    pub fn get_reader(&self) -> Result<BufReader<std::fs::File>> {
        Ok(BufReader::with_capacity(2048, std::fs::File::open(&self.path)?))
    }

    #[inline]
    pub fn get_file_reconstruction_info(&self, file_hash: &MerkleHash) -> Result<Option<MDBFileInfo>> {
        self.shard.get_file_reconstruction_info(&mut self.get_reader()?, file_hash)
    }

    #[inline]
    pub fn chunk_hash_dedup_query(
        &self,
        query_hashes: &[MerkleHash],
    ) -> Result<Option<(usize, FileDataSequenceEntry)>> {
        self.shard.chunk_hash_dedup_query(&mut self.get_reader()?, query_hashes)
    }

    #[inline]
    pub fn chunk_hash_dedup_query_direct(
        &self,
        query_hashes: &[MerkleHash],
        cas_block_index: u32,
        cas_chunk_offset: u32,
    ) -> Result<Option<(usize, FileDataSequenceEntry)>> {
        self.shard.chunk_hash_dedup_query_direct(
            &mut self.get_reader()?,
            query_hashes,
            cas_block_index,
            cas_chunk_offset,
        )
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
    pub fn verify_shard_integrity_debug_only(&self) {
        #[cfg(debug_assertions)]
        {
            self.verify_shard_integrity();
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

        // TODO: More parts; but this will at least succeed on the server end.
    }
}
