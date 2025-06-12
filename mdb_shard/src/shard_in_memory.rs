// The shard structure for the in memory querying

use std::collections::{BTreeMap, HashMap};
use std::io::{BufWriter, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use merklehash::{HashedWrite, MerkleHash};
use tracing::debug;

use crate::cas_structs::*;
use crate::error::Result;
use crate::file_structs::*;
use crate::shard_format::MDBShardInfo;
use crate::utils::{shard_file_name, temp_shard_file_name};

#[allow(clippy::type_complexity)]
#[derive(Clone, Default, Debug)]
pub struct MDBInMemoryShard {
    pub cas_content: BTreeMap<MerkleHash, Arc<MDBCASInfo>>,
    pub file_content: BTreeMap<MerkleHash, MDBFileInfo>,
    pub chunk_hash_lookup: HashMap<MerkleHash, (Arc<MDBCASInfo>, u64)>,
    current_shard_file_size: u64,
}

impl MDBInMemoryShard {
    pub fn add_cas_block(&mut self, cas_block_contents: impl Into<Arc<MDBCASInfo>>) -> Result<()> {
        let dest_content_v: Arc<MDBCASInfo> = cas_block_contents.into();

        self.cas_content
            .insert(dest_content_v.metadata.cas_hash, dest_content_v.clone());

        for (i, chunk) in dest_content_v.chunks.iter().enumerate() {
            self.chunk_hash_lookup
                .insert(chunk.chunk_hash, (dest_content_v.clone(), i as u64));
            self.current_shard_file_size += (size_of::<u64>() + 2 * size_of::<u32>()) as u64;
        }
        self.current_shard_file_size += dest_content_v.num_bytes();
        self.current_shard_file_size += (size_of::<u64>() + size_of::<u32>()) as u64;

        Ok(())
    }

    pub fn add_file_reconstruction_info(&mut self, file_info: MDBFileInfo) -> Result<()> {
        self.current_shard_file_size += file_info.num_bytes();
        self.current_shard_file_size += (size_of::<u64>() + size_of::<u32>()) as u64;

        self.file_content.insert(file_info.metadata.file_hash, file_info);

        Ok(())
    }

    pub fn union(&self, other: &Self) -> Result<Self> {
        let mut cas_content = self.cas_content.clone();
        other.cas_content.iter().for_each(|(k, v)| {
            cas_content.insert(*k, v.clone());
        });

        let mut file_content = self.file_content.clone();
        for (k, v) in &other.file_content {
            if let Some(mut old_v) = file_content.insert(*k, v.clone()) {
                // merge the contents to ensure we have all the information between
                // the two file contents (e.g. verification and metadata_ext)
                old_v.merge_from(v)?;
                file_content.insert(*k, old_v);
            };
        }

        let mut chunk_hash_lookup = self.chunk_hash_lookup.clone();
        other.chunk_hash_lookup.iter().for_each(|(k, v)| {
            chunk_hash_lookup.insert(*k, v.clone());
        });

        let mut s = Self {
            cas_content,
            file_content,
            current_shard_file_size: 0,
            chunk_hash_lookup,
        };

        s.recalculate_shard_size();
        Ok(s)
    }

    pub fn recalculate_shard_size(&mut self) {
        // Calculate the size
        let mut num_bytes = 0u64;
        for (_, cas_block_contents) in self.cas_content.iter() {
            num_bytes += cas_block_contents.num_bytes();

            // The cas lookup table
            num_bytes += (size_of::<u64>() + size_of::<u32>()) as u64;
        }

        for (_, file_info) in self.file_content.iter() {
            num_bytes += file_info.num_bytes();
            num_bytes += (size_of::<u64>() + size_of::<u32>()) as u64;
        }

        num_bytes += ((size_of::<u64>() + 2 * size_of::<u32>()) * self.chunk_hash_lookup.len()) as u64;

        self.current_shard_file_size = num_bytes;
    }

    pub fn difference(&self, other: &Self) -> Result<Self> {
        let mut s = Self {
            cas_content: other
                .cas_content
                .iter()
                .filter(|(k, _)| !self.cas_content.contains_key(k))
                .map(|(k, v)| (*k, v.clone()))
                .collect(),
            file_content: other
                .file_content
                .iter()
                .filter(|(k, _)| !self.file_content.contains_key(k))
                .map(|(k, v)| (*k, v.clone()))
                .collect(),
            chunk_hash_lookup: other
                .chunk_hash_lookup
                .iter()
                .filter(|(k, _)| !self.chunk_hash_lookup.contains_key(k))
                .map(|(k, v)| (*k, v.clone()))
                .collect(),
            current_shard_file_size: 0,
        };
        s.recalculate_shard_size();
        Ok(s)
    }

    /// Given a file pointer, returns the information needed to reconstruct the file.
    /// Returns the file info if the file hash was found, and None otherwise.
    pub fn get_file_reconstruction_info(&self, file_hash: &MerkleHash) -> Option<MDBFileInfo> {
        if let Some(mdb_file) = self.file_content.get(file_hash) {
            return Some(mdb_file.clone());
        }

        None
    }

    pub fn chunk_hash_dedup_query(&self, query_hashes: &[MerkleHash]) -> Option<(usize, FileDataSequenceEntry)> {
        if query_hashes.is_empty() {
            return None;
        }

        let (chunk_ref, chunk_index_start) = self.chunk_hash_lookup.get(&query_hashes[0])?;

        let chunk_index_start = *chunk_index_start as usize;

        let mut query_idx = 0;

        loop {
            if chunk_index_start + query_idx >= chunk_ref.chunks.len() {
                break;
            }
            if query_idx >= query_hashes.len()
                || chunk_ref.chunks[chunk_index_start + query_idx].chunk_hash != query_hashes[query_idx]
            {
                break;
            }
            query_idx += 1;
        }

        Some((
            query_idx,
            FileDataSequenceEntry::from_cas_entries(
                &chunk_ref.metadata,
                &chunk_ref.chunks[chunk_index_start..(chunk_index_start + query_idx)],
                chunk_index_start,
                chunk_index_start + query_idx,
            ),
        ))
    }

    pub fn num_cas_entries(&self) -> usize {
        self.cas_content.len()
    }

    pub fn num_file_entries(&self) -> usize {
        self.file_content.len()
    }

    pub fn stored_bytes_on_disk(&self) -> u64 {
        self.cas_content
            .iter()
            .fold(0u64, |acc, (_, cas)| acc + cas.metadata.num_bytes_on_disk as u64)
    }

    pub fn materialized_bytes(&self) -> u64 {
        self.file_content.iter().fold(0u64, |acc, (_, file)| {
            acc + file
                .segments
                .iter()
                .fold(0u64, |acc, entry| acc + entry.unpacked_segment_bytes as u64)
        })
    }

    pub fn stored_bytes(&self) -> u64 {
        self.cas_content
            .iter()
            .fold(0u64, |acc, (_, cas)| acc + cas.metadata.num_bytes_in_cas as u64)
    }

    pub fn is_empty(&self) -> bool {
        self.cas_content.is_empty() && self.file_content.is_empty()
    }

    /// Returns the number of bytes required
    pub fn shard_file_size(&self) -> u64 {
        self.current_shard_file_size + MDBShardInfo::non_content_byte_size()
    }

    /// Writes the shard out to a file.
    pub fn write_to_temp_shard_file(&self, temp_file_name: &Path, expiration: Option<Duration>) -> Result<MerkleHash> {
        let mut hashed_write; // Need to access after file is closed.

        {
            // Scoped so that file is closed and flushed before name is changed.

            let out_file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(temp_file_name)?;

            hashed_write = HashedWrite::new(out_file);

            let mut buf_write = BufWriter::new(&mut hashed_write);

            // Ask for write access, as we'll flush this at the end
            MDBShardInfo::serialize_from(&mut buf_write, self, expiration)?;

            debug!("Writing out in-memory shard to {temp_file_name:?}.");

            buf_write.flush()?;
        }

        // Get the hash
        hashed_write.flush()?;
        let shard_hash = hashed_write.hash();

        Ok(shard_hash)
    }
    pub fn write_to_directory(&self, directory: &Path, expiration: Option<Duration>) -> Result<PathBuf> {
        // First, create a temporary shard structure in that directory.
        let temp_file_name = directory.join(temp_shard_file_name());

        let shard_hash = self.write_to_temp_shard_file(&temp_file_name, expiration)?;

        let full_file_name = directory.join(shard_file_name(&shard_hash));

        std::fs::rename(&temp_file_name, &full_file_name)?;

        debug!("Wrote out in-memory shard to {full_file_name:?}.");

        #[cfg(debug_assertions)]
        {
            use crate::MDBShardFile;
            let shard_file = MDBShardFile::load_from_file(&full_file_name)?;
            shard_file.verify_shard_integrity();
        }

        Ok(full_file_name)
    }
}
