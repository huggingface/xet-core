// The shard structure for the in memory querying

use std::collections::BTreeMap;
use std::io::{BufWriter, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tracing::debug;

use super::file_structs::*;
#[cfg(debug_assertions)]
use super::shard_file_handle::{MDBShardFile, new_shard_file_cache};
use super::shard_format::MDBShardInfo;
use super::utils::{shard_file_name, temp_shard_file_name};
use super::xorb_structs::*;
use crate::MerkleHashMap;
use crate::error::Result;
use crate::merklehash::{HashedWrite, MerkleHash};

#[allow(clippy::type_complexity)]
#[derive(Clone, Default, Debug)]
pub struct MDBInMemoryShard {
    pub xorb_content: BTreeMap<MerkleHash, Arc<MDBXorbInfo>>,
    pub file_content: BTreeMap<MerkleHash, MDBFileInfo>,
    pub chunk_hash_lookup: MerkleHashMap<(Arc<MDBXorbInfo>, u64)>,
    current_shard_file_size: u64,
}

impl MDBInMemoryShard {
    pub fn add_xorb_block(&mut self, xorb_block_contents: impl Into<Arc<MDBXorbInfo>>) -> Result<()> {
        let dest_content_v: Arc<MDBXorbInfo> = xorb_block_contents.into();

        self.xorb_content
            .insert(dest_content_v.metadata.xorb_hash, dest_content_v.clone());

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
        if self.file_content.contains_key(&file_info.metadata.file_hash) {
            return Ok(());
        }

        self.current_shard_file_size += file_info.num_bytes();
        self.current_shard_file_size += (size_of::<u64>() + size_of::<u32>()) as u64;

        self.file_content.insert(file_info.metadata.file_hash, file_info);

        Ok(())
    }

    pub fn union(&self, other: &Self) -> Result<Self> {
        let mut xorb_content = self.xorb_content.clone();
        other.xorb_content.iter().for_each(|(k, v)| {
            xorb_content.insert(*k, v.clone());
        });

        let mut file_content = self.file_content.clone();
        for (k, v) in &other.file_content {
            file_content.entry(*k).or_insert_with(|| v.clone());
        }

        let mut chunk_hash_lookup = self.chunk_hash_lookup.clone();
        other.chunk_hash_lookup.iter().for_each(|(k, v)| {
            chunk_hash_lookup.insert(*k, v.clone());
        });

        let mut s = Self {
            xorb_content,
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
        for (_, xorb_block_contents) in self.xorb_content.iter() {
            num_bytes += xorb_block_contents.num_bytes();

            // The xorb lookup table
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
            xorb_content: other
                .xorb_content
                .iter()
                .filter(|(k, _)| !self.xorb_content.contains_key(k))
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
            FileDataSequenceEntry::from_xorb_entries(
                &chunk_ref.metadata,
                &chunk_ref.chunks[chunk_index_start..(chunk_index_start + query_idx)],
                chunk_index_start,
                chunk_index_start + query_idx,
            ),
        ))
    }

    pub fn num_xorb_entries(&self) -> usize {
        self.xorb_content.len()
    }

    pub fn num_file_entries(&self) -> usize {
        self.file_content.len()
    }

    pub fn stored_bytes_on_disk(&self) -> u64 {
        self.xorb_content
            .iter()
            .fold(0u64, |acc, (_, xorb)| acc + xorb.metadata.num_bytes_on_disk as u64)
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
        self.xorb_content
            .iter()
            .fold(0u64, |acc, (_, xorb)| acc + xorb.metadata.num_bytes_in_xorb as u64)
    }

    pub fn is_empty(&self) -> bool {
        self.xorb_content.is_empty() && self.file_content.is_empty()
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
            let cache = new_shard_file_cache();
            let shard_file = MDBShardFile::load_from_file(&full_file_name, &cache)?;
            shard_file.verify_shard_integrity();
        }

        Ok(full_file_name)
    }

    /// Serializes the shard to a vector of bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        MDBShardInfo::serialize_from(&mut buf, self, None)?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::super::shard_format::test_routines::gen_random_file_info;
    use super::*;

    #[test]
    fn add_file_reconstruction_info_keeps_first_duplicate_and_size() -> Result<()> {
        let mut rng = StdRng::seed_from_u64(11);
        let first = gen_random_file_info(&mut rng, &2, false, false);
        let mut duplicate = gen_random_file_info(&mut rng, &3, true, true);
        duplicate.metadata.file_hash = first.metadata.file_hash;

        let mut shard = MDBInMemoryShard::default();
        shard.add_file_reconstruction_info(first.clone())?;
        let size_after_first = shard.shard_file_size();

        shard.add_file_reconstruction_info(duplicate)?;

        assert_eq!(shard.num_file_entries(), 1);
        assert_eq!(shard.get_file_reconstruction_info(&first.metadata.file_hash), Some(first));
        assert_eq!(shard.shard_file_size(), size_after_first);

        Ok(())
    }

    #[test]
    fn union_keeps_left_file_info_for_duplicate_hash() -> Result<()> {
        let mut rng = StdRng::seed_from_u64(12);
        let first = gen_random_file_info(&mut rng, &2, false, false);
        let mut duplicate = gen_random_file_info(&mut rng, &3, true, true);
        duplicate.metadata.file_hash = first.metadata.file_hash;

        let mut left = MDBInMemoryShard::default();
        left.add_file_reconstruction_info(first.clone())?;

        let mut right = MDBInMemoryShard::default();
        right.add_file_reconstruction_info(duplicate)?;

        let union = left.union(&right)?;

        assert_eq!(union.num_file_entries(), 1);
        assert_eq!(union.get_file_reconstruction_info(&first.metadata.file_hash), Some(first));

        Ok(())
    }
}
