use std::collections::HashMap;

use bytes::Bytes;
use cas_object::SerializedCasObject;
use deduplication::{Chunk, RawXorbData};
use mdb_shard::file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo};
use mdb_shard::shard_in_memory::MDBInMemoryShard;
use merklehash::{MerkleHash, compute_data_hash, file_hash_with_salt};
use rand::prelude::*;

use crate::error::Result;
use crate::interface::Client;

/// Information about a term (segment) in the file, referencing an XORB and chunk range.
#[derive(Clone, Debug)]
pub struct FileTermReference {
    /// The XORB hash this term references.
    pub xorb_hash: MerkleHash,
    /// Start chunk index (inclusive) within the XORB.
    pub chunk_start: u32,
    /// End chunk index (exclusive) within the XORB.
    pub chunk_end: u32,
    /// The data for this term (concatenated chunk data).
    pub data: Vec<u8>,
    /// The chunk hashes for this term.
    pub chunk_hashes: Vec<MerkleHash>,
}

/// Complete information about a randomly generated file for testing purposes.
///
/// Contains all the metadata needed to verify that reconstruction and fetching
/// operations return correct data.
#[derive(Clone, Debug)]
pub struct RandomFileContents {
    /// The file hash (used for reconstruction queries).
    pub file_hash: MerkleHash,
    /// The complete file data.
    pub data: Vec<u8>,
    /// The RawXorbData for each XORB that was created, keyed by XORB hash.
    pub xorbs: HashMap<MerkleHash, RawXorbData>,
    /// Information about each term in file order.
    pub terms: Vec<FileTermReference>,
}

impl RandomFileContents {
    /// Verifies that the given data matches the expected data for a specific term.
    ///
    /// This checks that the hash of the provided data matches the expected XORB
    /// data for the term at the given index.
    ///
    /// # Arguments
    /// * `term_index` - The index of the term (0-based) in the terms list
    /// * `data` - The data to verify against the expected term data
    ///
    /// # Returns
    /// `true` if the data matches the expected term data, `false` otherwise.
    pub fn term_matches(&self, term_index: usize, data: &[u8]) -> bool {
        if term_index >= self.terms.len() {
            return false;
        }
        let term = &self.terms[term_index];
        term.data == data
    }

    /// Returns the expected data for a specific term.
    pub fn term_data(&self, term_index: usize) -> Option<&[u8]> {
        self.terms.get(term_index).map(|t| t.data.as_slice())
    }

    /// Returns the XORB hash for a specific term.
    pub fn term_xorb_hash(&self, term_index: usize) -> Option<MerkleHash> {
        self.terms.get(term_index).map(|t| t.xorb_hash)
    }

    /// Returns the chunk range for a specific term.
    pub fn term_chunk_range(&self, term_index: usize) -> Option<(u32, u32)> {
        self.terms.get(term_index).map(|t| (t.chunk_start, t.chunk_end))
    }
}

/// A trait that adds testing utility functions to the Client interface.
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[async_trait::async_trait]
pub trait ClientTestingUtils: Client + Send + Sync {
    /// Insert a random file into the local CAS.
    ///
    /// This function generates a random file with the given term specification.
    /// Each term is defined as `(xorb_seed, (chunk_start, chunk_end))` where:
    /// - `xorb_seed` determines the random data for that XORB
    /// - `chunk_start` and `chunk_end` define the range of chunks to include
    ///
    /// Returns a `RandomFileContents` struct containing all the metadata needed
    /// to verify reconstruction and fetching operations.
    async fn upload_random_file(
        &self,
        term_spec: &[(u64, (u64, u64))],
        chunk_size: usize,
    ) -> Result<RandomFileContents> {
        let mut xorb_num_chunks = HashMap::<u64, u64>::new();

        for &(xorb_seed, (_chunk_idx_start, chunk_idx_end)) in term_spec {
            let c: &mut u64 = xorb_num_chunks.entry(xorb_seed).or_default();
            *c = (*c).max(chunk_idx_end);
        }

        let mut shard = MDBInMemoryShard::default();
        let mut xorb_data = HashMap::<u64, RawXorbData>::new();

        for (&xorb_seed, n_chunks) in xorb_num_chunks.iter() {
            let mut rng = SmallRng::seed_from_u64(xorb_seed);
            let n_chunks = *n_chunks as usize;
            let mut chunks = Vec::with_capacity(n_chunks);

            for _idx in 0..n_chunks {
                let n = rng.random_range((chunk_size / 2 + 1)..chunk_size);
                let n_left = chunk_size - n;

                let mut rng_data = vec![0u8; n];
                rng.fill_bytes(&mut rng_data);

                let mut buf = vec![0u8; chunk_size];
                buf[..n].copy_from_slice(&rng_data[..n]);
                buf[n..].copy_from_slice(&rng_data[..n_left]);

                let hash = compute_data_hash(&buf);
                chunks.push(Chunk {
                    hash,
                    data: Bytes::from(buf),
                });
            }

            let raw_xorb = RawXorbData::from_chunks(&chunks, vec![0]);

            shard.add_cas_block(raw_xorb.cas_info.clone())?;

            let serialized_xorb = SerializedCasObject::from_xorb(raw_xorb.clone(), true)?;

            let upload_permit = self.acquire_upload_permit().await?;
            self.upload_xorb("default", serialized_xorb, None, upload_permit).await?;

            xorb_data.insert(xorb_seed, raw_xorb);
        }

        // Build the file info and file data from RawXorbData.
        let mut file_segments = Vec::new();
        let mut file_data = Vec::new();
        let mut chunk_file_hashes = Vec::new();
        let mut term_infos = Vec::new();

        for &(xorb_seed, (chunk_idx_start, chunk_idx_end)) in term_spec {
            let raw_xorb = xorb_data.get(&xorb_seed).unwrap();
            let xorb_h = raw_xorb.hash();

            let (c_lb, c_ub) = (chunk_idx_start as usize, chunk_idx_end as usize);

            let mut n_bytes = 0;
            let mut term_data = Vec::new();
            let mut term_chunk_hashes = Vec::new();

            for i in c_lb..c_ub {
                let chunk_bytes = &raw_xorb.data[i];
                let chunk_hash = raw_xorb.cas_info.chunks[i].chunk_hash;

                file_data.extend_from_slice(chunk_bytes);
                term_data.extend_from_slice(chunk_bytes);
                n_bytes += chunk_bytes.len();
                chunk_file_hashes.push((chunk_hash, chunk_bytes.len() as u64));
                term_chunk_hashes.push(chunk_hash);
            }

            file_segments.push(FileDataSequenceEntry::new(
                xorb_h,
                n_bytes,
                chunk_idx_start as usize,
                chunk_idx_end as usize,
            ));

            term_infos.push(FileTermReference {
                xorb_hash: xorb_h,
                chunk_start: chunk_idx_start as u32,
                chunk_end: chunk_idx_end as u32,
                data: term_data,
                chunk_hashes: term_chunk_hashes,
            });
        }

        let file_hash = file_hash_with_salt(&chunk_file_hashes, &[0; 32]);

        shard.add_file_reconstruction_info(MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash, file_segments.len(), false, false),
            segments: file_segments,
            verification: vec![],
            metadata_ext: None,
        })?;

        let upload_permit = self.acquire_upload_permit().await?;
        self.upload_shard(shard.to_bytes()?.into(), upload_permit).await?;

        // Convert xorb_data from seed-keyed to hash-keyed
        let xorbs = xorb_data.into_values().map(|x| (x.hash(), x)).collect();

        Ok(RandomFileContents {
            file_hash,
            data: file_data,
            xorbs,
            terms: term_infos,
        })
    }
}

impl<T: Client + Send + Sync> ClientTestingUtils for T {}
