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

/// A trait that adds testing utility functions to the Client interface.
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[async_trait::async_trait]
pub trait ClientTestingUtils: Client + Send + Sync {
    /// Insert a random file into the local CAS.
    ///
    /// This function is used to test the local CAS client.
    ///
    /// It generates a random file with a given number of chunks and chunk size.
    /// It then creates all the xorbs and shard definitions needed, returning
    /// the file data and the file hash.
    async fn upload_random_file(
        &self,
        term_spec: &[(u64, (u64, u64))],
        chunk_size: usize,
    ) -> Result<(Vec<u8>, MerkleHash)> {
        let mut xorb_num_chunks = HashMap::<u64, u64>::new();

        for &(xorb_seed, (_chunk_idx_start, chunk_idx_end)) in term_spec {
            let c: &mut u64 = xorb_num_chunks.entry(xorb_seed).or_default();
            *c = (*c).max(chunk_idx_end);
        }

        // Track the data so that we can reconstruct the whole file later.
        let mut xorb_data = HashMap::<u64, Vec<Chunk>>::new();
        let mut shard = MDBInMemoryShard::default();

        let mut xorb_hash = HashMap::<u64, MerkleHash>::new();

        for (&xorb_seed, n_chunks) in xorb_num_chunks.iter() {
            let mut rng = SmallRng::seed_from_u64(xorb_seed);

            let n_chunks = *n_chunks as usize;
            let mut chunks = Vec::with_capacity(n_chunks);

            for _idx in 0..n_chunks {
                // duplicate the range so that compression kicks in;
                // copy the second part of the chunk from the first part.

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

            // Create RawXorbData from the generated chunks.
            // file_boundaries indicates where new files start; use [0] for single file.
            let raw_xorb = RawXorbData::from_chunks(&chunks, vec![0]);

            // Record the xorb data.
            xorb_data.insert(xorb_seed, chunks);

            // Add it to the shard.
            shard.add_cas_block(raw_xorb.cas_info.clone())?;

            // Record the hash.
            xorb_hash.insert(xorb_seed, raw_xorb.hash());

            // Build SerializedCasObject
            let serialized_xorb = SerializedCasObject::from_xorb(raw_xorb.clone(), None, true)?;

            // upload the xorb
            let upload_permit = self.acquire_upload_permit().await?;
            self.upload_xorb("default", serialized_xorb, None, upload_permit).await?;
        }

        // Now, build the file info and file data.
        let mut file_segments = Vec::new();
        let mut file_data = Vec::new();
        let mut chunk_file_hashes = Vec::new();

        for &(xorb_seed, (chunk_idx_start, chunk_idx_end)) in term_spec {
            let xorb_hash = xorb_hash.get(&xorb_seed).unwrap();

            let (c_lb, c_ub) = (chunk_idx_start as usize, chunk_idx_end as usize);
            let chunks = &xorb_data.get(&xorb_seed).unwrap()[c_lb..c_ub];

            let mut n_bytes = 0;

            for chunk in chunks {
                file_data.extend_from_slice(&chunk.data);
                n_bytes += chunk.data.len();
                chunk_file_hashes.push((chunk.hash, chunk.data.len() as u64));
            }

            file_segments.push(FileDataSequenceEntry::new(
                *xorb_hash,
                n_bytes,
                chunk_idx_start as usize,
                chunk_idx_end as usize,
            ));
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

        Ok((file_data, file_hash))
    }
}

impl<T: Client + Send + Sync> ClientTestingUtils for T {}
