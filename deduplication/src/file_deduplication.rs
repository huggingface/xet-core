use std::collections::HashMap;
use std::result::Result;

use mdb_shard::file_structs::{
    FileDataSequenceEntry, FileDataSequenceHeader, FileMetadataExt, FileVerificationEntry, MDBFileInfo,
};
use mdb_shard::hash_is_global_dedup_eligible;
use merkledb::aggregate_hashes::file_node_hash;
use merklehash::{range_hash_from_chunks, MerkleHash};
use more_asserts::*;

use crate::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS};
use crate::data_aggregator::DataAggregator;
use crate::dedup_metrics::DeduplicationMetrics;
use crate::defrag_prevention::DefragPrevention;
use crate::interfaces::ChunkDeduplicator;
use crate::raw_xorb_data::RawXorbData;
use crate::Chunk;

pub struct FileDeduper<ErrorType> {
    dedup_manager: Box<dyn ChunkDeduplicator<ErrorType>>,

    /// The new data here that hasn't yet been deduplicated.
    new_data: Vec<Chunk>,

    /// The amount of new data we have.
    new_data_size: usize,

    /// A hashmap allowing deduplication against the current chunk.
    new_data_hash_lookup: HashMap<MerkleHash, usize>,

    /// The current chunk hashes for this file.
    chunk_hashes: Vec<(MerkleHash, usize)>,

    /// The current file data entries.
    file_info: Vec<FileDataSequenceEntry>,

    /// The list of indices in which the file entry references the current data
    internally_referencing_entries: Vec<usize>,

    /// Tracking the defragmentation of the file specification.
    defrag_tracker: DefragPrevention,

    /// The maximum number of bytes in a xorb. Can be changed by testing code.
    pub target_xorb_max_data_size: usize,

    /// The maximum number of chunks in a xorb. Can be changed by testing code.
    pub target_xorb_max_num_chunks: usize,

    /// The minimum number of chunks to wait for between generating global
    /// dedup queries.  Can be changed by testing code.
    pub min_spacing_between_global_dedup_queries: usize,

    /// The next chunk index that is eligible for global dedup queries
    next_chunk_index_elegible_for_global_dedup_query: usize,

    /// The repo salt; defaults to MerkleHash::default()
    repo_salt: MerkleHash,
}

impl<ErrorType> FileDeduper<ErrorType> {
    pub fn new(dedup_manager: Box<dyn ChunkDeduplicator<ErrorType>>) -> Self {
        Self {
            dedup_manager,
            new_data: Vec::new(),
            new_data_size: 0,
            new_data_hash_lookup: HashMap::new(),
            chunk_hashes: Vec::new(),
            file_info: Vec::new(),
            internally_referencing_entries: Vec::new(),
            defrag_tracker: DefragPrevention::default(),
            target_xorb_max_data_size: MAX_XORB_BYTES,
            target_xorb_max_num_chunks: MAX_XORB_CHUNKS,
            min_spacing_between_global_dedup_queries: 0,
            next_chunk_index_elegible_for_global_dedup_query: 0,
            repo_salt: MerkleHash::default(),
        }
    }

    pub fn set_repo_salt(&mut self, repo_salt: MerkleHash) {
        self.repo_salt = repo_salt
    }

    pub fn process_chunks(&mut self, chunks: &[Chunk]) -> Result<(DeduplicationMetrics, Vec<RawXorbData>), ErrorType> {
        // track the different deduplication statistics.
        let mut dedup_metrics = DeduplicationMetrics::default();

        // In case we need to cut new xorbs
        let mut ret_xorbs = Vec::new();

        // All the previous chunk are stored here, use it as the global chunk index start.
        let global_chunk_index_start = self.chunk_hashes.len();

        let chunk_hashes = Vec::from_iter(chunks.iter().map(|c| c.hash));

        // Now, parallelize the querying of potential new shards on the server end with
        // querying for dedup information of the chunks, which are the two most expensive
        // parts of the process.  Then when we go into the next section, everything is essentially
        // a local lookup table so the remaining work should be quite fast.

        // This holds the results of the dedup queries.
        let mut deduped_blocks = vec![None; chunks.len()];

        // Do at most two passes; 1) with global dedup querying possibly enabled, and 2) possibly rerunning
        // if the global dedup query came back with a new shard.

        for first_pass in [true, false] {
            // Now, go through and test all of these for whether or not they can be deduplicated.
            let mut local_chunk_index = 0;
            while local_chunk_index < chunks.len() {
                let global_chunk_index = global_chunk_index_start + local_chunk_index;

                // First check to see if we don't already know what these blocks are from a previous pass.
                if let Some((n_deduped, _)) = &deduped_blocks[local_chunk_index] {
                    local_chunk_index += n_deduped;
                } else if let Some((n_deduped, fse)) =
                    self.dedup_manager.chunk_hash_dedup_query(&chunk_hashes[local_chunk_index..])?
                {
                    if !first_pass {
                        // This means new shards were discovered; so these are global dedup elegible.  We'll record
                        // the rest later on
                        dedup_metrics.deduped_chunks_by_global_dedup += n_deduped;
                        dedup_metrics.deduped_bytes_by_global_dedup += fse.unpacked_segment_bytes as usize;
                    }

                    deduped_blocks[local_chunk_index] = Some((n_deduped, fse));
                    local_chunk_index += n_deduped;

                    // Now see if we can issue a background query against the global dedup server to see if
                    // any shards are present that give us more dedup ability.
                    //
                    // If we've already queried these against the global dedup, then we can proceed on without
                    // re-querying anything.  Only doing this on the first pass also gaurantees that in the case of
                    // errors on shard retrieval, we don't get stuck in a loop trying to download
                    // and reprocess.
                } else {
                    // Check for global deduplication.
                    if
                    // Only do this query on the first pass.
                    first_pass
                        // The first hash of every file and those maching a pattern are eligible. 
                        && (global_chunk_index == 0
                            || hash_is_global_dedup_eligible(&chunk_hashes[local_chunk_index]))
                        // Limit by enforcing at least 4MB between chunk queries.
                        && global_chunk_index >= self.next_chunk_index_elegible_for_global_dedup_query
                    {
                        self.dedup_manager.register_global_dedup_query(chunk_hashes[local_chunk_index]);
                        self.next_chunk_index_elegible_for_global_dedup_query =
                            global_chunk_index + self.min_spacing_between_global_dedup_queries;
                    }

                    local_chunk_index += 1;
                }
            }

            // Now, see if any of the chunk queries have completed.
            let new_shards_added = self.dedup_manager.complete_global_dedup_queries()?;

            if !new_shards_added {
                break;
            }
        }

        // Now, go through and process the result of the query.
        let mut cur_idx = 0;

        while cur_idx < chunks.len() {
            let mut dedupe_query = deduped_blocks[cur_idx].take();

            if dedupe_query.is_none() {
                // In this case, do a second query against the local xorb to see if we're just repeating previous
                // information in the xorb.
                dedupe_query = self.dedup_query_against_local_data(&chunk_hashes[cur_idx..]);
            }

            if let Some((n_deduped, fse)) = dedupe_query {
                dedup_metrics.deduped_chunks += n_deduped;
                dedup_metrics.deduped_bytes += fse.unpacked_segment_bytes as usize;
                dedup_metrics.total_chunks += n_deduped;
                dedup_metrics.total_bytes += fse.unpacked_segment_bytes as usize;

                // check the fragmentation state and if it is pretty fragmented,
                // we skip dedupe.  However, continuing the previous is always fine.
                if self.file_data_sequence_continues_current(&fse)
                    || self.defrag_tracker.allow_dedup_on_next_range(n_deduped)
                {
                    // We found one or more chunk hashes present
                    self.add_file_data_sequence_entry(fse, n_deduped);

                    cur_idx += n_deduped;
                    continue;
                } else {
                    dedup_metrics.defrag_prevented_dedup_chunks += n_deduped;
                    dedup_metrics.defrag_prevented_dedup_bytes += fse.unpacked_segment_bytes as usize;
                }
            }

            // Okay, now we need to add new data.
            let n_bytes = chunks[cur_idx].data.len();

            // Do we need to cut a new xorb first?
            if self.new_data_size + n_bytes > self.target_xorb_max_data_size
                || self.new_data.len() + 1 > self.target_xorb_max_num_chunks
            {
                ret_xorbs.push(self.cut_new_xorb());
            }

            dedup_metrics.total_chunks += 1;
            dedup_metrics.total_bytes += n_bytes;

            // This is a new data block.
            let add_new_data;

            if !self.file_info.is_empty()
                && self.file_info.last().unwrap().cas_hash == MerkleHash::default()
                && self.file_info.last().unwrap().chunk_index_end as usize == self.new_data.len()
            {
                // This is the next chunk in the CAS block we're building,
                // in which case we can just modify the previous entry.
                let last_entry = self.file_info.last_mut().unwrap();
                last_entry.unpacked_segment_bytes += n_bytes as u32;
                last_entry.chunk_index_end += 1;
                add_new_data = true;
                self.defrag_tracker.increment_last_range_in_fragmentation_estimate(1);
            } else {
                // This block is unrelated to the previous one.
                // This chunk will get the CAS hash updated when the local CAS block
                // is full and registered.
                let file_info_len = self.file_info.len();
                self.internally_referencing_entries.push(file_info_len);
                let chunk_idx = self.new_data.len();

                self.file_info.push(FileDataSequenceEntry::new(
                    MerkleHash::default(),
                    n_bytes,
                    chunk_idx,
                    chunk_idx + 1,
                ));
                self.defrag_tracker.add_range_to_fragmentation_estimate(1);
                add_new_data = true;
            }

            if add_new_data {
                let chunk = chunks[cur_idx].clone();
                self.new_data_hash_lookup.insert(chunk.hash, self.new_data.len());
                self.new_data.push(chunk);
            }

            // Next round.
            cur_idx += 1;
        }

        Ok((dedup_metrics, ret_xorbs))
    }

    fn file_data_sequence_continues_current(&self, fse: &FileDataSequenceEntry) -> bool {
        !self.file_info.is_empty()
            && self.file_info.last().unwrap().cas_hash == fse.cas_hash
            && self.file_info.last().unwrap().chunk_index_end == fse.chunk_index_start
    }

    /// Add a new file data sequence entry to the current process, possibly merging with the
    /// previous entry.
    fn add_file_data_sequence_entry(&mut self, fse: FileDataSequenceEntry, n_deduped: usize) {
        // Do we modify the previous entry as this is the next logical chunk, or do we
        // start a new entry?
        if self.file_data_sequence_continues_current(&fse) {
            // This block is the contiguous continuation of the last entry
            let last_entry = self.file_info.last_mut().unwrap();
            last_entry.unpacked_segment_bytes += fse.unpacked_segment_bytes as u32;
            last_entry.chunk_index_end = fse.chunk_index_end;

            // Update the fragmentation estimation window
            self.defrag_tracker.increment_last_range_in_fragmentation_estimate(n_deduped);
        } else {
            // Make sure we're tracking any that we need to fill in later.
            if fse.cas_hash == MerkleHash::default() {
                self.internally_referencing_entries.push(self.file_info.len());
            }
            // This block is new
            self.file_info.push(fse);
            self.defrag_tracker.add_range_to_fragmentation_estimate(n_deduped);
        }
    }

    /// Cut a new xorb from the existing data.  This may need to be called repeatedly if there are
    fn cut_new_xorb(&mut self) -> RawXorbData {
        // Cut the new xorb.
        let new_xorb = RawXorbData::from_chunks(&self.new_data[..]);

        let xorb_hash = new_xorb.hash();

        // Go through and replace all the indices in the file sequence entries with
        // the new xorb if referenced.
        for &idx in self.internally_referencing_entries.iter() {
            let fse = &mut self.file_info[idx];
            debug_assert_eq!(fse.cas_hash, MerkleHash::default());
            debug_assert_lt!(fse.chunk_index_start as usize, self.new_data.len());
            debug_assert_le!(fse.chunk_index_end as usize, self.new_data.len());

            fse.cas_hash = xorb_hash;
        }

        #[cfg(debug_assertions)]
        {
            // For bookkeeping checks, make sure we have everything.
            for fse in self.file_info.iter() {
                debug_assert_ne!(fse.cas_hash, MerkleHash::default());
            }
        }

        new_xorb
    }

    /// Do a query against the local data; this would return an entry with MerkleHash::default(), which
    /// would need to get filled in.
    fn dedup_query_against_local_data(&mut self, chunks: &[MerkleHash]) -> Option<(usize, FileDataSequenceEntry)> {
        // It's important for the defrag prevention to have a good estimate of the number of chunks in
        // a row that can be deduplicated, so this pulls through the
        if let Some(&base_idx) = self.new_data_hash_lookup.get(&chunks[0]) {
            let mut n_bytes = self.new_data[base_idx].data.len();

            let mut end_idx = base_idx + 1;
            for i in 1..chunks.len() {
                if let Some(&idx) = self.new_data_hash_lookup.get(&chunks[i]) {
                    if idx == base_idx + i {
                        end_idx = idx + 1;
                        n_bytes += self.new_data[idx].data.len();
                        continue;
                    }
                }
                break;
            }

            Some((end_idx - base_idx, FileDataSequenceEntry::new(MerkleHash::default(), n_bytes, base_idx, end_idx)))
        } else {
            None
        }
    }

    /// Convert the internal state to a DataAggregator object that can be  
    pub fn finalize(self, file_hash_salt: Option<MerkleHash>, metadata_ext: Option<FileMetadataExt>) -> DataAggregator {
        let file_hash = file_node_hash(&self.chunk_hashes, &file_hash_salt.unwrap_or_default().into()).unwrap();

        let metadata = FileDataSequenceHeader::new(file_hash, self.file_info.len(), true, metadata_ext.is_some());

        let mut chunk_idx = 0;

        // Create the file verification stamp.
        let verification = self
            .file_info
            .iter()
            .map(|entry| {
                let n_chunks = (entry.chunk_index_end - entry.chunk_index_start) as usize;
                let chunk_hashes: Vec<_> = self.chunk_hashes[chunk_idx..chunk_idx + n_chunks]
                    .iter()
                    .map(|(hash, _)| *hash)
                    .collect();
                let range_hash = range_hash_from_chunks(&chunk_hashes);
                chunk_idx += n_chunks;

                FileVerificationEntry::new(range_hash)
            })
            .collect();

        let fi = MDBFileInfo {
            metadata,
            segments: self.file_info,
            verification,
            metadata_ext,
        };

        DataAggregator::new(self.new_data, fi, self.internally_referencing_entries)
    }
}
