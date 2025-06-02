use std::mem::take;

use mdb_shard::file_structs::MDBFileInfo;
use merklehash::MerkleHash;
use more_asserts::*;

use crate::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS};
use crate::raw_xorb_data::RawXorbData;
use crate::Chunk;

#[derive(Default, Debug)]
pub struct DataAggregator {
    // Bytes of all chunks accumulated in one CAS block concatenated together.
    pub chunks: Vec<Chunk>,

    // Number of bytes
    num_bytes: usize,

    // The file info of files that are still being processed.
    // As we're building this up, we assume that all files that do not have a size in the header are
    // not finished yet and thus cannot be uploaded.
    //
    // All the cases the marker hash for a cas info entry will be filled in with the cas hash for
    // an entry once the cas block is finalized and uploaded.  These correspond to the indices given
    // alongwith the file info.
    // This tuple contains the file info (which may be modified) and the divisions in the chunks corresponding
    // to this file.  It also includes an optional file ID
    pub pending_file_info: Vec<(MDBFileInfo, Vec<usize>, u64)>,

    // The specific chunk indices at which a new file starts.  This is used for the compression
    // heuristic; which compression method to use is calculated once per file section for each xorb.
    pub file_boundaries: Vec<usize>,
}

impl DataAggregator {
    pub(crate) fn new(
        chunks: Vec<Chunk>,
        pending_file_info: MDBFileInfo,
        internally_referencing_entries: Vec<usize>,
        file_id: u64,
    ) -> Self {
        let num_bytes = chunks.iter().map(|c| c.data.len()).sum();

        // This is just one file here, so start it off like this.
        let file_boundaries = if chunks.is_empty() { vec![] } else { vec![0] };

        Self {
            chunks,
            num_bytes,
            pending_file_info: vec![(pending_file_info, internally_referencing_entries, file_id)],
            file_boundaries,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty() && self.pending_file_info.is_empty()
    }

    pub fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    pub fn num_bytes(&self) -> usize {
        debug_assert_eq!(self.chunks.iter().map(|c| c.data.len()).sum::<usize>(), self.num_bytes);
        self.num_bytes
    }

    /// Finalize the result, returning the xorb data, and a Vec of (file_id, file_info, n_bytes_in_xorb);
    /// i.e. the file info that's included in this along
    /// with the number of bytes in each file that is part of this xorb.
    pub fn finalize(mut self) -> (RawXorbData, Vec<(u64, MDBFileInfo, u64)>) {
        // First, cut the xorb for this one.
        let xorb_data = RawXorbData::from_chunks(&self.chunks, take(&mut self.file_boundaries));
        let xorb_hash = xorb_data.hash();

        debug_assert_le!(self.num_bytes(), *MAX_XORB_BYTES);
        debug_assert_le!(self.num_chunks(), *MAX_XORB_CHUNKS);

        let mut ret = vec![0u64; self.pending_file_info.len()];

        // Now that we have the CAS hash, fill in any blocks with the referencing xorb
        // hash as needed.
        for (f_idx, (fi, chunk_hash_indices_ref, _file_id)) in self.pending_file_info.iter_mut().enumerate() {
            for &i in chunk_hash_indices_ref.iter() {
                debug_assert_eq!(fi.segments[i].cas_hash, MerkleHash::marker());
                fi.segments[i].cas_hash = xorb_hash;
                ret[f_idx] += fi.segments[i].unpacked_segment_bytes as u64;
            }

            // Incorporated this info, so clear this.
            chunk_hash_indices_ref.clear();

            #[cfg(debug_assertions)]
            {
                // Make sure our bookkeeping along the way was good.
                for fse in fi.segments.iter() {
                    debug_assert_ne!(fse.cas_hash, MerkleHash::marker());
                }
            }
        }

        (
            xorb_data,
            self.pending_file_info
                .into_iter()
                .zip(ret)
                .map(|((fi, _, file_id), byte_count)| (file_id, fi, byte_count))
                .collect(),
        )
    }

    pub fn merge_in(&mut self, mut other: DataAggregator) {
        debug_assert_le!(self.num_bytes() + other.num_bytes(), *MAX_XORB_BYTES);
        debug_assert_le!(self.num_chunks() + other.num_chunks(), *MAX_XORB_BYTES);

        let shift = self.chunks.len() as u32;
        self.chunks.append(&mut other.chunks);
        self.num_bytes += other.num_bytes;

        // Adjust the chunk indices and shifts for
        for file_info in other.pending_file_info.iter_mut() {
            for fi in file_info.0.segments.iter_mut() {
                // To transfer the cas chunks from the other data aggregator to this one,
                // shift chunk indices so the new index start and end values reflect the
                // append opperation above.
                if fi.cas_hash == MerkleHash::marker() {
                    fi.chunk_index_start += shift;
                    fi.chunk_index_end += shift;
                }
            }
        }

        self.pending_file_info.append(&mut other.pending_file_info);

        // Append the file boundaries from the other aggregator, tracking the shifts.
        self.file_boundaries
            .extend(other.file_boundaries.into_iter().map(|idx| idx + (shift as usize)));
    }
}
