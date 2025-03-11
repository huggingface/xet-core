use mdb_shard::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfo};
use mdb_shard::file_structs::MDBFileInfo;
use merkledb::aggregate_hashes::cas_node_hash;
use merklehash::MerkleHash;

#[derive(Default, Debug)]
pub(crate) struct CASDataAggregator {
    /// Bytes of all chunks accumulated in one CAS block concatenated together.
    pub data: Vec<u8>,
    /// Metadata of all chunks accumulated in one CAS block. Each entry is
    /// (chunk hash, chunk size).
    pub chunks: Vec<(MerkleHash, usize)>,
    // The file info of files that are still being processed.
    // As we're building this up, we assume that all files that do not have a size in the header are
    // not finished yet and thus cannot be uploaded.
    //
    // All the cases the default hash for a cas info entry will be filled in with the cas hash for
    // an entry once the cas block is finalized and uploaded.  These correspond to the indices given
    // alongwith the file info.
    // This tuple contains the file info (which may be modified) and the divisions in the chunks corresponding
    // to this file.
    pub pending_file_info: Vec<(MDBFileInfo, Vec<usize>)>,
}

impl CASDataAggregator {
    pub fn is_empty(&self) -> bool {
        self.data.is_empty() && self.chunks.is_empty() && self.pending_file_info.is_empty()
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Finalize the result, returning the CAS info, xorb data, and the file info that's included in this.
    pub fn finalize(mut self) -> (MDBCASInfo, Vec<u8>, Vec<MDBFileInfo>) {
        // First, cut the xorb for this one.
        let raw_bytes_len = self.data.len();
        let cas_hash = cas_node_hash(&self.chunks[..]);

        // Now that we have the CAS hash, fill in any blocks with the referencing xorb
        // hash as needed.
        for (fi, chunk_hash_indices) in self.pending_file_info.iter_mut() {
            for &i in chunk_hash_indices.iter() {
                debug_assert_eq!(fi.segments[i].cas_hash, MerkleHash::default());
                fi.segments[i].cas_hash = cas_hash;
            }
        }

        // Build the MDBCASInfo struct.
        let metadata = CASChunkSequenceHeader::new(cas_hash, self.chunks.len(), raw_bytes_len);

        let mut pos = 0;
        let chunks: Vec<_> = self
            .chunks
            .iter()
            .map(|(h, len)| {
                let ret = CASChunkSequenceEntry::new(*h, *len, pos);
                pos += *len;
                ret
            })
            .collect();
        let cas_info = MDBCASInfo { metadata, chunks };

        (cas_info, self.data, self.pending_file_info.into_iter().map(|(fi, _)| fi).collect())
    }

    pub fn merge_in(&mut self, mut other: CASDataAggregator) {
        let shift = self.chunks.len() as u32;
        self.data.append(&mut other.data);
        self.chunks.append(&mut other.chunks);

        // Adjust the chunk indices and shifts for
        for file_info in other.pending_file_info.iter_mut() {
            for fi in file_info.0.segments.iter_mut() {
                // To transfer the cas chunks from the other data aggregator to this one,
                // shift chunk indices so the new index start and end values reflect the
                // append opperation above.
                if fi.cas_hash == MerkleHash::default() {
                    fi.chunk_index_start += shift;
                    fi.chunk_index_end += shift;
                }
            }
        }

        self.pending_file_info.append(&mut other.pending_file_info);
    }
}
