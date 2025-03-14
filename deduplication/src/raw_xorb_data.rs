use std::sync::Arc;

use mdb_shard::cas_structs::{CASChunkSequenceEntry, CASChunkSequenceHeader, MDBCASInfo};
use merkledb::aggregate_hashes::cas_node_hash;
use merklehash::MerkleHash;
use more_asserts::*;

use crate::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS};
use crate::Chunk;

/// This struct is the data needed to cut a
#[derive(Default, Debug)]
pub struct RawXorbData {
    /// The data for the xorb info.
    pub data: Vec<Arc<[u8]>>,

    /// The cas info associated with the current xorb.
    pub cas_info: MDBCASInfo,
}

impl RawXorbData {
    // Construct from raw chunks.  chunk data from raw chunks.
    pub fn from_chunks(chunks: &[Chunk]) -> Self {
        debug_assert_le!(chunks.len(), MAX_XORB_CHUNKS);

        let mut data = Vec::with_capacity(chunks.len());
        let mut chunk_seq_entries = Vec::with_capacity(chunks.len());

        // Build the sequences.
        let mut pos = 0;
        for c in chunks {
            chunk_seq_entries.push(CASChunkSequenceEntry::new(c.hash, c.data.len(), pos));
            data.push(c.data.clone());
            pos += c.data.len();
        }
        let num_bytes = pos;

        debug_assert_le!(num_bytes, MAX_XORB_BYTES);

        let cas_hash = cas_node_hash(chunks);

        // Build the MDBCASInfo struct.
        let metadata = CASChunkSequenceHeader::new(cas_hash, chunks.len(), num_bytes);

        let cas_info = MDBCASInfo {
            metadata,
            chunks: chunk_seq_entries,
        };

        RawXorbData { data, cas_info }
    }

    pub fn hash(&self) -> MerkleHash {
        self.cas_info.metadata.cas_hash
    }

    pub fn num_bytes(&self) -> usize {
        self.cas_info.metadata.num_bytes_in_cas as usize
    }
}
