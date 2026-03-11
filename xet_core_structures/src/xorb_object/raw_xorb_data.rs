use more_asserts::*;

use super::Chunk;
use super::constants::{MAX_XORB_BYTES, MAX_XORB_CHUNKS};
use crate::merklehash::{MerkleHash, xorb_hash};
use crate::metadata_shard::xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader};

/// This struct is the data needed to cut a
#[derive(Default, Debug, Clone)]
pub struct RawXorbData {
    /// The data for the xorb info.
    pub data: Vec<bytes::Bytes>,

    /// The cas info associated with the current xorb.
    pub xorb_info: MDBXorbInfo,

    /// The indices where a new file starts, to be used for the compression heuristic.
    pub file_boundaries: Vec<usize>,
}

impl RawXorbData {
    // Construct from raw chunks.  chunk data from raw chunks.
    pub fn from_chunks(chunks: &[Chunk], file_boundaries: Vec<usize>) -> Self {
        debug_assert_le!(chunks.len(), *MAX_XORB_CHUNKS);

        let mut data = Vec::with_capacity(chunks.len());
        let mut chunk_seq_entries = Vec::with_capacity(chunks.len());

        // Build the sequences.
        let mut pos = 0;
        for c in chunks {
            chunk_seq_entries.push(XorbChunkSequenceEntry::new(c.hash, c.data.len(), pos));
            data.push(c.data.clone());
            pos += c.data.len();
        }
        let num_bytes = pos;

        debug_assert_le!(num_bytes, *MAX_XORB_BYTES);

        let hash_and_len: Vec<_> = chunks.iter().map(|c| (c.hash, c.data.len() as u64)).collect();
        let xorb_hash = xorb_hash(&hash_and_len);

        // Build the MDBXorbInfo struct.
        let metadata = XorbChunkSequenceHeader::new(xorb_hash, chunks.len(), num_bytes);

        let xorb_info = MDBXorbInfo {
            metadata,
            chunks: chunk_seq_entries,
        };

        RawXorbData {
            data,
            xorb_info,
            file_boundaries,
        }
    }

    pub fn hash(&self) -> MerkleHash {
        self.xorb_info.metadata.xorb_hash
    }

    pub fn num_bytes(&self) -> usize {
        let n = self.xorb_info.metadata.num_bytes_in_xorb as usize;

        debug_assert_eq!(n, self.data.iter().map(|c| c.len()).sum::<usize>());

        n
    }
}

pub mod test_utils {
    use super::RawXorbData;

    pub fn raw_xorb_to_vec(xorb: &RawXorbData) -> Vec<u8> {
        let mut new_vec = Vec::with_capacity(xorb.num_bytes());

        for ch in xorb.data.iter() {
            new_vec.extend_from_slice(ch);
        }

        new_vec
    }
}
