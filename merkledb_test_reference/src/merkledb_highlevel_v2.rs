use crate::chunk_iterator::*;
use crate::internal_methods::*;
use crate::merkledbbase::*;
use crate::merklenode::*;

pub trait MerkleDBHighLevelMethodsV2: MerkleDBBase {
    /// Adds a chunk to the database
    /// Returns (node, new_node)
    /// new_node = true if this is a new chunk that has never been seen
    fn add_chunk(&mut self, chunk: &ChunkInfo) -> (MerkleNode, bool) {
        self.maybe_add_node(&chunk.hash, chunk.length, Vec::new())
    }

    /// Merges a collection of chunks to form a file node
    fn merge_to_file(&mut self, nodes: &[MerkleNode]) -> MerkleNode {
        merge(self, nodes.to_owned(), true, false)
    }

    /// Merges a collection of chunks to form a CAS node
    fn merge_to_cas(&mut self, nodes: &[MerkleNode]) -> MerkleNode {
        merge(self, nodes.to_owned(), false, true)
    }

    /// Merges a collection of chunks where the root is untagged
    fn merge(&mut self, nodes: &[MerkleNode]) -> MerkleNode {
        merge(self, nodes.to_owned(), false, false)
    }
}
