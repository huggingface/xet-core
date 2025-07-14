use std::collections::HashSet;

use merklehash::MerkleHash;

use crate::chunk_iterator::*;
use crate::constants::*;
use crate::internal_methods::*;
use crate::merkledbbase::*;
use crate::merklenode::*;

/**
 * An opaque struct used to stage a batch collection of inserts (like a directory)
 */
pub struct InsertionStaging {
    /// all the file roots inserted so far
    file_roots: Vec<MerkleNode>,
    /// all the leaves without CAS entries
    nodes_without_cas_entry: Vec<MerkleNode>,
    /// sum of length of all the nodes_without_cas_entry
    nodes_without_cas_entry_length: usize,
    /// hashset of all the unique ids in nodes_without_cas_entry
    ids_in_nodes_without_cas_entry: HashSet<MerkleNodeId>,
    /// all new cas roots created
    cas_roots: Vec<MerkleNode>,
    /// We will aim to create CAS nodes of this size
    cas_block_size: usize,
}
impl InsertionStaging {
    /// Merge with another Insertion Staging struct
    pub fn combine(&mut self, mut other: InsertionStaging) {
        self.file_roots.append(&mut other.file_roots);
        self.nodes_without_cas_entry.append(&mut other.nodes_without_cas_entry);
        self.nodes_without_cas_entry_length += other.nodes_without_cas_entry_length;
    }

    /// Modifies the target CAS Block size.
    pub fn set_target_cas_block_size(&mut self, cas_block_size: usize) {
        self.cas_block_size = cas_block_size;
    }
    /// constructs all the required cas nodes by grouping them into groups
    /// of up to the cas_block_size (default TARGET_CAS_BLOCK_SIZE)
    fn build_cas_nodes(&mut self, db: &mut (impl MerkleDBBase + ?Sized), flush: bool) {
        if self.nodes_without_cas_entry_length >= self.cas_block_size {
            let mut running_sum: usize = 0;
            let mut start = 0;
            for i in 0..self.nodes_without_cas_entry.len() {
                running_sum += self.nodes_without_cas_entry[i].len();
                if running_sum >= self.cas_block_size {
                    let end = i + 1;
                    let cas_root = merge(db, self.nodes_without_cas_entry[start..end].to_vec(), false, true);
                    self.cas_roots.push(cas_root);

                    for j in start..end {
                        self.ids_in_nodes_without_cas_entry
                            .remove(&self.nodes_without_cas_entry[j].id());
                    }
                    self.nodes_without_cas_entry_length -= running_sum;
                    start = end;
                    running_sum = 0;
                }
            }
            self.nodes_without_cas_entry = self.nodes_without_cas_entry[start..].to_vec();
        }
        if flush && !self.nodes_without_cas_entry.is_empty() {
            let cas_root = merge(db, std::mem::take(&mut self.nodes_without_cas_entry), false, true);
            self.cas_roots.push(cas_root);
        }
    }
}

pub trait MerkleDBHighLevelMethodsV1: MerkleDBBase {
    /** creates a new insertion staging object. The basic usage is
     *  - start_insertion_staging
     *  - add_file
     *  - add_file
     *  - add_file
     *  - finalize
     */
    fn start_insertion_staging(&self) -> InsertionStaging {
        InsertionStaging {
            file_roots: Vec::new(),
            nodes_without_cas_entry: Vec::new(),
            nodes_without_cas_entry_length: 0,
            ids_in_nodes_without_cas_entry: HashSet::new(),
            cas_roots: Vec::new(),
            cas_block_size: TARGET_CAS_BLOCK_SIZE,
        }
    }

    /// Adds a file
    fn add_file(&mut self, staging: &mut InsertionStaging, chunk: &[ChunkInfo]) -> MerkleHash {
        if chunk.is_empty() {
            return MerkleHash::default();
        }
        let ch: Vec<MerkleNode> = chunk.iter().map(|i| node_from_hash(self, &i.hash, i.length)).collect();

        // we filter nodes_without_cas_entry to be unique
        // hashset return true if the set does not have it already present
        // and false otherwise. So this is a cute compact stable way to
        // select only unique entries.
        let mut nodes_without_cas_entry: Vec<MerkleNode> = ch
            .iter()
            .filter(|x| !self.node_attributes(x.id()).unwrap_or_default().has_cas_data())
            .filter(|x| staging.ids_in_nodes_without_cas_entry.insert(x.id()))
            .cloned()
            .collect();
        let file_root = merge(self, ch, true, false);
        staging.nodes_without_cas_entry_length += nodes_without_cas_entry.iter().map(|x| x.len()).sum::<usize>();
        staging.nodes_without_cas_entry.append(&mut nodes_without_cas_entry);

        staging.build_cas_nodes(self, false);
        let hash = *file_root.hash();
        staging.file_roots.push(file_root);
        hash
    }

    /// Finalizes the insertion.
    fn finalize(&mut self, mut staging: InsertionStaging) -> MerkleNode {
        if staging.file_roots.is_empty() {
            return MerkleNode::default();
        }
        // sort by length then hash
        let comparator = |a: &MerkleNode, b: &MerkleNode| {
            if a.len() != b.len() {
                a.len().partial_cmp(&b.len()).unwrap()
            } else {
                a.hash().partial_cmp(b.hash()).unwrap()
            }
        };
        staging.file_roots.sort_unstable_by(comparator);
        staging.build_cas_nodes(self, true);
        merge(self, staging.file_roots, false, false)
    }
}
