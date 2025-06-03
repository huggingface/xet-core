use std::collections::HashSet;

use crate::chunk_iterator::ChunkInfo;
use crate::internal_methods::*;
use crate::merkledb_highlevel_v1::*;
use crate::merkledb_reconstruction::MerkleDBReconstruction;
use crate::merkledbbase::MerkleDBBase;
use crate::merklememdb::MerkleMemDB;
use crate::merklenode::*;

pub trait MerkleDBDebugMethods: MerkleDBBase + MerkleDBReconstruction {
    fn print_node_details(&self, node: &MerkleNode) -> String {
        let attr = self.node_attributes(node.id()).unwrap_or_default();
        let mut ret = String::new();
        if attr.has_cas_data() {
            let cas_report = if let Ok(r) = find_ancestor_reconstructor(self, node, NodeDataType::CAS) {
                format!(
                    "\tSubstring [{}, {}) of CAS entry {}\n",
                    r.start,
                    r.end,
                    self.find_node_by_id(r.id).map_or("?".to_string(), |n| n.hash().to_string())
                )
            } else {
                "\tHas CAS data but unable to derive origin CAS node\n".to_string()
            };
            ret.push_str(&cas_report);
        }
        if attr.has_file_data() {
            let cas_report = if let Ok(r) = find_ancestor_reconstructor(self, node, NodeDataType::FILE) {
                format!(
                    "\tSubstring [{}, {}) of FILE entry {}\n",
                    r.start,
                    r.end,
                    self.find_node_by_id(r.id).map_or("?".to_string(), |n| n.hash().to_string())
                )
            } else {
                "\tHas FILE data but unable to derive origin FILE node\n".to_string()
            };
            ret.push_str(&cas_report);
        }
        ret.push('\n');
        if attr.has_file_data() {
            let recon_report = if let Ok(res) = self.reconstruct_from_cas(&[node.clone()]) {
                format!("CAS Reconstruction: {res:?}")
            } else {
                "\tUnable to reconstruct from CAS\n".to_string()
            };
            ret.push_str(&recon_report);
        }

        ret
    }
    /**
     * Checks that Hash->Id and Id->Hash match up
     *
     * Returns true if the invariants pass, and false otherwise.
     */
    fn check_hash_invertibility_invariant(&self) -> bool {
        let mut ret = true;
        for i in 0..self.get_sequence_number() {
            // there is no requirement that IDs be sequential just monotonic
            if let Some(node) = self.find_node_by_id(i as MerkleNodeId) {
                if let Some(hashtoid) = self.hash_to_id(node.hash()) {
                    if hashtoid != (i as MerkleNodeId) {
                        eprintln!("Node {node:?} hash_to_id resolves to {hashtoid:?} which should be {i:?}");
                        ret = false;
                    }
                } else {
                    eprintln!("Node {node:?} hash_to_id resolves to None which should be {i:?}");
                }
            }
        }
        ret
    }
    /**
     * Checks that children and parent of every node exists.
     *
     * Returns true if the invariants pass, and false otherwise.
     */
    fn check_reachability_invariant(&self) -> bool {
        let mut id_exists: HashSet<MerkleNodeId> = HashSet::new();
        for i in 0..self.get_sequence_number() {
            // there is no requirement that IDs be sequential just monotonic
            if self.find_node_by_id(i as MerkleNodeId).is_some() {
                id_exists.insert(i);
            }
        }
        let mut ret = true;
        for i in 0..self.get_sequence_number() {
            if let Some(node) = self.find_node_by_id(i as MerkleNodeId) {
                let mut chlensum: usize = 0;
                for (ch, len) in node.children().iter() {
                    if !id_exists.contains(ch) {
                        eprintln!("Child {ch:?} of Node {node:?} does not exist");
                        ret = false;
                    }
                    chlensum += len;
                }
                if !node.children().is_empty() && node.len() != chlensum {
                    eprintln!("Sum of children length do not sum to node length. {node:?}");
                }
                if let Some(attr) = self.node_attributes(i as MerkleNodeId) {
                    let cas_parent = attr.cas_parent();
                    let file_parent = attr.file_parent();
                    if cas_parent != 0 && !id_exists.contains(&cas_parent) {
                        eprintln!("CAS Parent in attribute {attr:?} of Node {node:?} does not exist");
                        ret = false;
                    }
                    if file_parent != 0 && !id_exists.contains(&file_parent) {
                        eprintln!("FILE Parent in attribute {attr:?} of Node {node:?} does not exist");
                        ret = false;
                    }
                }
            }
        }
        ret
    }
    /**
     * Check that File and Cas parents lead to a File or Cas node
     */
    fn check_parent_invariant(&self) -> bool {
        let mut ret = true;
        for i in 0..self.get_sequence_number() {
            if let Some(node) = self.find_node_by_id(i as MerkleNodeId) {
                if let Some(attr) = self.node_attributes(i as MerkleNodeId) {
                    if attr.has_cas_data() {
                        let f = find_ancestor_reconstructor(self, &node, NodeDataType::CAS);
                        if f.is_err() {
                            eprintln!("Parent invariant broken on node {node:?}. Unable to track to CAS root");
                        }
                        ret &= f.is_ok();
                    } else if attr.has_file_data() {
                        let f = find_ancestor_reconstructor(self, &node, NodeDataType::FILE);
                        ret &= f.is_ok();
                        if f.is_err() {
                            eprintln!("Parent invariant broken on node {node:?}. Unable to track to File root");
                        }
                    };
                }
            }
        }
        ret
    }

    /// if A has B as a parent, then B must have A as a child
    fn check_parent_invariant_basic(&self) -> bool {
        let mut ret = true;
        for i in 0..self.get_sequence_number() {
            if let Some(node) = self.find_node_by_id(i as MerkleNodeId) {
                let nodeattr = self.node_attributes(node.id()).unwrap();
                let cas_parent = nodeattr.cas_parent();
                if cas_parent > 0 {
                    if let Some(parent) = self.find_node_by_id(cas_parent) {
                        if !parent.children().iter().any(|x| x.0 == node.id()) {
                            eprintln!("CAS Parent node of {node:?} has no children. Parent: {parent:?}");
                            ret = false;
                        }
                    } else {
                        eprintln!("CAS Parent node of {node:?} not found, Attr: {nodeattr:?}");
                        ret = false;
                    }
                }
                let file_parent = nodeattr.file_parent();
                if file_parent > 0 {
                    if let Some(parent) = self.find_node_by_id(file_parent) {
                        if !parent.children().iter().any(|x| x.0 == node.id()) {
                            eprintln!("FILE Parent node of {node:?} has no children. Parent: {parent:?}");
                            ret = false;
                        }
                    } else {
                        eprintln!("FILE Parent node of {node:?} not found, Attr: {nodeattr:?}");
                        ret = false;
                    }
                }
            }
        }
        ret
    }

    fn every_root_is_cas_and_file_reachable_invariant(&self) -> bool {
        let mut ret = true;
        for i in 0..self.get_sequence_number() {
            if let Some(node) = self.find_node_by_id(i as MerkleNodeId) {
                let nodeattr = self.node_attributes(node.id()).unwrap();
                // we only check roots. i.e. nodes with no parents.
                if nodeattr.cas_parent() > 0 || nodeattr.file_parent() > 0 {
                    continue;
                }
                // every node must be cas to file or file to cas solvable
                let cas_to_file = self.reconstruct_from_file(&[node.clone()]).unwrap();
                let file_to_cas = self.reconstruct_from_cas(&[node.clone()]).unwrap();
                if cas_to_file.is_empty() && file_to_cas.is_empty() {
                    let attr = self.node_attributes(node.id()).unwrap();
                    eprintln!(
                        "CAS File relation invariant broken for node {node:?}\n\
                        \tattr: {attr:?}\n\
                        \tcas_to_file: {cas_to_file:?}\n\
                        \tfile_to_cas: {file_to_cas:?}\n"
                    );
                    ret = false;
                }
                // check that the lengths match up for each cas_to_file,
                // file_to_cas entry
                //
                for (h, range) in cas_to_file {
                    let total_len: usize = range.iter().map(|x| x.end - x.start).sum();
                    let n = self.find_node(&h).unwrap();
                    if n.len() != total_len {
                        let attr = self.node_attributes(n.id()).unwrap();
                        eprintln!(
                            "When querying {node:?}, attr {nodeattr:?},\n\
                            \tCAS To file length mismatch for node {n:?}. attr: {attr:?}, \n\
                            \tranges acquired is {range:?}\n"
                        );
                        ret = false;
                    }
                }
                for (h, range) in file_to_cas {
                    let total_len: usize = range.iter().map(|x| x.end - x.start).sum();
                    let n = self.find_node(&h).unwrap();
                    if n.len() != total_len {
                        let attr = self.node_attributes(n.id()).unwrap();
                        eprintln!(
                            "When querying {node:?}, attr {nodeattr:?}, \n\
                            \tFile To CAS length mismatch for node {n:?}. \n\
                            \tattr: {attr:?}, ranges acquired is {range:?}\n"
                        );
                        ret = false;
                    }
                }
            }
        }
        ret
    }

    /// compare the given hash with what I get with a fresh insertion
    /// of hunks into a MerkleDB
    fn fresh_hash_of_chunks(&self, chunks: &[ChunkInfo]) -> MerkleHash {
        let mut db = MerkleMemDB::default();
        let mut staging = db.start_insertion_staging();
        db.add_file(&mut staging, chunks);
        let ret = db.finalize(staging);
        *ret.hash()
    }

    /// compare the CAS hash in the current database, with the hash
    /// computed by hashing the set of chunks making up the CAS.
    fn validate_db_cas_node(&self, cashash: &MerkleHash) -> bool {
        let cas_node = self.find_node(cashash).unwrap();
        let cas_leaves = self.find_all_leaves(&cas_node).unwrap();
        let chunks = cas_leaves
            .iter()
            .map(|x| ChunkInfo {
                hash: *x.hash(),
                length: x.len(),
            })
            .collect::<Vec<_>>();
        let newhash = self.fresh_hash_of_chunks(&chunks);
        if newhash != *cashash {
            eprintln!(
                "CAS Hash Validation Mismatch \n\
                \tExpecting {cashash:?}, got {newhash:?} \n\
                \tChunks are {chunks:?}\n"
            );
        }
        newhash == *cashash
    }

    fn validate_every_cas_node_hash(&self) -> bool {
        let mut ret = true;
        let mut cashashes: Vec<MerkleHash> = Vec::new();
        for i in 0..self.get_sequence_number() {
            if let Some(node) = self.find_node_by_id(i as MerkleNodeId) {
                if let Some(attr) = self.node_attributes(i as MerkleNodeId) {
                    if attr.is_cas() {
                        cashashes.push(*node.hash());
                    }
                }
            }
        }
        for hash in cashashes {
            ret &= self.validate_db_cas_node(&hash);
        }
        ret
    }
    fn all_invariant_checks(&self) -> bool {
        self.db_invariant_checks()
            && self.check_reachability_invariant()
            && self.check_hash_invertibility_invariant()
            && self.every_root_is_cas_and_file_reachable_invariant()
            && self.validate_every_cas_node_hash()
            && self.check_parent_invariant_basic()
    }

    fn only_file_invariant_checks(&self) -> bool {
        self.db_invariant_checks()
            && self.check_reachability_invariant()
            && self.check_hash_invertibility_invariant()
            && self.check_parent_invariant_basic()
    }
}
