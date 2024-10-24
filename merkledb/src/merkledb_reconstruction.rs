use merklehash::MerkleHash;

use crate::error::*;
use crate::internal_methods::*;
use crate::merkledbbase::MerkleDBBase;
use crate::merklenode::*;
///
///  Describes methods for performing FILE2CAS or CAS2FILE reconstructions from a MerkleDB
pub trait MerkleDBReconstruction: MerkleDBBase {
    /**
     * Look for all CAS's which are dependent exclusively on descendents of
     * this node, and returns the concatenation of FILE ranges needed to
     * reconstruct each CAS node.  Only traverses nodes
     * which were inserted after after_sequence.
     *
     * Details:
     * There are situations where
     * the result may be incomplete and hence is misleading.
     * Say I have the following graph:
     *
     * F1   F2   [CAS1]
     *  \    \   /   /
     *   \    \ /   /
     *    \    X   /
     *     \  / \ /
     *       N   M
     *
     * If I query file_to_cas(F1), I will get the correct range information
     * (how to construct F1).
     *
     * But if I query cas_to_file(F1) this gets a bit strange:
     * The initial search will find all all the CAS nodes which are dependent
     * on F1 and will find CAS1. However since F1 is queried, it will only find
     * the F1 relation and the resultant range will be incomplete since CAS1
     * is really produced by blocks from F1 and F2. This will then be filtered
     * out and so cas_to_file(F1) will not return CAS1.
     *
     * Really if you want to find all CASes needed for a given file,
     * use find_all_file_to_cas().
     */
    fn reconstruct_from_file(&self, root: &[MerkleNode]) -> Result<Vec<(MerkleHash, Vec<ObjectRange>)>> {
        let ret = find_reconstruction(self, root, NodeDataType::FILE)?;
        Ok(ret
            .into_iter()
            .filter(|(h, range)| {
                let n = self.find_node(h).unwrap();
                let total_len: usize = range.iter().map(|x| x.end - x.start).sum();
                n.len() == total_len
            })
            .collect())
    }

    /**
     * Look for all FILE which are only dependent exclusively on
     * descendents of this node
     * and returns the concatenation of CAS ranges
     * needed to reconstruct each FILE node.
     * Only traverses nodes which were inserted after after_sequence.
     */
    fn reconstruct_from_cas(&self, root: &[MerkleNode]) -> Result<Vec<(MerkleHash, Vec<ObjectRange>)>> {
        let ret = find_reconstruction(self, root, NodeDataType::CAS)?;
        // we need to filter this
        Ok(ret
            .into_iter()
            .filter(|(h, range)| {
                let n = self.find_node(h).unwrap();
                let total_len: usize = range.iter().map(|x| x.end - x.start).sum();
                n.len() == total_len
            })
            .collect())
    }

    fn find_all_leaves(&self, root: &MerkleNode) -> Result<Vec<MerkleNode>> {
        Ok(find_descendent_reconstructor(self, root, |n, _| n.children().is_empty())?
            .root_ranges_in_descendent
            .iter()
            .map(|x| self.find_node_by_id(x.0).unwrap())
            .collect())
    }

    /**
     *  From a given set of cas roots, return a list of data hashes and ranges.  
     *
     *  Each ObjectRange in the return value corresponds to the hash of a file
     *  node and the  of bytes in that file that equals one chunk.       *
     */
    fn find_cas_node_file_data(&self, cas_root: &MerkleNode) -> Result<Vec<ObjectRange>> {
        // First, we need to get all the base nodes for each of the leaves here.  The return
        // value of find_reconstruction has the ordered list of base data to be used.
        let file_node_ranges = find_reconstruction(self, &[cas_root.clone()], NodeDataType::FILE)?;

        // To get all the proper chunk boundaries, we need to use all the leaves
        // to determine the ranges of these nodes.

        let leaves: Vec<MerkleNode> = find_descendent_reconstructor(self, cas_root, |n, _| n.children().is_empty())?
            .root_ranges_in_descendent
            .iter()
            .map(|x| self.find_node_by_id(x.0).unwrap())
            .collect();

        let mut ret: Vec<ObjectRange> = Vec::new();

        assert!(!file_node_ranges.is_empty());

        let mut fnr_idx: usize = 0;
        let mut fnr_range_idx: usize = 0;

        let mut in_node_start_idx: usize = 0;

        for i in 0..leaves.len() {
            assert_ne!(fnr_idx, file_node_ranges.len());
            assert_ne!(fnr_range_idx, file_node_ranges[fnr_idx].1.len());

            let in_node_end_idx = in_node_start_idx + leaves[i].len();
            let cur_src_range = &(file_node_ranges[fnr_idx].1[fnr_range_idx]);

            ret.push(ObjectRange {
                hash: file_node_ranges[fnr_idx].0,
                start: cur_src_range.start + in_node_start_idx,
                end: cur_src_range.start + in_node_end_idx,
            });

            // Advance the pointer in the subnodes
            if in_node_end_idx == cur_src_range.end - cur_src_range.start {
                fnr_range_idx += 1;
                in_node_start_idx = 0;

                if fnr_range_idx == file_node_ranges[fnr_idx].1.len() {
                    fnr_idx += 1;
                    fnr_range_idx = 0;

                    if fnr_idx == file_node_ranges.len() {
                        // If this is true, then we should exactly be on the last loop.
                        assert_eq!(i, leaves.len() - 1);
                    }
                }
            } else {
                // The boundaries should match up exactly
                assert!(in_node_end_idx < cur_src_range.end - cur_src_range.start);
            }
        }

        assert_eq!(leaves.len(), ret.len());
        Ok(ret)
    }

    fn find_all_descendents_of_type(&self, root: MerkleNode, nodetype: NodeDataType) -> Result<Vec<MerkleNode>> {
        if self.node_attributes(root.id()).unwrap_or_default().is_type(nodetype) {
            Ok(vec![root])
        } else {
            Ok(find_descendent_reconstructor(self, &root, |_, attr| attr.is_type(nodetype))?
                .root_ranges_in_descendent
                .iter()
                .map(|x| self.find_node_by_id(x.0).unwrap())
                .collect())
        }
    }
}
