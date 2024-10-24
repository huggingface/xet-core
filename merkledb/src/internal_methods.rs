use std::collections::{HashMap, HashSet};

use super::constants::*;
use super::merklenode::*;
use crate::error::*;
use crate::merkledbbase::MerkleDBBase;
/************************************************************************* */
/*  */
/* Internal Algorithms */
/*  */
/************************************************************************* */

/**
 * Inserts a leaf node described by just a hash and a length into a database,
 * returning an existing node if one already exists.
 */
pub fn node_from_hash(db: &mut (impl MerkleDBBase + ?Sized), hash: &MerkleHash, len: usize) -> MerkleNode {
    db.add_node(hash, len, Vec::new())
}

/**
 * Inserts an interior node described by just a hash and a length into a database,
 * returning an existing node if one already exists.
 */
pub fn node_from_children(db: &mut (impl MerkleDBBase + ?Sized), children: &[MerkleNode], len: usize) -> MerkleNode {
    let hash = hash_node_sequence(children);
    let children_id: Vec<_> = children.iter().map(|x| (x.id(), x.len())).collect();
    db.add_node(&hash, len, children_id)
}

/**
 * Builds one level of hashes above "nodes".
 * Returns a pair of arrays (parent_of_node, parents)
 *
 * parents is the level set above nodes.
 * parent_of_node is the same length as nodes, and parent_of_node[i] is
 * the id of the parent of node[i].
 *
 * For instance, if given a list of nodes [n1,n2,n3,n4], with IDs [1,2,3,4]
 * this function may return
 *
 * ```ignore
 * parent_of_node = [5, 5, 6, 6]
 * parents = [n5, n6]
 * ```
 *
 * This means that 2 parent nodes n5, n6 were created. Where n5 has 2 children
 * [n1,n2], and n6 has two children [n3,n4].
 */
pub fn merge_one_level(
    db: &mut (impl MerkleDBBase + ?Sized),
    nodes: &[MerkleNode],
) -> (Vec<MerkleNodeId>, Vec<MerkleNode>) {
    /*
     * We basically loop through the set of nodes tracking a window between
     * cur_children_start_idx and idx (current index).
     * [. . . . . . . . . . . ]
     *          ^   ^
     *          |   |
     *  start_idx   |
     *              |
     *             idx
     *
     * When the current node at idx satisfies the cut condition:
     *  - the hash % MEAN_TREE_BRANCHING_FACTOR == 0: assuming a random
     *  hash distribution, this implies on average, the number of children
     *  is MEAN_TREE_BRANCHING_FACTOR,
     *  - OR this is the last node in the list.
     *  - subject to each parent must have at least 2 children, and at most MEAN_TREE_BRANCHING_FACTOR * 2 children:
     *    This ensures that the graph always has at most 1/2 the number of parents as children. and we don't have too
     *    wide branches.
     *
     * We build a parent, update the indices, and shift start_idx to the
     * next index (idx + 1) to set up the window for the next parent.
     */
    let total_children = nodes.len();
    // return value. the set of parents created
    let mut parents: Vec<MerkleNode> = Vec::new();
    // return value. the parent of each input node
    let mut parent_of_node: Vec<MerkleNodeId> = vec![0; nodes.len()];

    // the start of the window for the next parent
    let mut cur_children_start_idx = 0;
    // The total length of the string represented by the next parent.
    // (we are tracking this because it is needed metadata for the node
    // creation)
    let mut cur_children_total_len: usize = 0;

    for (idx, node) in nodes.iter().enumerate() {
        cur_children_total_len += node.len();
        let test_hash = node.hash();
        // minumum number of children is 2
        // maximum number of children is 2* mean_branching_factor
        // and we test for a cut by
        // modding last 64 bits of the hash with the mean branching factor
        let num_children_so_far = idx - cur_children_start_idx;
        // sorry. I like the extra parens here. Its hard to remember which has
        // precedence && or ||
        #[allow(unused_parens)]
        if (num_children_so_far >= 2 && test_hash[3] % MEAN_TREE_BRANCHING_FACTOR == 0)
            || num_children_so_far >= 2 * (MEAN_TREE_BRANCHING_FACTOR as usize)
            || idx + 1 == total_children
        {
            // cut a parent node here
            let parent_node = node_from_children(db, &nodes[cur_children_start_idx..=idx], cur_children_total_len);
            let parent_id = parent_node.id();
            parents.push(parent_node);
            #[allow(clippy::needless_range_loop)]
            for ch_index in cur_children_start_idx..=idx {
                parent_of_node[ch_index] = parent_id;
            }
            // set up for the next window
            cur_children_total_len = 0;
            cur_children_start_idx = idx + 1;
        }
    }
    (parent_of_node, parents)
}

/**
 * A simple helper function that loops over nodes and parent_of_node
 * (as produced by merge_one_level) and assigns the parent to each node.
 */
fn assign_node_parents(
    db: &mut (impl MerkleDBBase + ?Sized),
    nodes: &mut [MerkleNode],
    parent_of_node: &[u64],
    parent_type: NodeDataType,
) {
    for (node, parent_id) in nodes.iter().zip(parent_of_node.iter()) {
        let mut attr = db.node_attributes(node.id()).unwrap_or_default();
        if attr.parent(parent_type) == ID_UNASSIGNED {
            attr.set_parent(parent_type, *parent_id);
            db.set_node_attributes(node.id(), &attr);
        }
    }
}

/**
 * This is the return type of `find_descendent_reconstructor`.
 * It is used to describe how the root relates to the descendents found
 * and vice versa.
 * - For the root, how to put together the descendent nodes to construct the value at the root.
 * - and for each descendent, what subrange of the root does it correspond to.
 */
#[derive(Default)]
pub struct RootConstructionDescription {
    pub root_id: MerkleNodeId,
    /**
     * A collection of descendent ranges that when put together will make up the root node
     */
    pub descendent_ranges_for_root: Vec<ObjectRangeById>,

    /**
     * For each descendent range, the corresponding range in the root node.
     */
    pub root_ranges_in_descendent: Vec<(MerkleNodeId, ObjectRangeById)>,
}

/**
 * Main recursive implementation for find_descendent_reconstructor
 */
pub fn find_descendent_reconstructor_impl<'a>(
    db: &(impl MerkleDBBase + ?Sized),
    node: &MerkleNode,
    root_id: MerkleNodeId,
    mut root_start_byte: usize,
    visited: &'a mut HashMap<MerkleNodeId, Vec<ObjectRangeById>>,
    condition: &impl Fn(&MerkleNode, &MerkleNodeAttributes) -> bool,
    ret_root_ranges_in_descendent: &mut Vec<(MerkleNodeId, ObjectRangeById)>,
) -> Result<&'a mut Vec<ObjectRangeById>> {
    if visited.contains_key(&node.id()) {
        return Ok(visited.get_mut(&node.id()).unwrap());
    }

    let attr = db.node_attributes(node.id()).unwrap();
    if condition(node, &attr) {
        ret_root_ranges_in_descendent.push((
            node.id(),
            ObjectRangeById {
                id: root_id,
                start: root_start_byte,
                end: root_start_byte + node.len(),
            },
        ));
        let myrange = ObjectRangeById {
            id: node.id(),
            start: 0,
            end: node.len(),
        };
        visited.insert(node.id(), vec![myrange]);
    } else {
        // if this is a leaf, we are in trouble. That means we that there is
        // a path in which the user specified condition is never achieved.
        // TODO: this will generally indicate a graph inconsistency and we may
        // need a way to diagnose this.
        if node.children().is_empty() {
            return Err(MerkleDBError::GraphInvariantError("Reached a leaf while searching for descendents".into()));
        }
        let mut concat_desc_range: Vec<ObjectRangeById> = Vec::new();
        for ch in node.children().iter() {
            let chnode = db.find_node_by_id(ch.0).unwrap();
            let desc_range = find_descendent_reconstructor_impl(
                db,
                &chnode,
                root_id,
                root_start_byte,
                visited,
                condition,
                ret_root_ranges_in_descendent,
            )?;
            concat_desc_range.extend(desc_range.iter().cloned());
            root_start_byte += chnode.len();
        }
        visited.insert(node.id(), concat_desc_range);
    }
    Ok(visited.get_mut(&node.id()).unwrap())
}
/**
 * Looks for all descendents of a given node matching a condition passed as
 * a function (condition) and returns a description of the forward-backward
 * relationship between the descendents and the root and vice versa.
 *
 * root: The root node to start walking from
 * condition: a user defined function which should return true if this node
 *            should be returned. The descendents of a "true" node will not
 *            be traversed.
 *
 * Returns an instance of RootConstructionDescription.
 *
 * Note that the condition must be satisfiable at some node on every path from
 * the root to the leaf. That is to say on a recursive DFS walk of the graph
 * which backtracks everytime condition=True, I should not ever reach a leaf
 * where condition(leaf) evaluates to False. Right now on such a failure,
 * we trigger an assertion failure. TODO will be to catch the failure and provide
 * remedies; generally in such a situation, it means that we have a graph
 * inconsistency.
 */
pub fn find_descendent_reconstructor(
    db: &(impl MerkleDBBase + ?Sized),
    root: &MerkleNode,
    condition: impl Fn(&MerkleNode, &MerkleNodeAttributes) -> bool,
) -> Result<RootConstructionDescription> {
    let root_id = root.id();
    let mut visited: HashMap<MerkleNodeId, Vec<ObjectRangeById>> = HashMap::new();
    let mut ret = RootConstructionDescription {
        root_id,
        ..Default::default()
    };

    ret.descendent_ranges_for_root.clone_from(find_descendent_reconstructor_impl(
        db,
        root,
        root_id,
        0,
        &mut visited,
        &condition,
        &mut ret.root_ranges_in_descendent,
    )?);
    Ok(ret)
}

/**
 * Find a way to reconstruct the given root nodes using ranges of nodes
 * of type dest_tag.
 *
 * As an example, lets consider the case where root node is a FILE
 * and dest_tag is CAS.  In this case, this function will return the sequence of
 * CAS nodes that have to be read to reconstruct the root FILE.
 *
 * Returns None if unable to solve.
 */
pub fn find_reconstruction(
    db: &(impl MerkleDBBase + ?Sized),
    root: &[MerkleNode],
    dest_tag: NodeDataType,
) -> Result<Vec<(MerkleHash, Vec<ObjectRange>)>> {
    // to make this more concrete in the comments we will assume
    // src_tag == CAS and dest_tag = FILE

    // these are nodes with both CAS and file parents.
    // These will our "bridge" between CAS and FILE nodes.
    // Call this N.
    // nodes_between_src_and_dest is now a list of nodes in N, and for each node
    // it's range in root.
    let root_reconstructors: Vec<Result<RootConstructionDescription>> = root
        .iter()
        .map(|x| find_descendent_reconstructor(db, x, |_, attr| attr.has_data(dest_tag)))
        .collect();

    // make sure all succeeded
    if !root_reconstructors.iter().all(|x| x.is_ok()) {
        return Err(MerkleDBError::GraphInvariantError("Reconstruction infeasible".into()));
    }
    let root_reconstructors: Vec<RootConstructionDescription> = root_reconstructors.into_iter().flatten().collect();

    let mut nodes_between_src_and_dest: HashSet<MerkleNodeId> = Default::default();
    for aroot in &root_reconstructors {
        for desc in &aroot.root_ranges_in_descendent {
            nodes_between_src_and_dest.insert(desc.0);
        }
    }
    // The basic strategy here is conceptually as follows
    // Since N has both CAS and FILE parents then the graph conceptually looks
    // like this
    //
    // CAS    FILE
    //  \     /
    //   \   /
    //    v v
    //     N
    //
    // (where FILE, CAS and N are all collection of nodes)
    // So to reconstruct say CAS from FILE, we
    // 1. Find how to construct CAS with nodes in N
    // 2. Find how to construct N from FILE
    // 3. combine (1) and (2) to get from CAS->FILE

    let src_to_n: Vec<(MerkleNodeId, Vec<ObjectRangeById>)> = root_reconstructors
        .into_iter()
        .map(|x| (x.root_id, x.descendent_ranges_for_root))
        .collect();

    // This gives the relationship between N->File
    let n_to_dest: HashMap<MerkleNodeId, ObjectRangeById> = nodes_between_src_and_dest
        .iter()
        .map(|x| db.find_node_by_id(*x).unwrap())
        .map(|x| (x.id(), find_ancestor_reconstructor(db, &x, dest_tag).unwrap()))
        .collect();

    // each entry n_to_dest[N] = [F, start, end]
    // tells me that I can construct the value at N with F[start, end]

    // we want to get cas to file, and so how that works is tht
    //
    // for each n_1[start_1,end_1], we map n_1 through n_to_dest
    // (say n_to_dest[n_1] = [f_k, start_k, end_k]
    //
    // substituting, this gives us
    //
    // f_k[start_k, end_k][start_1, end_1]
    //
    // Then to combine this into a single range, we just note that
    // length = end_1 - start_1, and so the combined range is just
    // f_k[start_k + start_1, length]
    //
    Ok(src_to_n
        .iter()
        .map(|(id, ranges)| {
            let new_ranges: Vec<ObjectRange> = ranges
                .iter()
                .map(|range| {
                    let length = range.end - range.start;
                    let frange = &n_to_dest[&range.id];
                    // sanity check
                    assert!(length <= frange.end - frange.start);
                    let new_start = range.start + frange.start;
                    ObjectRange {
                        hash: *db.find_node_by_id(frange.id).unwrap().hash(),
                        start: new_start,
                        end: new_start + length,
                    }
                })
                .collect();
            (*db.find_node_by_id(*id).unwrap().hash(), simplify_ranges(&new_ranges[..]))
        })
        .collect())
}

/**
 * Find the subrange of a parent of type reconstructor_type that can
 * reconstruct the value of this node.
 *
 * This function simply walks up the parent attribute for the target type
 * until it finds a node taged with the target typed. A bit of arithmetic
 * is needed to figure out the original node's range inside the parent's range
 * every iteration.
 */
pub fn find_ancestor_reconstructor(
    db: &(impl MerkleDBBase + ?Sized),
    node: &MerkleNode,
    reconstructor_type: NodeDataType,
) -> Result<ObjectRangeById> {
    let mut cur = node.clone();
    // the subrange of 'node' inside of 'cur'
    let mut range_start: usize = 0;
    loop {
        let attr = db.node_attributes(cur.id()).unwrap();
        assert!(attr.has_data(reconstructor_type));
        if attr.is_type(reconstructor_type) {
            return Ok(ObjectRangeById {
                id: cur.id(),
                start: range_start,
                end: range_start + node.len(),
            });
        }
        // we ascend
        let parent = attr.parent(reconstructor_type);
        if parent != ID_UNASSIGNED {
            let parent = db.find_node_by_id(parent).unwrap();
            let mut running_sum: usize = 0;
            // range_start is node's position relative to cur right now.
            //
            // we need to shift it to be relative to the parent
            // so we find where cur's position is in the list of
            // siblings and shift the range start.
            for (sibling, sibling_len) in parent.children().iter() {
                if *sibling == cur.id() {
                    range_start += running_sum;
                    break;
                }
                running_sum += sibling_len;
            }
            cur = parent;
            //
        } else {
            // something wrong. we should have hit
            // a node of the reconstructor type, but such a node
            // was not found
            return Err(MerkleDBError::GraphInvariantError(
                "Parent invariant violated. Did not reach a parent of appropriate type.".into(),
            ));
        }
    }
}
/**
 * Performs a level-wise merge of a collection of nodes, handling the
 * CAS assignment at the same time.
 *
 * The basic mechanism is that when a file is added, it produces a collection
 * of chunks (nodes). That is filtered to the subset which does not have data
 * since we may have chunks which were stored before (nodes_without_cas_entry).
 *
 * nodes are level-wise merged to a single root node which is a FILE
 * and parents are assigned accordingly for all the nodes in between.
 *
 * nodes_without_cas_entry are similarly level-wise merged but with a heuristic
 * along the way where a CAS node may be constructed if the node size is
 * which is sometimes.
 *
 */
pub fn merge(
    db: &mut (impl MerkleDBBase + ?Sized),
    mut nodes: Vec<MerkleNode>,
    root_is_file: bool,
    root_is_cas: bool,
) -> MerkleNode {
    // merge nodes to get the file representation
    assert!(!nodes.is_empty());
    while nodes.len() > 1 {
        let (parent_of_node, mut parents) = merge_one_level(db, &nodes);
        if root_is_file {
            assign_node_parents(db, &mut nodes, &parent_of_node, NodeDataType::FILE);
        }
        if root_is_cas {
            assign_node_parents(db, &mut nodes, &parent_of_node, NodeDataType::CAS);
        }
        nodes = std::mem::take(&mut parents);
    }
    let r = &nodes[0];
    if root_is_file {
        let mut attr = db.node_attributes(r.id()).unwrap_or_default();
        attr.set_file();
        db.set_node_attributes(r.id(), &attr);
    }
    if root_is_cas {
        let mut attr = db.node_attributes(r.id()).unwrap_or_default();
        attr.set_cas();
        db.set_node_attributes(r.id(), &attr);
    }
    nodes[0].clone()
}

/// Given a list of nodes with a particular node attribute
/// set the corresponding parent attribute in each descendent.
pub fn assign_all_parents(
    db: &mut (impl MerkleDBBase + ?Sized),
    mut roots: Vec<MerkleNodeId>,
    root_type: NodeDataType,
) {
    // we run a BFS against roots
    let mut next_roots: HashSet<MerkleNodeId> = HashSet::new();
    while !roots.is_empty() {
        for id in roots {
            if let Some(node) = db.find_node_by_id(id) {
                for (chid, _) in node.children() {
                    if let Some(mut attr) = db.node_attributes(*chid) {
                        attr.set_parent(root_type, id);
                        db.set_node_attributes(*chid, &attr);
                        next_roots.insert(*chid);
                    }
                }
            }
        }
        roots = next_roots.drain().collect();
        next_roots.clear();
    }
}
