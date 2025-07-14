/**
Basic Concepts
==============
The Merkle Tree (DAG) is a general DAG (Directed Acyclic Graph) of hashes and
how they relate to each other. Basically, each node represents a
a "byte string" and the DAG describes known substring relationships with each other.

For instance, take an arbitrary node in the graph, say a root node with hash
"AAA111". What this really means is that there is some byte
string that hashes to AAA111.  (For a very particular hash function which we
will describe later).

Furthermore, if the node AAA111 has 2 children "BBB222" and "CCC333", then
the concatenation of the byte strings which hash to "BBBB222" and "CCC333"
respectively *MUST* result in the byte string that hashed to "AAA111".

Formally, the following relationship is the primary invariant:


For a node $n$ with children $\Gamma(n)$, and a string $S_n$ s.t. $hash(S_n) = n$.
and $\forall c\in\Gamma(n) hash(S_c) = c$, then

  $S_n = \bigdot_c\in\Gamma(n) S_c$

where \bigdot represents concatenation. Note that the set of children \Gamma(n)
is ordered.


This relationship means that if we see a new node "FFF666" with children
["CCC333", and "ABABAB"], we know that the end of the string for "AAA111" must
matches the start of the string for "FFF666".

Note that the substring relationship only sufficient but not necessary. That is
if we have two strings with overlapping substrings, it is not guaranteed that
their descendent hashes will overlap at all as this is dependent on the tree
construction procedure.

Construction
------------
The key behind how the MerkleDAG works is that the hash function is exactly
the same as the tree construction procedure. That is Parent nodes are constructed
by hashing the hashes of its children. Thus as long as the children merging
algorithm and the leaf creation algorithm is deterministic, the same hash value
and most importantly, the entire tree structure will be deterministic for the
same inputs.

Metadata
--------
The MerkleDAG alone only provides information about how strings relate to each
other, however, in practice we need a lot more metadata to accomplish a storage
system.

1: We need to know which nodes correspond to actual files and how to
reconstruct them.
2. We need to know what to actually store.
3. Given a new file, we need to know how it relates with existing nodes and what
new nodes need to stored.

To make consistent some terminology.

Content Addressed Storage (CAS): A repository of hash->bytes

Chunks/Leaves: These are leaf nodes in the MerkleDAG and are produced when the Content
Defined Chunking procedure is run on a file.

Root: This are nodes in the MerkleDAG with no parent.

File Root: This are nodes in the MerkleDAG which correspond to actual user files.
*/
use super::error::*;
use super::merklenode::*;

/**
 * This defines the basic minimal database interface for a MerkleDB.
 *
 * The MerkleDB stores two classes of information:
 *  - MerkleNodes
 *  - MerkleNodeAttributes
 *
 * MerkleNodes are entirely immutable, and once created, cannot be modified.
 * On creation the MerkleNode is assigned a monotonic 64-bit ID.
 */
pub trait MerkleDBBase {
    ///
    /// Inserts a new node into the MerkleDB if a node with the hash does not
    /// already exists; returns the existing node from the DB otherwise.
    ///
    /// This implicitly creates an empty attribute for the node as well.
    /// That is, node_attribute(node.id) for a newly inserted node must succeed.
    ///
    /// Returns (node, new_node)
    /// new_node = true if this is a new node. and false if the node
    /// already exists.
    ///
    /// Note that changes may be commited until a flush() is issued.
    fn maybe_add_node(
        &mut self,
        hash: &MerkleHash,
        len: usize,
        children: Vec<(MerkleNodeId, usize)>,
    ) -> (MerkleNode, bool);

    ///
    /// Inserts a new node into the MerkleDB if a node with the hash does not
    /// already exists; returns the existing node from the DB otherwise.
    ///
    /// This implicitly creates an empty attribute for the node as well.
    /// That is, node_attribute(node.id) for a newly inserted node must succeed.
    ///
    /// Note that changes may be commited until a flush() is issued.
    fn add_node(&mut self, hash: &MerkleHash, len: usize, children: Vec<(MerkleNodeId, usize)>) -> MerkleNode {
        self.maybe_add_node(hash, len, children).0
    }

    /// Finds a node by its ID
    fn find_node_by_id(&self, h: MerkleNodeId) -> Option<MerkleNode>;

    /// Converts a hash to an ID
    fn hash_to_id(&self, h: &MerkleHash) -> Option<MerkleNodeId>;

    /// Finds a node by its hash
    fn find_node(&self, h: &MerkleHash) -> Option<MerkleNode>;

    /// Finds the node attributes for a given node ID
    fn node_attributes(&self, h: MerkleNodeId) -> Option<MerkleNodeAttributes>;

    /// Updates the node attributes for a given node ID
    /// Note that changes may be commited until a flush() is issued.
    fn set_node_attributes(&mut self, h: MerkleNodeId, attr: &MerkleNodeAttributes) -> Option<()>;

    /// Flushes all changes.
    fn flush(&mut self) -> Result<()>;

    /** Returns a monotonic ID which can be used to identify the current
     * time state of the database. This is essentially the last node ID
     * inserted.
     */
    fn get_sequence_number(&self) -> MerkleNodeId;

    /**
     * Database specific invariant checks which cannot be checked by graph
     * traversal.
     * Returns true if the invariants pass, and false otherwise.
     */
    fn db_invariant_checks(&self) -> bool;

    /// Sets if we autosync on drop. Defaults to true
    fn autosync_on_drop(&mut self, autosync: bool);
}
