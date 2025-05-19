use std::cell::RefCell;
use std::convert::TryInto;
use std::fmt::Write;

use bitflags::bitflags;
pub use merklehash::MerkleHash;
use merklehash::*;
use serde::{Deserialize, Serialize};
/// A more compact representation of nodes in the MerkleTree.
/// The ID is a counter that begins at 1. 0 is not a valid ID.
pub type MerkleNodeId = u64;

/// Node parents can be unassigned in which case 0 is used to identify that case.
/// 0 is not a valid node ID.
pub const ID_UNASSIGNED: MerkleNodeId = 0;

/**
 * Nodes can have one or more sources of data.
 * Either from a FILE, CAS or both. This enumeration is used for several
 * NodeAttribute accessors.
 */
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum NodeDataType {
    CAS = 0,
    FILE = 1,
}
bitflags! {

    /**
     * Nodes can have one or more sources of data.
     * Either from a FILE, CAS or both. DataTypeBitfield provides
     * a bitfield type for use internally.
     */
    #[derive(Serialize, Deserialize, Copy, Clone, Default, Debug, PartialEq)]
    struct DataTypeBitfield: u8 {
        /// This node contains a Cas entry
        const CAS = 0x1_u8;
        /// This node contains a File entry
        const FILE = 0x2_u8;
    }
}

/************************************************************************* */
/*  */
/* MerkleNode */
/*  */
/************************************************************************* */

/**
 * Represents an immutable node in a MerkleDB. This node once constructed
 * and stored is completely immutable.
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MerkleNode {
    /// The Id of the current node.
    id: MerkleNodeId,
    /// The Merkle Hash of the current node. We don't serialize this
    hash: MerkleHash,
    /// The length of the bytes stored here
    len: usize,
    /// The IDs of the children
    children: Vec<(MerkleNodeId, usize)>,
}

impl Default for MerkleNode {
    fn default() -> MerkleNode {
        MerkleNode {
            id: ID_UNASSIGNED,
            hash: MerkleHash::default(),
            len: 0,
            children: Vec::new(),
        }
    }
}

impl MerkleNode {
    /// Gets the id of this node
    pub fn id(&self) -> MerkleNodeId {
        self.id
    }
    /// Gets the hash of this node
    pub fn hash(&self) -> &MerkleHash {
        &self.hash
    }
    /// Gets the length of data this node represents
    pub fn len(&self) -> usize {
        self.len
    }
    /// Whether the node contains empty data
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    /// Gets the list of IDs of the children and their lengths
    pub fn children(&self) -> &Vec<(MerkleNodeId, usize)> {
        &self.children
    }
    /// Updates the list of children.
    pub fn set_children(&mut self, ch: Vec<(MerkleNodeId, usize)>) {
        self.children = ch;
    }
    /// Constructs a new node
    pub fn new(id: MerkleNodeId, hash: MerkleHash, len: usize, children: Vec<(MerkleNodeId, usize)>) -> MerkleNode {
        MerkleNode {
            id,
            hash,
            len,
            children,
        }
    }
}

thread_local! {
static HASH_NODE_SEQUENCE_BUFFER: RefCell<String> =
    RefCell::new(String::with_capacity(1024));
}

/**
 * Hashing method used to derive a parent node from child node hashes.
 *
 * We just print out the string
 *
 * ```ignore
 * [child hash 1] : [child len 1]
 * [child hash 2] : [child len 2]
 * [child hash 3] : [child len 2]
 * ```
 *
 * With a "\n" after every line. And hash that.
 *
 * For performance, this function reuses an internal thread-local buffer.
 */
pub fn hash_node_sequence(hash: &[MerkleNode]) -> MerkleHash {
    HASH_NODE_SEQUENCE_BUFFER.with(|buffer| {
        let mut buf = buffer.borrow_mut();
        buf.clear();
        for node in hash.iter() {
            writeln!(buf, "{:x} : {}", node.hash(), node.len()).unwrap();
        }
        compute_internal_node_hash(buf.as_bytes())
    })
}

/************************************************************************* */
/*  */
/* MerkleNodeAttributes */
/*  */
/************************************************************************* */

/**
 * Each node may have additional attributes to identify where we can go
 * to find the value of a node. For instance, the node may by itself reference
 * an entry in CAS storage, or a File. This can be checked by inspecting the
 * attributes bitfield. Alternatively, it may be a substring / descendent of
 * a File, and the originating File can be found by following the parent[File]
 * link.
 */
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Default)]
pub struct MerkleNodeAttributes {
    /**
     * If parent[CAS] is set, the value of this node can be found by
     * traversing to parent[CAS]. parent[CAS] must have this node as a child.
     */
    parent: [MerkleNodeId; 2],
    /**
     * If attributes[CAS] is set, then the value of this node can be found
     * by querying CAS storage. Similarly, if attributes[FILE] is set
     * then there exists a file whose contents are exactly this node.
     */
    attributes: DataTypeBitfield,
}

impl MerkleNodeAttributes {
    /// Returns the parent of this node in the CAS graph.
    pub fn cas_parent(&self) -> MerkleNodeId {
        self.parent[NodeDataType::CAS as usize]
    }
    /// Returns the parent of this node in the FILE graph.
    pub fn file_parent(&self) -> MerkleNodeId {
        self.parent[NodeDataType::FILE as usize]
    }
    /** Sets parent of this node in the CAS graph.
     * Unchecked Invariant: parent must have this node as a child.
     */
    pub fn set_cas_parent(&mut self, parent: MerkleNodeId) {
        self.parent[NodeDataType::CAS as usize] = parent;
    }
    /** Sets parent of this node in the FILE graph.
     * Unchecked Invariant: parent must have this node as a child.
     */
    pub fn set_file_parent(&mut self, parent: MerkleNodeId) {
        self.parent[NodeDataType::FILE as usize] = parent;
    }
    /** Sets parent of this node in either FILE or CAS graph.
     * Unchecked Invariant: parent must have this node as a child.
     */
    pub fn set_parent(&mut self, parent_type: NodeDataType, parent: MerkleNodeId) {
        self.parent[parent_type as usize] = parent;
    }
    /**
     * Returns the parent of this node in either graph.
     */
    pub fn parent(&self, parent_type: NodeDataType) -> MerkleNodeId {
        self.parent[parent_type as usize]
    }
    /// Whether this node is a root of a file
    pub fn is_file(&self) -> bool {
        self.attributes.contains(DataTypeBitfield::FILE)
    }
    /// Whether this node points to a complete entry in Content Addressed Storage
    pub fn is_cas(&self) -> bool {
        self.attributes.contains(DataTypeBitfield::CAS)
    }
    /// Whether this node points to a complete entry in either FILE or CAS
    pub fn is_type(&self, data_type: NodeDataType) -> bool {
        if data_type == NodeDataType::CAS {
            self.is_cas()
        } else {
            self.is_file()
        }
    }

    /// Whether this node is a root of a file
    pub fn set_file(&mut self) {
        self.attributes.set(DataTypeBitfield::FILE, true);
    }
    /// Whether this node points to a complete entry in Content Addressed Storage
    pub fn set_cas(&mut self) {
        self.attributes.set(DataTypeBitfield::CAS, true);
    }

    /// Returns true if both self and other have the same attributes (FILE or CAS)
    pub fn type_equal(&self, other: &MerkleNodeAttributes) -> bool {
        self.attributes == other.attributes
    }
    /** Whether this node is a substring of a CAS entry.
     * This is a simple check: either this node is a CAS entry, or
     * it has a CAS parent.
     */
    pub fn has_cas_data(&self) -> bool {
        self.attributes.contains(DataTypeBitfield::CAS) || self.parent[NodeDataType::CAS as usize] != ID_UNASSIGNED
    }
    /** Whether this node is a substring of a FILE entry.
     * This is a simple check: either this node is a FILE entry, or
     * it has a CAS parent.
     */
    pub fn has_file_data(&self) -> bool {
        self.attributes.contains(DataTypeBitfield::FILE) || self.parent[NodeDataType::FILE as usize] != ID_UNASSIGNED
    }
    /**
     * Checks if this is a substring of either a CAS or FILE entry.
     */
    pub fn has_data(&self, data_type: NodeDataType) -> bool {
        if data_type == NodeDataType::CAS {
            self.has_cas_data()
        } else {
            self.has_file_data()
        }
    }
    pub fn merge_attributes(&mut self, other: &MerkleNodeAttributes) {
        self.attributes |= other.attributes;
    }
}
/************************************************************************* */
/*  */
/* ObjectRange */
/*  */
/************************************************************************* */

/**
 * Describes a range of bytes in an object.
 */
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ObjectRange {
    pub hash: MerkleHash,
    pub start: usize,
    pub end: usize,
}

/**
 * Describes a range of bytes in an object.
 */
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ObjectRangeById {
    pub id: MerkleNodeId,
    pub start: usize,
    pub end: usize,
}

/**
 * Given a collection of ranges, simplify the ranges by collapsing
 * consesutive ranges into one. For instance:
 *
 * ```ignore
 * [a, 0, 10], [a,10,20], [b,0, 15]
 * ```
 *
 * will be collapsed into
 *
 * ```ignore
 * [a, 0, 20], [b,0, 15]
 * ```
 */
pub fn simplify_ranges(ranges: &[ObjectRange]) -> Vec<ObjectRange> {
    let mut ret: Vec<ObjectRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if !ret.is_empty() {
            let last = ret.last_mut().unwrap();
            if last.hash == range.hash && last.end == range.start {
                last.end = range.end;
                continue;
            }
        }
        ret.push(range.clone());
    }
    ret
}

/************************************************************************* */
/*  */
/* Conversion to and from bytes for the RocksDB storage */
/*  */
/************************************************************************* */

pub trait RocksDBConversion<T> {
    // TODO: can we do better than Vec<u8> here? This incurs a small heap alloc
    // especially for NodeId and MerkleHash that should be unnecessary
    // Perhaps we can template around it?
    fn to_db_bytes(&self) -> Vec<u8>;
    fn from_db_bytes(bytes: &[u8]) -> T;
}
/**
 * Conversion routines to and from bytes
 */
impl RocksDBConversion<MerkleNode> for MerkleNode {
    fn to_db_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
    fn from_db_bytes(bytes: &[u8]) -> MerkleNode {
        bincode::deserialize(bytes).unwrap()
    }
}

/**
 * Conversion routines to and from bytes for MerkleNodeId.
 * We use Big Endian here so the lexicographic sort by RocksDB
 * gives the nodes in the right order
 */
impl RocksDBConversion<MerkleNodeId> for MerkleNodeId {
    fn to_db_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
    fn from_db_bytes(bytes: &[u8]) -> MerkleNodeId {
        MerkleNodeId::from_be_bytes(bytes.try_into().unwrap())
    }
}

impl RocksDBConversion<MerkleHash> for MerkleHash {
    fn to_db_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
    fn from_db_bytes(bytes: &[u8]) -> MerkleHash {
        MerkleHash::try_from(bytes).unwrap()
    }
}
