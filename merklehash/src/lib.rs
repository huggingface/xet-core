//! The merklehash module provides common and convenient operations
//! around the [DataHash] (aliased to [MerkleHash]).
//!
//! The [MerkleHash] is internally a 256-bit value stored as 4 u64 and is
//! a node in a MerkleTree. A MerkleTree is a hierarchical datastructure
//! where the leaves are hashes of data (for instance, blocks in a file).
//! Then the hash of each non-leaf node is derived from the hashes of its child
//! nodes.
//!
//! A default constructor is provided to make the hash of 0s.
//! ```ignore
//! // creates a default hash value of all 0s
//! let hash = MerkleHash::default();
//! ```
//!
//! Two hash functions are provided to compute a MerkleHash from a slice of
//! bytes. The first is [compute_data_hash] which should be used when computing
//! a hash from any user-provided sequence of bytes (i.e. the leaf nodes)
//! ```ignore
//! // compute from a byte slice of &[u8]
//! let string = "hello world";
//! let hash = compute_data_hash(slice.as_bytes());
//! ```
//!
//! The second is [compute_internal_node_hash] should be used when computing
//! the hash of interior nodes. Note that this method also just accepts a slice
//! of `&[u8]` and it is up to the caller to format the string appropriately.
//! For instance: the string could be simply the child hashes printed out
//! consecutively.
//!
//! The reason why this method does not simply take an array of Hashes, and
//! instead require the caller to format the input as a string is to allow the
//! user to add additional information to the string being hashed (beyond just
//! the hashes itself). i.e. the string being hashed could be a concatenation
//! of "hashes of children + children metadata".
//! ```ignore
//! let hash = compute_internal_node_hash(slice.as_bytes());
//! ```
//!
//! The two hash functions [compute_data_hash] and [compute_internal_node_hash]
//! are keyed differently such the same inputs will produce different outputs.
//! And in particular, it should be difficult to find a collision where
//! a `compute_data_hash(a) == compute_internal_node_hash(b)`

#![cfg_attr(feature = "strict", deny(warnings))]

pub mod data_hash;
pub use data_hash::*;
pub type MerkleHash = DataHash;
