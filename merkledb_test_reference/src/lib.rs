#![cfg_attr(feature = "strict", deny(warnings))]
#![allow(dead_code)]

pub mod aggregate_hashes;
mod async_chunk_iterator;
mod chunk_iterator;
pub mod constants;
pub mod error;
mod internal_methods;
mod merkledb_debug;
mod merkledb_highlevel_v1;
mod merkledb_highlevel_v2;
mod merkledb_ingestion_v1;
mod merkledb_reconstruction;
mod merkledbbase;
mod merkledbv1;
mod merkledbv2;
mod merklememdb;
mod merklenode;
mod tests;

pub use chunk_iterator::{chunk_target, low_variance_chunk_target, ChunkInfo};
pub use merkledbv1::MerkleDBV1;
pub use merkledbv2::MerkleDBV2;
pub use merklememdb::MerkleMemDB;
pub use merklenode::{MerkleNode, MerkleNodeAttributes, MerkleNodeId, NodeDataType, ObjectRange};

pub use crate::merkledb_highlevel_v1::InsertionStaging;
pub mod prelude {
    pub use crate::merkledb_debug::MerkleDBDebugMethods;
    pub use crate::merkledb_highlevel_v1::MerkleDBHighLevelMethodsV1;
    pub use crate::merkledb_ingestion_v1::MerkleDBIngestionMethodsV1;
    pub use crate::merkledb_reconstruction::MerkleDBReconstruction;
    pub use crate::merkledbbase::MerkleDBBase;
    pub use crate::merkledbv1::MerkleDBV1;
}

pub mod prelude_v2 {
    pub use crate::merkledb_debug::MerkleDBDebugMethods;
    pub use crate::merkledb_highlevel_v2::MerkleDBHighLevelMethodsV2;
    pub use crate::merkledb_reconstruction::MerkleDBReconstruction;
    pub use crate::merkledbbase::MerkleDBBase;
}
pub mod detail {
    pub use crate::merklenode::hash_node_sequence;
}
