use crate::merkledb_debug::*;
use crate::merkledb_highlevel_v2::*;
use crate::merkledbbase::MerkleDBBase;
use crate::merklememdb::MerkleMemDB;

pub trait MerkleDBV2: MerkleDBBase + MerkleDBHighLevelMethodsV2 + MerkleDBDebugMethods {}

impl MerkleDBHighLevelMethodsV2 for MerkleMemDB {}
