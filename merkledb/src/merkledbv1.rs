use crate::merklememdb::MerkleMemDB;

use crate::merkledb_debug::*;
use crate::merkledb_highlevel_v1::*;
use crate::merkledb_ingestion_v1::*;
use crate::merkledbbase::MerkleDBBase;

pub trait MerkleDBV1:
    MerkleDBBase + MerkleDBHighLevelMethodsV1 + MerkleDBIngestionMethodsV1 + MerkleDBDebugMethods
{
}

impl MerkleDBHighLevelMethodsV1 for MerkleMemDB {}
impl MerkleDBIngestionMethodsV1 for MerkleMemDB {}
impl MerkleDBV1 for MerkleMemDB {}
