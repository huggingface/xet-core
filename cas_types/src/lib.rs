use serde_repr::{Deserialize_repr, Serialize_repr};

use merklehash::MerkleHash;
use serde::{Deserialize, Serialize};

mod key;
pub use key::*;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UploadXorbResponse {
    pub was_inserted: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Range {
    pub start: u32,
    pub end: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CASReconstructionTerm {
    pub hash: HexMerkleHash,
    pub unpacked_length: u32,
    pub range: Range,
    pub range_start_offset: u32,
    pub url: String,
    pub url_range: Range,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryReconstructionResponse {
    pub reconstruction: Vec<CASReconstructionTerm>,
}

#[derive(Debug, Serialize_repr, Deserialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum UploadShardResponseType {
    Exists = 0,
    SyncPerformed = 1,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UploadShardResponse {
    pub result: UploadShardResponseType,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryChunkResponse {
    pub shard: MerkleHash,
}

pub type Salt = [u8; 32];
