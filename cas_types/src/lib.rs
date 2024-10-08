use merklehash::MerkleHash;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

mod error;
mod key;
pub use key::*;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UploadXorbResponse {
    pub was_inserted: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, PartialOrd, Ord, Default)]
pub struct Range {
    pub start: u32,
    pub end: u32,
}

impl std::fmt::Display for Range {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Range { start, end } = self;
        write!(f, "{start}-{end}")
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CASReconstructionTerm {
    pub hash: HexMerkleHash,
    pub unpacked_length: u32,
    // chunk index start and end in a xorb
    pub range: Range,
    pub url: String,
    // byte index start and end in a xorb
    pub url_range: Range,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryReconstructionResponse {
    // For range query [a, b) into a file content, the location
    // of "a" into the first range.
    pub offset_into_first_range: u32,
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
