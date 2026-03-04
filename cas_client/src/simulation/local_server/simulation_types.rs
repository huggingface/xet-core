use cas_types::CASReconstructionFetchInfo;
use merklehash::MerkleHash;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct XorbRangesRequest {
    pub ranges: Vec<(u32, u32)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct XorbRangesResponse {
    pub data: Vec<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileShardsEntry {
    pub file_hash: MerkleHash,
    pub shard_hash: MerkleHash,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfigDurationRequest {
    pub millis: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfigDelayRangeRequest {
    pub min_millis: Option<u64>,
    pub max_millis: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct XorbLengthResponse {
    pub length: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct XorbRawLengthResponse {
    pub length: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct XorbExistsResponse {
    pub exists: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileSizeResponse {
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchTermDataRequest {
    pub hash: MerkleHash,
    pub fetch_term: CASReconstructionFetchInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchTermDataResponse {
    pub data: Vec<u8>,
    pub chunk_byte_indices: Vec<u32>,
}
