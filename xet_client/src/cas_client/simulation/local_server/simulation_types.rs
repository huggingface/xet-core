use serde::{Deserialize, Serialize};
use xet_core_structures::merklehash::MerkleHash;

use crate::cas_client::simulation::deletion_controls::{ObjectETag, ObjectTagSet};
use crate::cas_types::XorbReconstructionFetchInfo;

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
    pub fetch_term: XorbReconstructionFetchInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchTermDataResponse {
    pub data: Vec<u8>,
    pub chunk_byte_indices: Vec<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HashWithETag {
    pub hash: MerkleHash,
    pub etag: ObjectETag,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ETagDeleteRequest {
    pub etag: ObjectETag,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ETagDeleteResponse {
    pub deleted: bool,
}

/// Body of a tag-set read or write. Ordered pairs rather than a map so the
/// wire form matches S3's tag set, where key order is preserved.
#[derive(Debug, Serialize, Deserialize)]
pub struct TagSetBody {
    pub tags: ObjectTagSet,
}
