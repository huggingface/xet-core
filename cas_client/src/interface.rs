use crate::error::Result;
use async_trait::async_trait;
use cas_types::QueryReconstructionResponse;
use chunk_cache::DiskCache;
use merklehash::MerkleHash;
use reqwest_middleware::ClientWithMiddleware;
use std::io::Write;

/// A Client to the CAS (Content Addressed Storage) service to allow storage and
/// management of XORBs (Xet Object Remote Block). A XORB represents a collection
/// of arbitrary bytes. These bytes are hashed according to a Xet Merkle Hash
/// producing a Merkle Tree. XORBs in the CAS are identified by a combination of
/// a prefix namespacing the XORB and the hash at the root of the Merkle Tree.
#[async_trait]
pub trait UploadClient {
    /// Insert the provided data into the CAS as a XORB indicated by the prefix and hash.
    /// The hash will be verified on the SERVER-side according to the chunk boundaries.
    /// Chunk Boundaries must be complete; i.e. the last entry in chunk boundary
    /// must be the length of data. For instance, if data="helloworld" with 2 chunks
    /// ["hello" "world"], chunk_boundaries should be [5, 10].
    /// Empty data and empty chunk boundaries are not accepted.
    ///
    /// Note that put may background in some implementations and a flush()
    /// will be needed.
    async fn put(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        data: Vec<u8>,
        chunk_and_boundaries: Vec<(MerkleHash, u32)>,
    ) -> Result<()>;

    /// Check if a XORB already exists.
    async fn exists(
        &self,
        prefix: &str,
        hash: &MerkleHash,
    ) -> Result<bool>;
}

/// A Client to the CAS (Content Addressed Storage) service to allow reconstructing a
/// pointer file based on FileID (MerkleHash).
#[async_trait]
pub trait ReconstructionClient {
    /// Get a entire file by file hash.
    async fn get_file(
        &self,
        http_client: &ClientWithMiddleware,
        disk_cache: Option<DiskCache>,
        hash: &MerkleHash,
        writer: &mut Box<dyn Write + Send>,
    ) -> Result<()>;

    /// Get a entire file by file hash at a specific bytes range.
    async fn get_file_byte_range(
        &self,
        http_client: &ClientWithMiddleware,
        hash: &MerkleHash,
        offset: u64,
        length: u64,
        writer: &mut Box<dyn Write + Send>,
    ) -> Result<()>;
}

pub trait Client: UploadClient + ReconstructionClient {}

/// A Client to the CAS (Content Addressed Storage) service that is able to obtain
/// the reconstruction info of a file by FileID (MerkleHash).
/// This trait is meant for internal (caching): external users to this crate don't
/// access these trait functions.
#[async_trait]
pub(crate) trait Reconstructable {
    async fn reconstruct(
        &self,
        hash: &MerkleHash,
        byte_range: Option<(u64, u64)>,
    ) -> Result<QueryReconstructionResponse>;
}
