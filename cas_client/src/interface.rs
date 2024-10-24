use std::io::Write;
use std::sync::Arc;

use async_trait::async_trait;
use cas_types::QueryReconstructionResponse;
use mdb_shard::shard_dedup_probe::ShardDedupProber;
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use merklehash::MerkleHash;
use reqwest_middleware::ClientWithMiddleware;

use crate::error::Result;
use crate::CasClientError;

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
    async fn exists(&self, prefix: &str, hash: &MerkleHash) -> Result<bool>;
}

/// A Client to the CAS (Content Addressed Storage) service to allow reconstructing a
/// pointer file based on FileID (MerkleHash).
///
/// To simplify this crate, it is intentional that the client does not create its own http_client or
/// spawn its own threads. Instead, it is expected to be given the parallism harness/threadpool/queue
/// on which it is expected to run. This allows the caller to better optimize overall system utilization
/// by controlling the number of concurrent requests.
#[async_trait]
pub trait ReconstructionClient {
    /// Get a entire file by file hash.
    ///
    /// The http_client passed in is a non-authenticated client. This is used to directly communicate
    /// with the backing store (S3) to retrieve xorbs.
    async fn get_file(
        &self,
        http_client: Arc<ClientWithMiddleware>,
        hash: &MerkleHash,
        writer: &mut Box<dyn Write + Send>,
    ) -> Result<()>;

    /// Get a entire file by file hash at a specific bytes range.
    ///
    /// The http_client passed in is a non-authenticated client. This is used to directly communicate
    /// with the backing store (S3) to retrieve xorbs.
    async fn get_file_byte_range(
        &self,
        http_client: Arc<ClientWithMiddleware>,
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
    async fn get_reconstruction(
        &self,
        hash: &MerkleHash,
        byte_range: Option<(u64, u64)>,
    ) -> Result<QueryReconstructionResponse>;
}

/// A Client to the Shard service. The shard service
/// provides for
/// 1. upload shard to the shard service
/// 2. querying of file->reconstruction information
/// 3. querying of chunk->shard information
pub trait ShardClientInterface:
    RegistrationClient + FileReconstructor<CasClientError> + ShardDedupProber<CasClientError> + Send + Sync
{
}

#[async_trait]
pub trait RegistrationClient {
    async fn upload_shard(
        &self,
        prefix: &str,
        hash: &MerkleHash,
        force_sync: bool,
        shard_data: &[u8],
        salt: &[u8; 32],
    ) -> Result<bool>;
}
