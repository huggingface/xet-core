use mdb_shard::shard_file_reconstructor::FileReconstructor;

use crate::CasClientError;

/// A Client to the Shard service. The shard service
/// provides for
/// 1. upload shard to the shard service
/// 2. querying of file->reconstruction information
/// 3. querying of chunk->shard information
#[cfg(not(target_family = "wasm"))]
pub trait ShardClientInterface:
    RegistrationClient + FileReconstructor<CasClientError> + ShardDedupProbe + Send + Sync
{
}

#[cfg(target_family = "wasm")]
pub trait ShardClientInterface: RegistrationClient + ShardDedupProbe {}

#[cfg(not(target_family = "wasm"))]
pub trait Client: UploadClient + ReconstructionClient + ShardClientInterface {}
#[cfg(target_family = "wasm")]
pub trait Client: UploadClient + ShardClientInterface {}

#[cfg(not(target_family = "wasm"))]
mod download {
    use std::collections::HashMap;
    use std::sync::Arc;

    use cas_types::{FileRange, QueryReconstructionResponse};
    use merklehash::MerkleHash;
    use utils::progress::ProgressUpdater;

    use crate::error::Result;
    use crate::output_provider::OutputProvider;

    /// A Client to the CAS (Content Addressed Storage) service to allow reconstructing a
    /// pointer file based on FileID (MerkleHash).
    ///
    /// To simplify this crate, it is intentional that the client does not create its own http_client or
    /// spawn its own threads. Instead, it is expected to be given the parallelism harness/threadpool/queue
    /// on which it is expected to run. This allows the caller to better optimize overall system utilization
    /// by controlling the number of concurrent requests.
    #[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
    #[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
    pub trait ReconstructionClient {
        /// Get an entire file by file hash with an optional bytes range.
        ///
        /// The http_client passed in is a non-authenticated client. This is used to directly communicate
        /// with the backing store (S3) to retrieve xorbs.
        async fn get_file(
            &self,
            hash: &MerkleHash,
            byte_range: Option<FileRange>,
            output_provider: &OutputProvider,
            progress_updater: Option<Arc<dyn ProgressUpdater>>,
        ) -> Result<u64>;

        async fn batch_get_file(&self, files: HashMap<MerkleHash, &OutputProvider>) -> Result<u64> {
            let mut n_bytes = 0;
            // Provide the basic naive implementation as a default.
            for (h, w) in files {
                n_bytes += self.get_file(&h, None, w, None).await?;
            }
            Ok(n_bytes)
        }
    }

    /// A Client to the CAS (Content Addressed Storage) service that is able to obtain
    /// the reconstruction info of a file by FileID (MerkleHash). Return
    /// - Ok(Some(response)) if the query succeeded,
    /// - Ok(None) if the specified range can't be satisfied,
    /// - Err(e) for other errors.
    #[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
    #[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
    pub trait Reconstruct {
        async fn get_reconstruction(
            &self,
            hash: &MerkleHash,
            byte_range: Option<FileRange>,
        ) -> Result<Option<QueryReconstructionResponse>>;
    }
}

mod upload {
    use merklehash::MerkleHash;

    use crate::error::Result;

    #[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
    #[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
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

    /// Probes for shards that provide dedup information for a chunk, and, if
    /// any are found, writes them to disk and returns the path.
    #[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
    #[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
    pub trait ShardDedupProbe {
        #[cfg(not(target_family = "wasm"))]
        async fn query_for_global_dedup_shard(
            &self,
            prefix: &str,
            chunk_hash: &MerkleHash,
            salt: &[u8; 32],
        ) -> Result<Option<std::path::PathBuf>>;
        // #[cfg(target_family = "wasm")]
        async fn query_for_global_dedup_shard_in_memory(
            &self,
            prefix: &str,
            chunk_hash: &MerkleHash,
            salt: &[u8; 32],
        ) -> Result<Option<Vec<u8>>>;
    }

    /// A Client to the CAS (Content Addressed Storage) service to allow storage and
    /// management of XORBs (Xet Object Remote Block). A XORB represents a collection
    /// of arbitrary bytes. These bytes are hashed according to a Xet Merkle Hash
    /// producing a Merkle Tree. XORBs in the CAS are identified by a combination of
    /// a prefix namespacing the XORB and the hash at the root of the Merkle Tree.
    #[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
    #[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
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
        ) -> Result<usize>;

        /// Check if a XORB already exists.
        async fn exists(&self, prefix: &str, hash: &MerkleHash) -> Result<bool>;
    }
}

#[cfg(not(target_family = "wasm"))]
pub use download::*;
pub use upload::*;
