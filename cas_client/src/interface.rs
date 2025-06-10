#[cfg(not(target_family = "wasm"))]
use mdb_shard::shard_file_reconstructor::FileReconstructor;

#[cfg(not(target_family = "wasm"))]
use crate::CasClientError;

/// A Client to the Shard service. The shard service
/// provides for
/// 1. upload shard to the shard service
/// 2. querying of file->reconstruction information
/// 3. querying of chunk->shard information
#[cfg(not(target_family = "wasm"))]
pub trait ShardClientInterface:
    RegistrationClient + FileReconstructor<CasClientError> + ShardDedupProber + Send + Sync
{
}

#[cfg(target_family = "wasm")]
pub trait ShardClientInterface: RegistrationClient + ShardDedupProber {}

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
    use progress_tracking::item_tracking::SingleItemProgressUpdater;

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
            progress_updater: Option<Arc<SingleItemProgressUpdater>>,
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
    use std::sync::Arc;

    use cas_object::SerializedCasObject;
    use merklehash::MerkleHash;
    use progress_tracking::upload_tracking::CompletionTracker;

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
    pub trait ShardDedupProber {
        #[cfg(not(target_family = "wasm"))]
        async fn query_for_global_dedup_shard(
            &self,
            prefix: &str,
            chunk_hash: &MerkleHash,
            salt: &[u8; 32],
        ) -> Result<Option<std::path::PathBuf>>;

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
        /// Insert a serialized XORB into the CAS, returning the number of bytes read.
        async fn upload_xorb(
            &self,
            prefix: &str,
            serialized_cas_object: SerializedCasObject,
            upload_tracker: Option<Arc<CompletionTracker>>,
        ) -> Result<u64>;

        /// Check if a XORB already exists.
        async fn exists(&self, prefix: &str, hash: &MerkleHash) -> Result<bool>;

        /// Indicates if the serialized cas object should have a written footer.
        /// This should only be true for testing with LocalClient.
        fn use_xorb_footer(&self) -> bool;
    }
}

#[cfg(not(target_family = "wasm"))]
pub use download::*;
pub use upload::*;
