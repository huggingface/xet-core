// #![allow(
//     unknown_lints,
//     renamed_and_removed_lints,
//     clippy::blocks_in_conditions,
//     clippy::blocks_in_if_conditions
// )]
pub mod error;
mod global_dedup_table;
mod http_shard_client;
mod local_shard_client;
//mod shard_client;

use crate::error::Result;
use async_trait::async_trait;
use error::ShardClientError;
pub use http_shard_client::HttpShardClient;
pub use local_shard_client::LocalShardClient;
use mdb_shard::shard_dedup_probe::ShardDedupProber;
use mdb_shard::shard_file_reconstructor::FileReconstructor;
use merklehash::MerkleHash;

/// A Client to the Shard service. The shard service
/// provides for
/// 1. upload shard to the shard service
/// 2. querying of file->reconstruction information
/// 3. querying of chunk->shard information
pub trait ShardClientInterface:
    RegistrationClient
    + FileReconstructor<ShardClientError>
    + ShardDedupProber<ShardClientError>
    + Send
    + Sync
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
