#![cfg_attr(feature = "strict", deny(warnings))]
#![allow(dead_code)]

pub use crate::error::CasClientError;
pub use interface::{Client, ReconstructionClient, UploadClient};
pub use local_client::tests_utils;
pub use local_client::LocalClient;
pub use http_client::build_auth_http_client;
pub use http_client::build_http_client;
pub use remote_client::RemoteClient;

// mod auth;
mod error;
mod interface;
mod http_client;
mod local_client;
mod remote_client;

mod global_dedup_table;
mod http_shard_client;
mod local_shard_client;

use crate::error::Result;
use async_trait::async_trait;
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
    + FileReconstructor<CasClientError>
    + ShardDedupProber<CasClientError>
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