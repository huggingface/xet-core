#![cfg_attr(feature = "strict", deny(warnings))]
#![allow(dead_code)]

pub use chunk_cache::CacheConfig;
pub use http_client::{build_auth_http_client, build_http_client};
use interface::RegistrationClient;
pub use interface::{Client, ReconstructionClient, UploadClient};
pub use local_client::{tests_utils, LocalClient};
pub use remote_client::RemoteClient;

pub use crate::error::CasClientError;
pub use crate::http_shard_client::HttpShardClient;
pub use crate::interface::ShardClientInterface;
pub use crate::local_shard_client::LocalShardClient;

mod error;
mod http_client;
mod interface;
mod local_client;
mod remote_client;

mod global_dedup_table;
mod http_shard_client;
mod local_shard_client;
