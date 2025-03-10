#![allow(dead_code)]

pub use chunk_cache::CacheConfig;
pub use http_client::{build_auth_http_client, build_http_client};
use interface::RegistrationClient;
pub use interface::{Client, ReconstructionClient, UploadClient};
pub use local_client::LocalClient;
pub use remote_client::RemoteClient;

pub use crate::error::CasClientError;
pub use crate::interface::ShardClientInterface;

mod error;
mod http_client;
mod interface;
mod local_client;
pub mod remote_client;
