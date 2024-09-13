#![cfg_attr(feature = "strict", deny(warnings))]
#![allow(dead_code)]

pub use crate::error::CasClientError;
pub use caching_client::{CachingClient, DEFAULT_BLOCK_SIZE};
pub use interface::Client;
pub use local_client::LocalClient;
pub use merklehash::MerkleHash; // re-export since this is required for the client API.
pub use passthrough_staging_client::PassthroughStagingClient;
pub use remote_client::RemoteClient;
pub use staging_client::{new_staging_client, new_staging_client_with_progressbar, StagingClient};
pub use staging_trait::{Staging, StagingBypassable};

mod caching_client;
mod cas_connection_pool;
mod client_adapter;
mod data_transport;
mod error;
mod interface;
mod local_client;
mod passthrough_staging_client;
mod remote_client;
mod staging_client;
mod staging_trait;
mod util;
