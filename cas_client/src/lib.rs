#![cfg_attr(feature = "strict", deny(warnings))]
#![allow(dead_code)]

pub use crate::error::CasClientError;
pub use caching_client::CachingClient;
pub use interface::Client;
pub use local_client::LocalClient;
pub use remote_client::RemoteClient;

mod caching_client;
mod error;
mod interface;
mod local_client;
mod remote_client;
