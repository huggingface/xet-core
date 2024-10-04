#![cfg_attr(feature = "strict", deny(warnings))]
#![allow(dead_code)]

pub use crate::error::CasClientError;
pub use auth::AuthMiddleware;
pub use caching_client::CachingClient;
pub use interface::{Client, ReconstructionClient, UploadClient};
pub use local_client::tests_utils;
pub use local_client::LocalClient;
pub use remote_client::build_reqwest_client;
pub use remote_client::RemoteClient;

mod auth;
mod caching_client;
mod error;
mod interface;
mod local_client;
mod remote_client;
