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
