//! Simulation module for local testing and development.
//!
//! This module provides clients and servers for testing CAS operations
//! without requiring a remote server:
//!
//! - [`LocalClient`]: A disk-backed client for testing (not available in WASM)
//! - [`MemoryClient`]: An in-memory client for testing
//! - [`local_server`]: An HTTP server wrapping a DirectAccessClient for integration testing (not available in WASM)
//! - [`DirectAccessClient`]: Trait for clients with direct XORB/file access
//! - [`client_testing_utils`]: Testing utilities for Client implementations (not available in WASM)
//! - [`client_unit_testing`]: Common unit tests for Client implementations (not available in WASM)

pub mod client_testing_utils;
#[cfg(all(test, not(target_family = "wasm")))]
pub mod client_unit_testing;
mod direct_access_client;
#[cfg(not(target_family = "wasm"))]
mod local_client;
#[cfg(not(target_family = "wasm"))]
pub mod local_server;
mod memory_client;
mod random_xorb;
#[cfg(unix)]
#[cfg(not(target_family = "wasm"))]
pub mod socket_proxy;
pub(crate) mod xorb_utils;

pub use client_testing_utils::{ClientTestingUtils, RandomFileContents};
pub use direct_access_client::DirectAccessClient;
#[cfg(not(target_family = "wasm"))]
pub use local_client::LocalClient;
#[cfg(not(target_family = "wasm"))]
pub use local_server::{LocalServer, LocalServerConfig, LocalTestServer};
pub use memory_client::MemoryClient;
pub use random_xorb::RandomXorb;
#[cfg(unix)]
#[cfg(not(target_family = "wasm"))]
pub use socket_proxy::UnixSocketProxy;
