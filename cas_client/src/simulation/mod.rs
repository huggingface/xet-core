//! Simulation module for local testing and development.
//!
//! This module provides clients and servers for testing CAS operations
//! without requiring a remote server:
//!
//! - [`LocalClient`]: A disk-backed client for testing
//! - [`MemoryClient`]: An in-memory client for testing
//! - [`local_server`]: An HTTP server wrapping a DirectAccessClient for integration testing
//! - [`DirectAccessClient`]: Trait for clients with direct XORB/file access
//! - [`client_testing_utils`]: Testing utilities for Client implementations
//! - [`client_unit_testing`]: Common unit tests for Client implementations

pub mod client_testing_utils;
#[cfg(test)]
pub mod client_unit_testing;
mod direct_access_client;
mod local_client;
pub mod local_server;
mod memory_client;

pub use client_testing_utils::{ClientTestingUtils, RandomFileContents};
pub use direct_access_client::DirectAccessClient;
pub use local_client::LocalClient;
pub use local_server::{LocalServer, LocalServerConfig, LocalTestServer};
pub use memory_client::MemoryClient;
