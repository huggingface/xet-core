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
//! - [`network_simulation`]: Bandwidth-limit proxy and network profiles (not available in WASM)

// --- Always-available lightweight types ---
pub mod client_testing_utils;
mod direct_access_client;
mod memory_client;
mod random_xorb;
pub(crate) mod xorb_utils;

pub use client_testing_utils::{ClientTestingUtils, RandomFileContents};
pub use direct_access_client::DirectAccessClient;
pub use memory_client::MemoryClient;
pub use random_xorb::RandomXorb;

// --- Non-WASM types that don't need heavy deps ---
#[cfg(not(target_family = "wasm"))]
mod deletion_controls;
#[cfg(not(target_family = "wasm"))]
mod local_client;

#[cfg(not(target_family = "wasm"))]
pub use deletion_controls::DeletionControlableClient;
#[cfg(not(target_family = "wasm"))]
pub use local_client::LocalClient;

// --- Test-only modules (always available for non-WASM) ---
#[cfg(all(test, not(target_family = "wasm")))]
pub mod client_unit_testing;
#[cfg(all(test, not(target_family = "wasm")))]
pub mod deletion_unit_testing;

// --- Heavy deps: require "simulation" feature (axum, tower-http, etc.) ---
#[cfg(all(feature = "simulation", not(target_family = "wasm")))]
pub mod local_server;
#[cfg(all(feature = "simulation", not(target_family = "wasm")))]
pub mod network_simulation;
#[cfg(all(feature = "simulation", not(target_family = "wasm")))]
mod simulation_client;
#[cfg(all(feature = "simulation", not(target_family = "wasm")))]
mod simulation_server;
#[cfg(all(feature = "simulation", unix, not(target_family = "wasm")))]
pub mod socket_proxy;

#[cfg(all(feature = "simulation", not(target_family = "wasm")))]
pub use local_server::{LocalServer, LocalServerConfig, SimulationControlClient};
#[cfg(all(feature = "simulation", not(target_family = "wasm")))]
pub use network_simulation::{NetworkConfig, NetworkProfile, NetworkProfileOptions, NetworkSimulationProxy};
#[cfg(all(feature = "simulation", not(target_family = "wasm")))]
pub use simulation_client::RemoteSimulationClient;
#[cfg(all(feature = "simulation", not(target_family = "wasm")))]
pub use simulation_server::{LocalTestServer, LocalTestServerBuilder};
#[cfg(all(feature = "simulation", unix, not(target_family = "wasm")))]
pub use socket_proxy::UnixSocketProxy;
