//! Network simulation: bandwidth-limit proxy with configurable bandwidth, latency, and drop.
//!
//! The proxy (bandwidth_limit_router) sits between clients and LocalTestServer.
//! Profiles are constructed only via NetworkProfileBuilder, which returns a NetworkProfileProvider.

mod bandwidth_limit_router;
mod network_profile;

pub use bandwidth_limit_router::NetworkSimulationProxy;
pub use network_profile::{NetworkConfig, NetworkProfile, NetworkProfileOptions};
