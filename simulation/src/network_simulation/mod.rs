//! Network simulation: bandwidth-limit proxy with configurable bandwidth, latency, and drop.
//!
//! The proxy (bandwidth_limit_router) sits between clients and LocalTestServer.

mod bandwidth_limit_router;
mod delay_profile_provider;
mod endpoint;
mod error;
mod network_profile;
mod profile_provider;

pub use bandwidth_limit_router::{
    BandwidthLimitProxyGuard, BandwidthRecording, ProxyConfig, start_bandwidth_limit_proxy,
    start_bandwidth_limit_proxy_with_config, start_bandwidth_limit_proxy_with_provider,
};
pub use delay_profile_provider::{DelayProfileProvider, FixedDelayProfileProvider};
pub use endpoint::endpoint_to_host_port;
pub use error::{Result, SimulationError};
pub use network_profile::{NetworkProfile, SlicerParams};
pub use profile_provider::{CyclingProfileProvider, NetworkProfileProvider};
