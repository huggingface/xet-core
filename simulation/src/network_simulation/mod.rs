//! Network simulation: bandwidth-limit proxy with configurable bandwidth, latency, and drop.
//!
//! The proxy (bandwidth_limit_router) sits between clients and LocalTestServer.

mod bandwidth_limit_router;
mod endpoint;
mod error;
mod network_profile;

pub use bandwidth_limit_router::{
    BandwidthLimitProxyGuard, ProxyConfig, ProxySchedule, start_bandwidth_limit_proxy,
    start_bandwidth_limit_proxy_with_config, start_bandwidth_limit_proxy_with_schedule,
};
pub use endpoint::endpoint_to_host_port;
pub use error::{Result, SimulationError};
pub use network_profile::{NetworkProfile, SlicerParams};
