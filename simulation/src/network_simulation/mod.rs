//! Network simulation: bandwidth limit proxy and Toxiproxy-backed profiles for CAS traffic.
//!
//! Total bandwidth is enforced by a custom proxy (bandwidth_limit_router) between
//! Toxiproxy and LocalTestServer. Latency, jitter, and congestion are applied via Toxiproxy.

mod bandwidth_limit_router;
mod error;
mod network_profile;
mod toxiproxy_router;

pub use bandwidth_limit_router::{BandwidthLimitProxyGuard, start_bandwidth_limit_proxy};
pub use error::{Result, SimulationError};
pub use network_profile::{NetworkProfile, SlicerParams};
pub use toxiproxy_router::{NetworkSimulationProxy, TOXIPROXY_ADDR_ENV, endpoint_to_host_port};
