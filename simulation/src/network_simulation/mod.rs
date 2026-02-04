//! Network simulation: Toxiproxy-backed profiles and proxy for CAS traffic.
//!
//! Route traffic through a Toxiproxy server with configurable bandwidth,
//! latency, jitter, and congestion (slicer) per upload/download direction.

mod error;
mod network_profile;
mod proxy_router;

pub use error::{Result, SimulationError};
pub use network_profile::{NetworkProfile, SlicerParams};
pub use proxy_router::{NetworkSimulationProxy, TOXIPROXY_ADDR_ENV};
