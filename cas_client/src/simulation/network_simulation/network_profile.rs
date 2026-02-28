//! Network profile for the bandwidth-limit proxy: bandwidth, latency, jitter, drop.
//!
//! Users construct profiles via [`NetworkProfileOptions`], which builds a
//! [`NetworkProfile`]. When no congestion is requested, the builder returns a [`ConsistentNetworkProfile`].
//! The proxy queries the provider every 50ms (see `REFILL_INTERVAL_MS`).

use std::sync::Arc;
use std::time::Duration;

use human_bandwidth::parse_bandwidth as parse_bandwidth_str;

use crate::error::{CasClientError, Result};

/// What the proxy applies: bandwidth (bytes/sec), latency, jitter, drop probability.
/// Only `bandwidth_bytes_per_sec` is optional (None = no limit); the rest default to zero.
#[derive(Clone, Debug)]
pub struct NetworkConfig {
    pub bandwidth_bytes_per_sec: Option<u64>,
    pub latency: Duration,
    pub jitter: Duration,
    pub drop_probability: f64,
}

/// Returns the network profile that should apply at a given elapsed time since the run started.
pub trait NetworkConfigProvider: Send + Sync {
    fn current_config(&self, elapsed: Duration) -> NetworkConfig;

    /// Latency at the given elapsed time (for reporting without exposing [`NetworkConfig`]).
    fn latency_at(&self, elapsed: Duration) -> Duration {
        self.current_config(elapsed).latency
    }
}

// What actually gets used everywhere.
pub type NetworkProfile = Arc<dyn NetworkConfigProvider>;

/// Builder for network profiles. Build a provider with:
/// `NetworkProfileOptions::new().with_bandwidth_str(...).with_latency(...).with_congestion(...).build()`.
#[derive(Clone)]
pub struct NetworkProfileOptions {
    max_bandwidth_bytes_per_sec: Option<u64>,
    base_latency: Duration,
    jitter: Duration,
    congestion: Option<CongestedNetworkConfig>,
    congestion_strength: f64,
}

/// Valid strings for [`NetworkProfileOptions::with_congestion`].
pub const VALID_CONGESTION_PROFILE_NAMES: &[&str] = &["none", "bursty", "light", "moderate", "heavy"];

/// Bursty congestion: alternating "on" (degraded) and "off" (normal) phases.
/// In degraded phase: latency = base * (1 + strength * latency_degradation_scale),
/// bandwidth = base * (strength * bandwidth_scale + (1 - strength)).
#[derive(Clone, Debug)]
pub struct CongestedNetworkConfig {
    pub time_on: Duration,
    pub time_off: Duration,
    /// Degraded latency = base * (1 + strength * latency_degradation_scale).
    pub latency_degradation_scale: f64,
    /// Degraded bandwidth = base * (strength * bandwidth_scale + (1 - strength)).
    pub bandwidth_scale: f64,
}

impl NetworkProfileOptions {
    pub fn new() -> Self {
        Self {
            max_bandwidth_bytes_per_sec: None,
            base_latency: Duration::ZERO,
            jitter: Duration::ZERO,
            congestion: None,
            congestion_strength: 1.0,
        }
    }

    /// Max bandwidth in bytes per second.
    pub fn with_bandwidth_bytes_per_sec(mut self, bytes_per_sec: u64) -> Self {
        self.max_bandwidth_bytes_per_sec = if bytes_per_sec == 0 { None } else { Some(bytes_per_sec) };
        self
    }

    /// Max bandwidth from a human-readable string (e.g. `"10mbps"`, `"100kbps"`). Case-insensitive.
    /// Stored as bytes per second internally.
    pub fn with_bandwidth_str(mut self, s: &str) -> Result<Self> {
        let bw = parse_bandwidth_str(s.trim())
            .map_err(|e| CasClientError::Other(format!("invalid bandwidth {:?}: {}", s, e)))?;
        let bps = bw.as_bps();
        let bps = u64::try_from(bps).unwrap_or(u64::MAX);
        if bps == 0 {
            return Err(CasClientError::Other(format!("invalid bandwidth: {:?}", s)));
        }
        self.max_bandwidth_bytes_per_sec = Some(bps / 8);
        Ok(self)
    }

    /// Base latency and jitter (both as `Duration`).
    pub fn with_latency(mut self, latency: Duration, jitter: Duration) -> Self {
        self.base_latency = latency;
        self.jitter = jitter;
        self
    }

    /// Congestion pattern by name: one of [`VALID_CONGESTION_PROFILE_NAMES`].
    pub fn with_congestion(mut self, name: &str) -> Result<Self> {
        let name = name.trim().to_lowercase();
        self.congestion = match name.as_str() {
            "none" => None,
            "bursty" => Some(CongestedNetworkConfig {
                time_on: Duration::from_secs(5),
                time_off: Duration::from_secs(10),
                latency_degradation_scale: 2.0,
                bandwidth_scale: 0.5,
            }),
            "light" => Some(CongestedNetworkConfig {
                time_on: Duration::from_secs(5),
                time_off: Duration::from_secs(20),
                latency_degradation_scale: 0.25,
                bandwidth_scale: 0.8,
            }),
            "moderate" => Some(CongestedNetworkConfig {
                time_on: Duration::from_secs(10),
                time_off: Duration::from_secs(20),
                latency_degradation_scale: 1.0,
                bandwidth_scale: 0.8,
            }),
            "heavy" => Some(CongestedNetworkConfig {
                time_on: Duration::from_secs(20),
                time_off: Duration::from_secs(20),
                latency_degradation_scale: 2.0,
                bandwidth_scale: 0.7,
            }),
            _ => {
                return Err(CasClientError::Other(format!(
                    "unknown congestion profile {:?}; valid: {:?}",
                    name, VALID_CONGESTION_PROFILE_NAMES
                )));
            },
        };
        Ok(self)
    }

    /// How much congestion applies: 0.0 = no effect, 1.0 = full effect.
    pub fn with_congestion_strength(mut self, strength: f64) -> Self {
        self.congestion_strength = strength.clamp(0.0, 1.0);
        self
    }

    /// Build a provider that returns the current profile at each elapsed time.
    /// When congestion is None, returns a [`ConsistentNetworkProfile`]. Otherwise returns a
    /// [`CongestedNetworkProfile`].
    pub fn build(self) -> NetworkProfile {
        let base = NetworkConfig {
            bandwidth_bytes_per_sec: self.max_bandwidth_bytes_per_sec,
            latency: self.base_latency,
            jitter: self.jitter,
            drop_probability: 0.0,
        };

        match self.congestion {
            None => Arc::new(ConsistentNetworkProfile { config: base }),
            Some(congestion) => Arc::new(CongestedNetworkProfile {
                base,
                congestion,
                congestion_strength: self.congestion_strength,
            }),
        }
    }
}

impl Default for NetworkProfileOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Provider that always returns the same config. Used when no congestion is set.
#[derive(Clone)]
pub struct ConsistentNetworkProfile {
    pub(super) config: NetworkConfig,
}

impl NetworkConfigProvider for ConsistentNetworkProfile {
    fn current_config(&self, _elapsed: Duration) -> NetworkConfig {
        self.config.clone()
    }
}

/// Provider that applies bursty congestion: alternating degraded and normal phases.
pub struct CongestedNetworkProfile {
    base: NetworkConfig,
    congestion: CongestedNetworkConfig,
    congestion_strength: f64,
}

impl NetworkConfigProvider for CongestedNetworkProfile {
    fn current_config(&self, elapsed: Duration) -> NetworkConfig {
        let c = &self.congestion;
        let cycle_secs = c.time_on.as_secs_f64() + c.time_off.as_secs_f64();
        if cycle_secs <= 0.0 {
            return self.base.clone();
        }
        let pos_secs = elapsed.as_secs_f64() % cycle_secs;
        let in_degraded = pos_secs < c.time_on.as_secs_f64();
        if !in_degraded {
            return self.base.clone();
        }

        let s = self.congestion_strength;
        let base_lat_ms = self.base.latency.as_secs_f64() * 1000.0;
        let latency_multiplier = 1.0 + s * c.latency_degradation_scale;
        let degraded_latency_ms = (base_lat_ms * latency_multiplier).round().max(0.0);
        let degraded_latency = Duration::from_secs_f64(degraded_latency_ms / 1000.0);

        let base_bw = self.base.bandwidth_bytes_per_sec.unwrap_or(0) as f64;
        let bandwidth_bytes_per_sec = (base_bw * (s * c.bandwidth_scale + (1.0 - s))).round() as u64;
        let bandwidth_bytes_per_sec = if bandwidth_bytes_per_sec > 0 {
            Some(bandwidth_bytes_per_sec)
        } else {
            None
        };

        NetworkConfig {
            bandwidth_bytes_per_sec,
            latency: degraded_latency,
            jitter: self.base.jitter,
            drop_probability: self.base.drop_probability,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_with_bandwidth_str() {
        let builder = NetworkProfileOptions::new().with_bandwidth_str("1mbps").unwrap();
        let prov = builder.build();
        let p = prov.current_config(Duration::ZERO);
        assert_eq!(p.bandwidth_bytes_per_sec, Some(125_000));
    }

    #[test]
    fn test_with_congestion() {
        let prov = NetworkProfileOptions::new()
            .with_latency(Duration::from_millis(50), Duration::from_millis(10))
            .with_bandwidth_bytes_per_sec(1_250_000)
            .with_congestion("heavy")
            .unwrap()
            .build();
        // Heavy: time_on 20s, time_off 20s. Query at 25s to get normal (non-degraded) phase.
        let p = prov.current_config(Duration::from_secs(25));
        assert_eq!(p.bandwidth_bytes_per_sec, Some(1_250_000));
        assert_eq!(p.latency, Duration::from_millis(50));
        assert_eq!(p.jitter, Duration::from_millis(10));
    }

    #[test]
    fn test_with_congestion_invalid() {
        assert!(NetworkProfileOptions::new().with_congestion("invalid").is_err());
    }
}
