//! Server Latency Simulation for Local CAS Server
//!
//! This module provides configurable latency simulation (delays and congestion) for the local server.
//! It tracks active connections and applies delays or rejects requests based on the configured profile.
//! The profile can be updated dynamically via the HTTP API.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use rand::Rng;
use rand::rngs::ThreadRng;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Configuration profile for server latency simulation (delays and congestion).
///
/// All fields are optional, allowing partial updates via HTTP.
/// When a profile is uploaded, only non-None fields overwrite the existing profile.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServerLatencyProfile {
    /// Random delay range for all API calls (milliseconds).
    /// Format: (min_ms, max_ms). Applied before processing each request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub random_delay_ms: Option<(u64, u64)>,

    /// Number of concurrent connections above which degradation kicks in.
    /// If None, connection-based degradation is disabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_degradation_threshold: Option<u64>,

    /// Congestion penalty delay range (min_ms, max_ms).
    /// Applied when active connections exceed the degradation threshold.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub congestion_penalty_ms: Option<(u64, u64)>,

    /// Probability of returning 503 when degraded (0.0-1.0).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub congestion_error_rate: Option<f64>,
}

impl ServerLatencyProfile {
    pub const VALID_NAMES: &[&str] = &["none", "light", "realistic", "heavy"];

    /// Construct a profile by name. Returns `None` if the name is not recognized.
    /// Valid names: "none", "light", "realistic", "heavy".
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "none" => Some(Self::none()),
            "light" => Some(Self::light()),
            "realistic" => Some(Self::realistic()),
            "heavy" => Some(Self::heavy()),
            _ => None,
        }
    }

    pub fn none() -> Self {
        Self::default()
    }

    /// Realistic API delay: 400–1000 ms random delay.
    pub fn realistic() -> Self {
        Self {
            random_delay_ms: Some((400, 1000)),
            congestion_penalty_ms: Some((1000, 4000)),
            congestion_error_rate: Some(0.05),
            ..Default::default()
        }
    }

    /// Light delay: 100–250 ms random delay.
    pub fn light() -> Self {
        Self {
            random_delay_ms: Some((100, 250)),
            congestion_penalty_ms: Some((1000, 4000)),
            congestion_error_rate: Some(0.05),
            ..Default::default()
        }
    }

    /// Heavy delay: 600–1200 ms random delay with aggressive congestion penalties.
    pub fn heavy() -> Self {
        Self {
            random_delay_ms: Some((600, 1200)),
            congestion_penalty_ms: Some((1000, 10000)),
            congestion_error_rate: Some(0.15),
            ..Default::default()
        }
    }

    /// Enables connection-based degradation: when active connections exceed `threshold`,
    /// the configured congestion penalty and error rate take effect.
    pub fn with_connection_degradation_threshold(mut self, threshold: u64) -> Self {
        self.connection_degradation_threshold = Some(threshold);
        self
    }
}

/// Guard that tracks an active connection and decrements the connection count on drop.
pub struct ConnectionGuard {
    state: Arc<LatencySimulation>,
    should_simulate_error: bool,
}

impl ConnectionGuard {
    /// Returns an error response if latency simulation indicates an error should be simulated.
    ///
    /// Returns `Some(Response)` with INTERNAL_SERVER_ERROR if the connection should be rejected,
    /// or `None` if the request should proceed normally.
    pub fn simulate_error(&self) -> Option<Response> {
        if self.should_simulate_error {
            Some((StatusCode::INTERNAL_SERVER_ERROR, "Server error").into_response())
        } else {
            None
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.state.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Manages latency simulation state and profile.
pub(super) struct LatencySimulation {
    profile: RwLock<ServerLatencyProfile>,
    active_connections: AtomicU64,
}

impl LatencySimulation {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            profile: RwLock::new(ServerLatencyProfile::default()),
            active_connections: AtomicU64::new(0),
        })
    }

    /// Update the profile, merging with existing values.
    ///
    /// Only non-None fields in `update` overwrite the existing profile.
    pub async fn update_profile(&self, update: ServerLatencyProfile) {
        let mut profile = self.profile.write().await;
        if let Some(delay) = update.random_delay_ms {
            profile.random_delay_ms = Some(delay);
        }
        if let Some(threshold) = update.connection_degradation_threshold {
            profile.connection_degradation_threshold = Some(threshold);
        }
        if let Some(penalty) = update.congestion_penalty_ms {
            profile.congestion_penalty_ms = Some(penalty);
        }
        if let Some(rate) = update.congestion_error_rate {
            profile.congestion_error_rate = Some(rate);
        }
    }

    /// Register a connection, applying latency simulation.
    ///
    /// This increments the active connection count, applies any configured delays,
    /// and returns a guard that must be kept alive for the duration of the request.
    ///
    /// The guard automatically decrements the connection count when dropped.
    /// Use `guard.simulate_error()` to check if an error should be returned.
    pub async fn register_connection(self: &Arc<Self>) -> ConnectionGuard {
        let count = self.active_connections.fetch_add(1, Ordering::Relaxed) + 1;
        let profile = self.profile.read().await.clone();

        // Apply random delay if configured
        let random_delay = profile.random_delay_ms.map(|(min_ms, max_ms)| {
            Duration::from_millis(ThreadRng::default().random_range(min_ms..=(max_ms.max(min_ms))))
        });

        // Check degradation
        let should_simulate_error = if let Some(threshold) = profile.connection_degradation_threshold
            && count > threshold
        {
            if let Some(error_rate) = profile.congestion_error_rate
                && ThreadRng::default().random_range(0.0..1.0) < error_rate
            {
                true
            } else {
                // Apply congestion penalty delay
                if let Some((min_ms, max_ms)) = profile.congestion_penalty_ms {
                    let penalty_ms = if min_ms == max_ms {
                        min_ms
                    } else {
                        ThreadRng::default().random_range(min_ms..=max_ms)
                    };
                    let penalty_delay = Duration::from_millis(penalty_ms);
                    let total_delay = random_delay.map(|d| d + penalty_delay).unwrap_or(penalty_delay);
                    tokio::time::sleep(total_delay).await;
                } else if let Some(delay) = random_delay {
                    tokio::time::sleep(delay).await;
                }
                false
            }
        } else {
            if let Some(delay) = random_delay {
                tokio::time::sleep(delay).await;
            }
            false
        };

        ConnectionGuard {
            state: Arc::clone(self),
            should_simulate_error,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn profile_from_name_valid_and_invalid() {
        for name in ServerLatencyProfile::VALID_NAMES {
            assert!(ServerLatencyProfile::from_name(name).is_some());
        }
        assert!(ServerLatencyProfile::from_name("nonexistent").is_none());
    }

    #[tokio::test]
    async fn no_profile_no_delay_no_error() {
        let sim = LatencySimulation::new();
        let guard = sim.register_connection().await;
        assert!(guard.simulate_error().is_none());
        assert_eq!(sim.active_connections.load(Ordering::Relaxed), 1);
        drop(guard);
        assert_eq!(sim.active_connections.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn connection_count_tracks_multiple_guards() {
        let sim = LatencySimulation::new();
        let g1 = sim.register_connection().await;
        let g2 = sim.register_connection().await;
        let g3 = sim.register_connection().await;
        assert_eq!(sim.active_connections.load(Ordering::Relaxed), 3);
        drop(g2);
        assert_eq!(sim.active_connections.load(Ordering::Relaxed), 2);
        drop(g1);
        drop(g3);
        assert_eq!(sim.active_connections.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn update_profile_merges_fields() {
        let sim = LatencySimulation::new();
        sim.update_profile(ServerLatencyProfile {
            random_delay_ms: Some((10, 20)),
            ..Default::default()
        })
        .await;
        sim.update_profile(ServerLatencyProfile {
            congestion_error_rate: Some(0.5),
            ..Default::default()
        })
        .await;
        let profile = sim.profile.read().await;
        assert_eq!(profile.random_delay_ms, Some((10, 20)));
        assert_eq!(profile.congestion_error_rate, Some(0.5));
    }

    #[tokio::test]
    async fn degradation_threshold_below_count_may_error() {
        let sim = LatencySimulation::new();
        sim.update_profile(ServerLatencyProfile {
            random_delay_ms: Some((0, 0)),
            connection_degradation_threshold: Some(0),
            congestion_error_rate: Some(1.0),
            congestion_penalty_ms: Some((0, 0)),
        })
        .await;
        let guard = sim.register_connection().await;
        assert!(guard.simulate_error().is_some());
        drop(guard);
    }
}
