//! Delay Simulation for Local CAS Server
//!
//! This module provides configurable delay and congestion simulation for the local server.
//! It tracks active connections and applies delays or rejects requests based on configured profiles.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rand::Rng;
use rand::rngs::ThreadRng;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Configuration profile for delay and congestion simulation.
///
/// All fields are optional, allowing partial updates via HTTP.
/// When a profile is uploaded, only non-None fields overwrite the existing profile.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServerDelayProfile {
    /// Random delay range for all API calls (milliseconds).
    /// Format: (min_ms, max_ms). Applied before processing each request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub random_delay_ms: Option<(u64, u64)>,

    /// Number of concurrent connections above which congestion kicks in.
    /// If None, congestion is disabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_threshold: Option<u64>,

    /// Minimum delay when congested (milliseconds).
    /// Applied when active connections exceed the threshold.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_congestion_penalty_ms: Option<u64>,

    /// Maximum delay when congested (milliseconds).
    /// Applied when active connections exceed the threshold.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_congestion_penalty_ms: Option<u64>,

    /// Probability of returning 503 when congested (0.0-1.0).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub congestion_error_rate: Option<f64>,
}

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// Guard that tracks an active connection and decrements the connection count on drop.
///
/// This guard is Send + Sync because it only contains Send + Sync types (Arc and bool).
pub struct ConnectionGuard {
    state: Arc<DelaySimulation>,
    should_simulate_error: bool,
}

impl ConnectionGuard {
    /// Returns an error response if delay simulation indicates an error should be simulated.
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

/// Manages delay simulation state and profile.
pub(super) struct DelaySimulation {
    profile: RwLock<ServerDelayProfile>,
    active_connections: AtomicU64,
}

impl DelaySimulation {
    /// Create a new delay simulation with default (empty) profile.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            profile: RwLock::new(ServerDelayProfile::default()),
            active_connections: AtomicU64::new(0),
        })
    }

    /// Update the profile, merging with existing values.
    ///
    /// Only non-None fields in `update` overwrite the existing profile.
    pub async fn update_profile(&self, update: ServerDelayProfile) {
        let mut profile = self.profile.write().await;
        if let Some(delay) = update.random_delay_ms {
            profile.random_delay_ms = Some(delay);
        }
        if let Some(threshold) = update.connection_threshold {
            profile.connection_threshold = Some(threshold);
        }
        if let Some(min) = update.min_congestion_penalty_ms {
            profile.min_congestion_penalty_ms = Some(min);
        }
        if let Some(max) = update.max_congestion_penalty_ms {
            profile.max_congestion_penalty_ms = Some(max);
        }
        if let Some(rate) = update.congestion_error_rate {
            profile.congestion_error_rate = Some(rate);
        }
    }

    /// Get the current profile (for reading).
    #[allow(dead_code)]
    pub async fn get_profile(&self) -> ServerDelayProfile {
        self.profile.read().await.clone()
    }

    /// Get the current number of active connections.
    #[allow(dead_code)]
    pub fn active_connections(&self) -> u64 {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Register a connection, applying delay simulation.
    ///
    /// This increments the active connection count, applies any configured delays,
    /// and returns a guard that must be kept alive for the duration of the request.
    ///
    /// The guard automatically decrements the connection count when dropped.
    /// Use `guard.simulate_error()` to check if an error should be returned.
    pub async fn register_connection(self: &Arc<Self>) -> ConnectionGuard {
        // Increment active connections
        let count = self.active_connections.fetch_add(1, Ordering::Relaxed) + 1;

        let profile = self.profile.read().await;

        // Apply random delay if configured
        let random_delay = profile.random_delay_ms.map(|(min_ms, max_ms)| {
            Duration::from_millis(ThreadRng::default().random_range(min_ms..=(max_ms.max(min_ms))))
        });

        // Check congestion
        let should_simulate_error = if let Some(threshold) = profile.connection_threshold
            && count > threshold
        {
            // We're congested
            if let Some(error_rate) = profile.congestion_error_rate
                && ThreadRng::default().random_range(0.0..1.0) < error_rate
            {
                // Will simulate error
                true
            } else {
                // Apply congestion penalty delay
                if let (Some(min_ms), Some(max_ms)) =
                    (profile.min_congestion_penalty_ms, profile.max_congestion_penalty_ms)
                {
                    let penalty_ms = if min_ms == max_ms {
                        min_ms
                    } else {
                        ThreadRng::default().random_range(min_ms..=max_ms)
                    };
                    let penalty_delay = Duration::from_millis(penalty_ms);
                    // Combine with random delay if present
                    let total_delay = random_delay.map(|d| d + penalty_delay).unwrap_or(penalty_delay);
                    tokio::time::sleep(total_delay).await;
                } else if let Some(delay) = random_delay {
                    tokio::time::sleep(delay).await;
                }
                false
            }
        } else {
            // Not congested, apply random delay if configured
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

impl Default for DelaySimulation {
    fn default() -> Self {
        Self {
            profile: RwLock::new(ServerDelayProfile::default()),
            active_connections: AtomicU64::new(0),
        }
    }
}
