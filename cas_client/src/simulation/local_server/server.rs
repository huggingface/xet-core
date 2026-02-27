//! Local CAS Server Implementation
//!
//! This module provides `LocalServer`, an HTTP server that wraps `LocalClient`
//! and exposes its functionality via REST API endpoints compatible with `RemoteClient`.
//!
//! # Architecture
//!
//! The server uses Axum as its HTTP framework and shares an `Arc<LocalClient>`
//! across all request handlers. Routes are organized to match the API expected
//! by `RemoteClient`, with some legacy route aliases for compatibility.
//!
//! # Example
//!
//! ```no_run
//! use cas_client::{LocalServer, LocalServerConfig};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = LocalServerConfig {
//!         data_directory: "./data".into(),
//!         host: "127.0.0.1".to_string(),
//!         port: 8080,
//!         in_memory: false,
//!     };
//!     let server = LocalServer::new(config).await?;
//!     server.run().await?;
//!     Ok(())
//! }
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::routing::{get, head, post};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

use super::handlers;
use super::latency_simulation::LatencySimulation;
use crate::error::{CasClientError, Result};
use crate::simulation::{DirectAccessClient, LocalClient, MemoryClient};

/// Configuration for the local CAS server.
#[derive(Debug, Clone)]
pub struct LocalServerConfig {
    /// Directory where CAS data (XORBs, shards, indices) will be stored.
    /// Only used when `in_memory` is false.
    pub data_directory: PathBuf,
    /// Network interface to bind to (e.g., "127.0.0.1" or "0.0.0.0").
    pub host: String,
    /// TCP port number for the HTTP server.
    pub port: u16,
    /// Whether to use in-memory storage instead of disk-backed storage.
    pub in_memory: bool,
}

impl Default for LocalServerConfig {
    fn default() -> Self {
        Self {
            data_directory: PathBuf::from("./local_cas_data"),
            host: "127.0.0.1".to_string(),
            port: 8080,
            in_memory: false,
        }
    }
}

/// A local HTTP server that wraps a `DirectAccessClient` and exposes CAS operations via REST API.
///
/// This server implements the same API that `RemoteClient` expects, making it useful for:
/// - Integration testing without a remote backend
/// - Local development and debugging
/// - Offline CAS workflows
///
/// The server can use either a disk-backed `LocalClient` or an in-memory `MemoryClient`.
pub struct LocalServer {
    config: LocalServerConfig,
    client: Arc<dyn DirectAccessClient>,
    latency_simulation: Arc<LatencySimulation>,
}

impl LocalServer {
    /// Creates a new server with the given configuration.
    ///
    /// If `in_memory` is false, creates a new `LocalClient` pointing to the configured data directory.
    /// If `in_memory` is true, creates a new `MemoryClient` (data directory is ignored).
    pub async fn new(config: LocalServerConfig) -> Result<Self> {
        let client: Arc<dyn DirectAccessClient> = if config.in_memory {
            MemoryClient::new()
        } else {
            LocalClient::new(&config.data_directory).await?
        };
        let latency_simulation = LatencySimulation::new();
        Ok(Self {
            config,
            client,
            latency_simulation,
        })
    }

    /// Creates a server from an existing `DirectAccessClient`.
    ///
    /// Useful when you want to share a client instance between the server
    /// and other code (e.g., for testing where you want to verify server behavior
    /// against direct client access).
    pub fn from_client(client: Arc<dyn DirectAccessClient>, host: String, port: u16) -> Self {
        let latency_simulation = LatencySimulation::new();
        Self {
            config: LocalServerConfig {
                data_directory: PathBuf::new(),
                host,
                port,
                in_memory: false,
            },
            client,
            latency_simulation,
        }
    }

    /// Returns a clone of the underlying client.
    pub fn client(&self) -> Arc<dyn DirectAccessClient> {
        self.client.clone()
    }

    /// Returns the server's bind address as "host:port".
    pub fn addr(&self) -> String {
        format!("{}:{}", self.config.host, self.config.port)
    }

    /// Builds the Axum router with all CAS API routes.
    ///
    /// Routes follow the pattern used by RemoteClient:
    /// - `/v1/` prefixed routes for chunks, xorbs, reconstructions, and files
    /// - Root-level `/reconstructions` for batch queries and `/shards` for uploads
    /// - `/simulation/` prefixed routes for testing/simulation configuration
    fn create_router(&self) -> Router {
        Router::new()
            .route("/health", get(handlers::health_check))
            .nest(
                "/v1",
                Router::new()
                    .route("/reconstructions", get(handlers::batch_get_reconstruction))
                    .route("/reconstructions/{file_id}", get(handlers::get_reconstruction))
                    .route("/chunks/{prefix}/{hash}", get(handlers::get_dedup_info_by_chunk))
                    .route("/xorbs/{prefix}/{hash}", head(handlers::head_xorb).post(handlers::post_xorb))
                    .route("/files/{file_id}", head(handlers::head_file))
                    .route("/get_xorb/{prefix}/{hash}/", get(handlers::get_file_term_data))
                    .route("/fetch_term", get(handlers::fetch_term)),
            )
            .nest(
                "/simulation",
                Router::new()
                    .route("/ping", get(handlers::ping))
                    .route("/set_config", post(handlers::set_config))
                    .route("/dummy_upload", post(handlers::dummy_upload)),
            )
            // Routes used by RemoteClient without /v1/ prefix
            .route("/reconstructions", get(handlers::batch_get_reconstruction))
            .route("/shards", post(handlers::post_shard))
            .layer(CorsLayer::very_permissive())
            .with_state(handlers::ServerState {
                client: self.client.clone(),
                latency_simulation: self.latency_simulation.clone(),
            })
    }

    /// Runs the server, listening for incoming HTTP requests.
    ///
    /// This method blocks until the server is shut down via signal (Ctrl+C on Unix).
    /// For programmatic shutdown, use `run_until_stopped` instead.
    pub async fn run(&self) -> Result<()> {
        let addr: SocketAddr = self
            .addr()
            .parse()
            .map_err(|e| CasClientError::Other(format!("Failed to parse address: {e}")))?;

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| CasClientError::Other(format!("Failed to bind to {addr}: {e}")))?;

        tracing::info!("Local CAS server listening on {}", addr);

        let router = self.create_router();

        axum::serve(listener, router.into_make_service())
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|e| CasClientError::Other(format!("Server error: {e}")))
    }

    /// Runs the server until a shutdown signal is received on the provided channel.
    ///
    /// This is useful for tests where you want programmatic control over server lifecycle.
    pub async fn run_until_stopped(&self, shutdown_rx: tokio::sync::oneshot::Receiver<()>) -> Result<()> {
        let addr: SocketAddr = self
            .addr()
            .parse()
            .map_err(|e| CasClientError::Other(format!("Failed to parse address: {e}")))?;

        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| CasClientError::Other(format!("Failed to bind to {addr}: {e}")))?;

        tracing::info!("Local CAS server listening on {}", addr);

        let router = self.create_router();

        axum::serve(listener, router.into_make_service())
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(|e| CasClientError::Other(format!("Server error: {e}")))
    }
}

/// Waits for a shutdown signal (currently blocks forever as there's no SIGTERM handling).
async fn shutdown_signal() {
    std::future::pending::<()>().await
}
