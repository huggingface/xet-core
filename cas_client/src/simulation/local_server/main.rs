//! Local CAS Server Binary
//!
//! This binary provides a local HTTP server that wraps `LocalClient`, exposing
//! the same REST API that `RemoteClient` expects from a remote CAS server.
//!
//! # Purpose
//!
//! The local CAS server enables:
//! - **Testing**: Run integration tests against a local server using `RemoteClient` without needing a remote backend.
//! - **Development**: Develop and debug CAS client interactions locally.
//! - **Offline workflows**: Store and retrieve CAS objects without network access.
//!
//! # Usage
//!
//! ```bash
//! # Start with default settings (port 8080, data in ./local_cas_data)
//! local_cas_server
//!
//! # Specify custom data directory and port
//! local_cas_server --data-directory /path/to/storage --port 9000
//!
//! # Bind to all interfaces
//! local_cas_server --host 0.0.0.0 --port 8080
//! ```
//!
//! # API Endpoints
//!
//! The server exposes the following endpoints (compatible with `RemoteClient`):
//!
//! - `GET /health` - Health check endpoint
//! - `GET /v1/reconstructions/{file_id}` - Get file reconstruction info
//! - `POST /v1/reconstructions` - Batch query for multiple file reconstructions
//! - `GET /v1/chunks/{prefix}/{hash}` - Query for global deduplication shard
//! - `HEAD /v1/xorbs/{prefix}/{hash}` - Check if XORB exists
//! - `POST /v1/xorbs/{prefix}/{hash}` - Upload a XORB
//! - `POST /shards` - Upload a shard
//! - `HEAD /v1/files/{file_id}` - Get file size
//! - `GET /get_xorb/{prefix}/{hash}/` - Download XORB data
//!
//! # Environment Variables
//!
//! - `RUST_LOG` - Control logging verbosity (e.g., `RUST_LOG=info` or `RUST_LOG=debug`)

use std::path::PathBuf;

use cas_client::{LocalServer, LocalServerConfig};
use clap::Parser;
use tracing_subscriber::EnvFilter;

/// A local HTTP server that wraps a DirectAccessClient for testing and development.
///
/// This server exposes the same REST API as the remote CAS server, allowing
/// RemoteClient to connect and interact with locally stored CAS objects.
/// Useful for integration testing, development, and offline workflows.
///
/// By default uses disk-backed storage; use --in-memory for in-memory storage.
#[derive(Parser, Debug)]
#[command(name = "local_cas_server")]
#[command(version, about, long_about = None)]
struct Args {
    /// Directory where CAS data (XORBs, shards, indices) will be stored.
    ///
    /// This directory will be created if it doesn't exist. All CAS objects
    /// uploaded to this server will be persisted here. Multiple server
    /// instances can share the same directory for read operations, but
    /// concurrent writes should be avoided.
    ///
    /// Ignored when --in-memory is specified.
    #[arg(short, long, default_value = "./local_cas_data")]
    data_directory: PathBuf,

    /// Network interface to bind the server to.
    ///
    /// Use "127.0.0.1" (default) for local-only access, or "0.0.0.0" to
    /// accept connections from any interface.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// TCP port number for the HTTP server.
    ///
    /// The server will listen on this port for incoming HTTP requests.
    /// Make sure this port is not already in use by another process.
    #[arg(short, long, default_value = "8080")]
    port: u16,

    /// Use in-memory storage instead of disk-backed storage.
    ///
    /// When enabled, all data is stored in memory and will be lost when
    /// the server stops. This is useful for testing or ephemeral workloads.
    /// The --data-directory option is ignored when this is enabled.
    #[arg(long, default_value = "false")]
    in_memory: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing with environment filter (respects RUST_LOG)
    tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env()).init();

    let args = Args::parse();

    let config = LocalServerConfig {
        data_directory: args.data_directory,
        host: args.host,
        port: args.port,
        in_memory: args.in_memory,
    };

    tracing::info!("Starting local CAS server with config: {:?}", config);
    if config.in_memory {
        tracing::info!("Storage mode: in-memory");
    } else {
        tracing::info!("Storage mode: disk-backed");
        tracing::info!("Data directory: {:?}", config.data_directory);
    }
    tracing::info!("Listening on: {}:{}", config.host, config.port);

    let server = LocalServer::new(config).await?;
    server.run().await?;

    Ok(())
}
