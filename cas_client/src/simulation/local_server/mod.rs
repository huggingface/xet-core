//! Local CAS Server Module
//!
//! This module provides an HTTP server that wraps `LocalClient`, exposing the same
//! REST API that `RemoteClient` expects from a remote CAS server. This enables:
//!
//! - **Integration testing**: Test `RemoteClient` against a local server
//! - **Development**: Debug CAS operations without network dependencies
//! - **Offline workflows**: Store and retrieve CAS objects locally
//!
//! # Components
//!
//! - [`LocalServer`]: The main server struct that manages the HTTP listener
//! - [`LocalServerConfig`]: Configuration for the server (host, port, data directory)
//! - `handlers`: HTTP request handlers for each API endpoint

mod handlers;
mod latency_simulation;
mod server;

pub use latency_simulation::ServerLatencyProfile;
pub use server::{LocalServer, LocalServerConfig};
