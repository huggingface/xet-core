//! Errors for network simulation (proxy, endpoint parsing).

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SimulationError {
    #[error("Proxy error: {0}")]
    Proxy(String),

    #[error("Invalid endpoint: {0}")]
    InvalidEndpoint(String),

    #[error("No available port")]
    NoAvailablePort,

    #[error("Invalid profile: {0}")]
    InvalidProfile(String),
}

pub type Result<T> = std::result::Result<T, SimulationError>;
