//! Errors for network simulation (Toxiproxy, endpoint parsing).

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SimulationError {
    #[error("Toxiproxy error: {0}")]
    Toxiproxy(String),

    #[error("Invalid endpoint: {0}")]
    InvalidEndpoint(String),

    #[error("No available port")]
    NoAvailablePort,

    #[error("Invalid profile: {0}")]
    InvalidProfile(String),
}

pub type Result<T> = std::result::Result<T, SimulationError>;
