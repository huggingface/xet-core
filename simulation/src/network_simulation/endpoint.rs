//! Endpoint parsing for network simulation (e.g. http://host:port -> host:port).

use super::error::{Result, SimulationError};

/// Extracts host:port from an HTTP(S) endpoint URL.
pub fn endpoint_to_host_port(endpoint: &str) -> Result<String> {
    let s = endpoint.trim_end_matches('/');
    let host_port = s
        .strip_prefix("http://")
        .or_else(|| s.strip_prefix("https://"))
        .ok_or_else(|| SimulationError::InvalidEndpoint(endpoint.to_string()))?;
    if host_port.is_empty() {
        return Err(SimulationError::InvalidEndpoint(endpoint.to_string()));
    }
    Ok(host_port.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endpoint_to_host_port() {
        assert_eq!(endpoint_to_host_port("http://127.0.0.1:12345").unwrap(), "127.0.0.1:12345");
        assert_eq!(endpoint_to_host_port("https://localhost:8080/").unwrap(), "localhost:8080");
        assert!(endpoint_to_host_port("ftp://x").is_err());
        assert!(endpoint_to_host_port("http://").is_err());
    }
}
