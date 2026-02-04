//! Network profile for Toxiproxy: bandwidth, latency, jitter, and congestion (slicer).
//!
//! Profiles are direction-agnostic; apply to upload or download via
//! `NetworkSimulationProxy::apply_upload_profile()` / `apply_download_profile()`.
//!
//! Use congestion presets (`none()`, `medium()`, `heavy_congestion()`, `bursty()`)
//! then override with `with_bandwidth()`, `with_latency()` (string parsing, e.g. `"100kbps"`, `"100ms"`).

use super::error::{Result, SimulationError};

/// Slicer toxic parameters for bursty/congested links (fragments data with delay).
#[derive(Clone, Debug)]
pub struct SlicerParams {
    /// Average chunk size in bytes.
    pub average_size: u32,
    /// Size variation in bytes.
    pub size_variation: u32,
    /// Delay between chunks in milliseconds.
    pub delay_ms: u32,
}

/// Describes bandwidth, latency, jitter, and optional slicer for one direction.
///
/// Build from a congestion preset then chain `with_bandwidth()`, `with_latency()`.
/// Bandwidth/latency accept strings like `"100kbps"`, `"1mbps"`, `"100ms"`, `"1s"`.
#[derive(Clone)]
pub struct NetworkProfile {
    pub(crate) bandwidth_kbps: Option<u32>,
    pub(crate) latency_ms: Option<u32>,
    pub(crate) jitter_ms: Option<u32>,
    pub(crate) slicer: Option<SlicerParams>,
}

impl NetworkProfile {
    /// No congestion: empty profile (no toxics).
    pub fn none() -> Self {
        Self {
            bandwidth_kbps: None,
            latency_ms: None,
            jitter_ms: None,
            slicer: None,
        }
    }

    /// Light congestion: 50 ms latency, 10 ms jitter (bandwidth unset).
    pub fn medium() -> Self {
        Self {
            bandwidth_kbps: None,
            latency_ms: Some(50),
            jitter_ms: Some(10),
            slicer: None,
        }
    }

    /// Heavy congestion: 200 ms latency, 50 ms jitter, slicer for packet fragmentation (bandwidth unset).
    pub fn heavy_congestion() -> Self {
        Self {
            bandwidth_kbps: None,
            latency_ms: Some(200),
            jitter_ms: Some(50),
            slicer: Some(SlicerParams {
                average_size: 1024,
                size_variation: 512,
                delay_ms: 100,
            }),
        }
    }

    /// Bursty link: slicer with small chunks and delay, variable latency (bandwidth unset).
    pub fn bursty() -> Self {
        Self {
            bandwidth_kbps: None,
            latency_ms: Some(20),
            jitter_ms: Some(20),
            slicer: Some(SlicerParams {
                average_size: 512,
                size_variation: 256,
                delay_ms: 50,
            }),
        }
    }

    /// Override bandwidth. Accepts e.g. `"100kbps"`, `"1mbps"`, `"10mbps"` (case-insensitive).
    pub fn with_bandwidth(mut self, s: &str) -> Result<Self> {
        self.bandwidth_kbps = Some(parse_bandwidth(s)?);
        Ok(self)
    }

    /// Override latency and jitter. Accepts e.g. `"100ms"`, `"1s"` (case-insensitive).
    pub fn with_latency(mut self, latency: &str, jitter: &str) -> Result<Self> {
        self.latency_ms = Some(parse_duration(latency)?);
        self.jitter_ms = Some(parse_duration(jitter)?);
        Ok(self)
    }

    /// Override slicer (for fragmentation / bursty behavior).
    pub fn with_slicer(mut self, average_size: u32, size_variation: u32, delay_ms: u32) -> Self {
        self.slicer = Some(SlicerParams {
            average_size,
            size_variation,
            delay_ms,
        });
        self
    }

    /// Clears all settings.
    pub fn clear(&mut self) {
        self.bandwidth_kbps = None;
        self.latency_ms = None;
        self.jitter_ms = None;
        self.slicer = None;
    }

    pub fn is_empty(&self) -> bool {
        self.bandwidth_kbps.is_none() && self.latency_ms.is_none() && self.slicer.is_none()
    }
}

/// Parse bandwidth string to kbps. E.g. "100kbps", "1mbps", "1Mbps".
fn parse_bandwidth(s: &str) -> Result<u32> {
    let s = s.trim().to_lowercase();
    let s = s.as_str();
    let (num_str, unit) = split_number_unit(s)?;
    let n: u32 = num_str.parse().map_err(|_| invalid_profile("bandwidth", s))?;
    let kbps = match unit {
        "" | "kbps" => n,
        "mbps" => n.checked_mul(1000).ok_or_else(|| invalid_profile("bandwidth", s))?,
        "gbps" => n.checked_mul(1_000_000).ok_or_else(|| invalid_profile("bandwidth", s))?,
        _ => return Err(invalid_profile("bandwidth", s)),
    };
    if kbps == 0 {
        return Err(invalid_profile("bandwidth", s));
    }
    Ok(kbps)
}

/// Parse duration string to milliseconds. E.g. "100ms", "1s".
fn parse_duration(s: &str) -> Result<u32> {
    let s = s.trim().to_lowercase();
    let s = s.as_str();
    let (num_str, unit) = split_number_unit(s)?;
    let n: u32 = num_str.parse().map_err(|_| invalid_profile("duration", s))?;
    let ms = match unit {
        "" | "ms" => n,
        "s" => n.checked_mul(1000).ok_or_else(|| invalid_profile("duration", s))?,
        _ => return Err(invalid_profile("duration", s)),
    };
    Ok(ms)
}

fn split_number_unit(s: &str) -> Result<(&str, &str)> {
    let s = s.trim();
    if s.is_empty() {
        return Err(invalid_profile("value", s));
    }
    let boundary = s
        .char_indices()
        .find(|(_, c)| !c.is_ascii_digit() && *c != '.')
        .map(|(i, _)| i)
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(boundary);
    let unit = unit.trim();
    if num.is_empty() {
        return Err(invalid_profile("value", s));
    }
    Ok((num, unit))
}

fn invalid_profile(kind: &str, s: &str) -> SimulationError {
    SimulationError::InvalidProfile(format!("invalid {}: {:?}", kind, s))
}
