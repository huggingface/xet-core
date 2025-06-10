use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bipbuffer::BipBuffer;
use cas_client::exports::reqwest;
use cas_client::exports::reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::{debug, Subscriber};
use tracing_subscriber::Layer;
use xet_threadpool::exports::tokio;

pub const TELEMETRY_PRE_ALLOC_BYTES: usize = 2 * 1024 * 1024;
pub const TELEMETRY_PERIOD_MS: u64 = 100;
pub const HF_DEFAULT_ENDPOINT: &str = "https://huggingface.co";
pub const HF_DEFAULT_STAGING_ENDPOINT: &str = "https://hub-ci.huggingface.co";
pub const TELEMETRY_SUFFIX: &str = "api/telemetry/xet/cli";

#[derive(Debug)]
pub struct LogBufferStats {
    pub records_written: AtomicU64,
    pub records_refused: AtomicU64,
    pub bytes_written: AtomicU64,
    pub records_read: AtomicU64,
    pub records_corrupted: AtomicU64,
    pub bytes_read: AtomicU64,
    pub records_transmitted: AtomicU64,
    pub records_dropped: AtomicU64,
    pub bytes_refused: AtomicU64,
}

impl Default for LogBufferStats {
    fn default() -> Self {
        Self {
            records_written: AtomicU64::new(0),
            records_refused: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            records_read: AtomicU64::new(0),
            records_corrupted: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            records_transmitted: AtomicU64::new(0),
            records_dropped: AtomicU64::new(0),
            bytes_refused: AtomicU64::new(0),
        }
    }
}

fn is_staging_mode() -> bool {
    matches!(env::var("HUGGINGFACE_CO_STAGING").as_deref(), Ok("1"))
}

pub fn get_telemetry_endpoint() -> Option<String> {
    Some(env::var("HF_ENDPOINT").unwrap_or_else(|_| {
        if is_staging_mode() {
            HF_DEFAULT_STAGING_ENDPOINT.to_string()
        } else {
            HF_DEFAULT_ENDPOINT.to_string()
        }
    }))
}

pub type TelemetryTaskInfo = (Arc<Mutex<BipBuffer<u8>>>, Arc<LogBufferStats>);

pub async fn get_telemetry_task(telemetry_task_info: TelemetryTaskInfo) {
    let (log_buffer, log_stats) = telemetry_task_info;
    let client = reqwest::Client::new();
    let telemetry_url = format!("{}/{}", get_telemetry_endpoint().unwrap_or_default(), TELEMETRY_SUFFIX);

    loop {
        let mut read_len: usize = 0;
        let mut http_header_map: HeaderMap = HeaderMap::new();

        {
            let mut buffer = log_buffer.lock().unwrap();

            if let Some(block) = buffer.read() {
                read_len = block.len();
                log_stats.bytes_read.fetch_add(read_len as u64, Ordering::Relaxed);

                if let Ok(deserialized) = serde_json::from_slice::<SerializableHeaders>(block) {
                    if let Ok(http_header_map_deserialized) = deserialized.try_into() {
                        log_stats.records_read.fetch_add(1, Ordering::Relaxed);
                        http_header_map = http_header_map_deserialized;
                    } else {
                        log_stats.records_corrupted.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    log_stats.records_corrupted.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        if read_len > 0 {
            let mut buffer = log_buffer.lock().unwrap();
            buffer.decommit(read_len);
        }

        if !http_header_map.is_empty() {
            if let Ok(response) = client.head(telemetry_url.clone()).headers(http_header_map).send().await {
                if response.status().is_success() {
                    log_stats.records_transmitted.fetch_add(1, Ordering::Relaxed);
                } else {
                    debug!("Failed to transmit telemetry to {}: HTTP status {}", telemetry_url, response.status());
                    log_stats.records_dropped.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                debug!("Failed to send HEAD request to {}: Error occurred during transmission", telemetry_url);
                log_stats.records_dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
        debug!("Stats from telemetry {:?}", log_stats);
        tokio::time::sleep(tokio::time::Duration::from_millis(TELEMETRY_PERIOD_MS)).await;
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct SerializableHeaders {
    headers: HashMap<String, String>,
}

impl From<&HeaderMap> for SerializableHeaders {
    fn from(header_map: &HeaderMap) -> Self {
        let headers = header_map
            .iter()
            .filter_map(|(name, value)| {
                let name = name.to_string();
                let value = value.to_str().ok()?.to_string();
                Some((name, value))
            })
            .collect();

        SerializableHeaders { headers }
    }
}

impl TryFrom<SerializableHeaders> for HeaderMap {
    type Error = reqwest::header::InvalidHeaderValue;

    fn try_from(serializable: SerializableHeaders) -> Result<Self, Self::Error> {
        let mut header_map = HeaderMap::new();
        for (key, value) in serializable.headers {
            let name = HeaderName::from_bytes(key.as_bytes()).unwrap();
            let val = HeaderValue::from_str(&value)?;
            header_map.insert(name, val);
        }
        Ok(header_map)
    }
}

pub struct LogBufferLayer {
    pub buffer: Arc<Mutex<BipBuffer<u8>>>,
    pub stats: Arc<LogBufferStats>,
    telemetry_versions: String,
}

impl LogBufferLayer {
    pub fn new(py: Python, size: usize) -> Self {
        let buffer = Arc::new(Mutex::new(BipBuffer::new(size)));
        let stats = Arc::new(LogBufferStats::default());
        // populate remote telemetry calls with versions for python and hf_hub if possible
        let mut telemetry_versions = String::new();

        // Get Python version
        if let Ok(sys) = py.import("sys") {
            if let Ok(version) = sys.getattr("version").and_then(|v| v.extract::<String>()) {
                if let Some(python_version_number) = version.split_whitespace().next() {
                    telemetry_versions.push_str(&format!("python/{python_version_number}; "));
                }
            }
        }

        // Get huggingface_hub+hf_xet versions
        let package_names = ["huggingface_hub", "hfxet"];
        if let Ok(importlib_metadata) = py.import("importlib.metadata") {
            for package_name in package_names.iter() {
                if let Ok(version) = importlib_metadata
                    .call_method1("version", (package_name,))
                    .and_then(|v| v.extract::<String>())
                {
                    telemetry_versions.push_str(&format!("{package_name}/{version}; "));
                }
            }
        }

        Self {
            buffer,
            stats,
            telemetry_versions,
        }
    }
}

impl<S> Layer<S> for LogBufferLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut buffer = self.buffer.lock().unwrap();
        let mut http_headers = HeaderMap::new();

        {
            let mut user_agent = self.telemetry_versions.clone();
            let mut visitor = |field: &tracing::field::Field, value: &dyn std::fmt::Debug| {
                user_agent.push_str(&format!("{}/{:?}; ", field.name(), value));
            };
            event.record(&mut visitor);
            user_agent = user_agent.replace("\"", "");
            if let Ok(header_value) = HeaderValue::from_str(&user_agent) {
                http_headers.insert("User-Agent", header_value);
            } else {
                self.stats.records_refused.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        let serializable: SerializableHeaders = (&http_headers).into();
        if let Ok(serialized_headers) = serde_json::to_string(&serializable) {
            if let Ok(reserved) = buffer.reserve(serialized_headers.len()) {
                if reserved.len() < serialized_headers.len() {
                    // log goes to /dev/null if not enough free space
                    self.stats.records_refused.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .bytes_refused
                        .fetch_add(serialized_headers.len() as u64, Ordering::Relaxed);
                    buffer.commit(0);
                } else {
                    self.stats.records_written.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .bytes_written
                        .fetch_add(serialized_headers.len() as u64, Ordering::Relaxed);
                    reserved[..serialized_headers.len()].copy_from_slice(serialized_headers.as_bytes());
                    buffer.commit(serialized_headers.len());
                }
            } else {
                self.stats.records_refused.fetch_add(1, Ordering::Relaxed);
                self.stats
                    .bytes_refused
                    .fetch_add(serialized_headers.len() as u64, Ordering::Relaxed);
            }
        } else {
            self.stats.records_refused.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use bipbuffer::BipBuffer;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    #[test]
    fn test_buffer_layer() {
        let buffer = Arc::new(Mutex::new(BipBuffer::new(50 * 2)));
        let stats = Arc::new(LogBufferStats::default());
        let layer = LogBufferLayer {
            buffer: Arc::clone(&buffer),
            stats: Arc::clone(&stats),
            telemetry_versions: "Testing".to_owned(),
        };

        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "client_telemetry", "50 b event");
            assert_eq!(stats.records_written.load(Ordering::Relaxed), 1);
            assert_eq!(stats.records_refused.load(Ordering::Relaxed), 0);
            assert_eq!(stats.bytes_written.load(Ordering::Relaxed), 50);
            assert_eq!(stats.bytes_refused.load(Ordering::Relaxed), 0);

            for _ in 0..9 {
                tracing::info!(target: "client_telemetry", "test event");
            }
            assert_eq!(stats.records_written.load(Ordering::Relaxed), 2);
            assert_eq!(stats.records_refused.load(Ordering::Relaxed), 8);
            assert_eq!(stats.bytes_written.load(Ordering::Relaxed), 50 * 2);
            assert_eq!(stats.bytes_refused.load(Ordering::Relaxed), 50 * 8);
        });
    }

    #[test]
    fn test_serializable() {
        let mut header_map = HeaderMap::new();
        header_map.insert("Content-Type", HeaderValue::from_static("application/json"));
        header_map.insert("Authorization", HeaderValue::from_static("Bearer token"));

        let serializable: SerializableHeaders = (&header_map).into();

        assert_eq!(serializable.headers.get("content-type"), Some(&"application/json".to_string()));
        assert_eq!(serializable.headers.get("authorization"), Some(&"Bearer token".to_string()));

        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        headers.insert("Authorization".to_string(), "Bearer token".to_string());

        let serializable = SerializableHeaders { headers };
        let header_map: Result<HeaderMap, _> = HeaderMap::try_from(serializable);

        assert!(header_map.is_ok());
        let header_map = header_map.unwrap();
        assert_eq!(header_map.get("Content-Type").unwrap(), "application/json");
        assert_eq!(header_map.get("Authorization").unwrap(), "Bearer token");
    }
}
