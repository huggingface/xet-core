use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bipbuffer::BipBuffer;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use tracing::Subscriber;
use tracing_subscriber::filter::FilterFn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

/// Default log level for the library to use. Override using `RUST_LOG` env variable.
/// TODO: probably change default to warn or error before shipping.
const DEFAULT_LOG_LEVEL: &str = "info";
const TELEMETRY_PRE_ALLOC_BYTES: usize = 2 * 1024 * 1024;
const TELEMETRY_PERIOD_MS: u64 = 100;
const HF_DEFAULT_ENDPOINT: &str = "https://huggingface.co";
const HF_DEFAULT_STAGING_ENDPOINT: &str = "https://hub-ci.huggingface.co";
const TELEMETRY_SUFFIX: &str = "api/telemetry/xet/cli";

fn is_telemetry_disabled() -> bool {
    matches!(env::var("HF_HUB_DISABLE_TELEMETRY").as_deref(), Ok("1"))
}
fn is_staging_mode() -> bool {
    matches!(env::var("HUGGINGFACE_CO_STAGING").as_deref(), Ok("1"))
}
fn get_telemetry_endpoint() -> Option<String> {
    if is_telemetry_disabled() {
        return None;
    }
    Some(env::var("HF_ENDPOINT").unwrap_or_else(|_| {
        if is_staging_mode() {
            HF_DEFAULT_STAGING_ENDPOINT.to_string()
        } else {
            HF_DEFAULT_ENDPOINT.to_string()
        }
    }))
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

#[derive(Default)]
pub struct LogBufferStats {
    // producers write into the circular buffer
    pub records_written: AtomicU64,
    pub records_refused: AtomicU64,
    pub bytes_written: AtomicU64,

    // consumer reads from the circular buffer
    pub records_read: AtomicU64,
    pub records_corrupted: AtomicU64,
    pub bytes_read: AtomicU64,

    // consumer transmits telemetry to moonlanding
    pub records_transmitted: AtomicU64,
    pub records_dropped: AtomicU64,
    pub bytes_refused: AtomicU64,
}

/// A high-performance telemetry store using a pre-allocated memory buffer.
///
/// `LogBufferLayer` is designed for collecting structured telemetry data with multiple producers
/// and a single consumer (MPSC model). It leverages a pre-allocated memory buffer (`BipBuffer`)
/// to ensure low latency and predictable performance.
///
/// Key features:
/// - **Multiple Producers, Single Consumer (MPSC)**: Allows multiple threads to write telemetry records simultaneously
///   while a single consumer processes the records in order.
/// - **Variable-Length Records**: Supports storing records of varying sizes, making it suitable for dynamic telemetry
///   payloads.
/// - **Pre-allocated Memory**: Uses a fixed-size buffer to avoid dynamic memory allocation, ensuring high performance
///   and minimal overhead.
///
/// ### Example Usage
/// ```rust
/// use tracing::info;
///
/// info!(
///   target: "client_telemetry",
///   repo = "acme/cats",
///   file_name = "data/1.parquet",
///   action = "upload",
///   start_time = "2024-12-01T12:00:00Z",
///   metadata_timestamp_offset = 500,
///   metadata_bytes = 1024,
///   data_bytes = 1048576,
///   data_deduped_bytes = 524288,
///   data_compressed_bytes = 262144,
///   end_time = "2024-12-01T12:01:00Z"
/// );
/// ```
struct LogBufferLayer {
    buffer: Arc<Mutex<BipBuffer<u8>>>,
    stats: Arc<LogBufferStats>,
}

impl<S> Layer<S> for LogBufferLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut buffer = self.buffer.lock().unwrap();
        let mut http_headers = HeaderMap::new();

        {
            let mut visitor = |field: &tracing::field::Field, value: &dyn std::fmt::Debug| {
                http_headers.insert(field.name(), HeaderValue::from_str(&format!("{:?}", value)).unwrap());
            };
            event.record(&mut visitor);
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

pub fn initialize_logging() {
    // TODO: maybe have an env variable for writing to a log file instead of stderr
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_line_number(true)
        .with_file(true)
        .with_target(false)
        .json();

    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(DEFAULT_LOG_LEVEL))
        .unwrap_or_default();
    if is_telemetry_disabled() {
        tracing_subscriber::registry().with(fmt_layer).with(filter_layer).init();
    } else {
        let telemetry_log_buffer: Arc<Mutex<BipBuffer<u8>>> =
            Arc::new(Mutex::new(BipBuffer::new(TELEMETRY_PRE_ALLOC_BYTES)));
        let telemetry_log_stats = Arc::new(LogBufferStats::default());
        let telemetry_buffer_layer = LogBufferLayer {
            buffer: Arc::clone(&telemetry_log_buffer),
            stats: Arc::clone(&telemetry_log_stats),
        };
        let telemetry_filter_layer =
            telemetry_buffer_layer.with_filter(FilterFn::new(|meta| meta.target() == "client_telemetry"));
        tracing_subscriber::registry()
            .with(fmt_layer)
            .with(filter_layer)
            .with(telemetry_filter_layer)
            .init();

        let _telemetry_task = tokio::task::spawn({
            let log_buffer = Arc::clone(&telemetry_log_buffer);
            let log_stats = Arc::clone(&telemetry_log_stats);
            async move {
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
                        if let Some(endpoint) = get_telemetry_endpoint() {
                            let telemetry_url = format!("https://{endpoint}/{TELEMETRY_SUFFIX}");
                            let client = reqwest::Client::new();
                            if let Ok(response) = client.head(telemetry_url).headers(http_header_map).send().await {
                                if response.status().is_success() {
                                    log_stats.records_transmitted.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    log_stats.records_dropped.fetch_add(1, Ordering::Relaxed);
                                }
                            } else {
                                log_stats.records_dropped.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(TELEMETRY_PERIOD_MS)).await;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use bipbuffer::BipBuffer;

    use super::*;

    #[test]
    fn test_buffer_layer() {
        let buffer = Arc::new(Mutex::new(BipBuffer::new(36 * 2)));
        let stats = Arc::new(LogBufferStats::default());
        let layer = LogBufferLayer {
            buffer: Arc::clone(&buffer),
            stats: Arc::clone(&stats),
        };

        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "client_telemetry", "36 b event");
            assert_eq!(stats.records_written.load(Ordering::Relaxed), 1);
            assert_eq!(stats.records_refused.load(Ordering::Relaxed), 0);
            assert_eq!(stats.bytes_written.load(Ordering::Relaxed), 36);
            assert_eq!(stats.bytes_refused.load(Ordering::Relaxed), 0);

            for _ in 0..9 {
                tracing::info!(target: "client_telemetry", "test event");
            }
            assert_eq!(stats.records_written.load(Ordering::Relaxed), 2);
            assert_eq!(stats.records_refused.load(Ordering::Relaxed), 8);
            assert_eq!(stats.bytes_written.load(Ordering::Relaxed), 36 * 2);
            assert_eq!(stats.bytes_refused.load(Ordering::Relaxed), 36 * 8);
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
