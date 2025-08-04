use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bipbuffer::BipBuffer;
use cas_client::exports::reqwest;
use cas_client::exports::reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use tracing::{debug, Subscriber};
use tracing_subscriber::Layer;
use xet_threadpool::errors::MultithreadedRuntimeError;
use xet_threadpool::exports::tokio;

pub const TELEMETRY_PRE_ALLOC_BYTES: usize = 2 * 1024 * 1024;
pub const TELEMETRY_PERIOD_MS: u64 = 100;
pub const HF_DEFAULT_ENDPOINT: &str = "https://huggingface.co";
pub const HF_DEFAULT_STAGING_ENDPOINT: &str = "https://hub-ci.huggingface.co";
pub const TELEMETRY_SUFFIX: &str = "api/telemetry/xet/cli";

#[derive(Debug)]
pub struct LoggingStats {
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

impl Default for LoggingStats {
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

pub fn get_telemetry_endpoint() -> String {
    env::var("HF_ENDPOINT").unwrap_or_else(|_| {
        if is_staging_mode() {
            HF_DEFAULT_STAGING_ENDPOINT.to_string()
        } else {
            HF_DEFAULT_ENDPOINT.to_string()
        }
    })
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

pub struct TelemetryLogger {
    log_buffer: Mutex<BipBuffer<u8>>,
    stats: LoggingStats,
    version_info: String,
}

#[derive(Clone)]
pub struct TelemetryLoggerPtr(Arc<TelemetryLogger>);

impl TelemetryLogger {
    pub(crate) fn init(version_info: String) -> Result<TelemetryLoggerPtr, MultithreadedRuntimeError> {
        let log_buffer = Mutex::new(BipBuffer::new(TELEMETRY_PRE_ALLOC_BYTES));
        let stats = LoggingStats::default();

        // Start up the background process.
        let s = Arc::new(Self {
            log_buffer,
            stats,
            version_info,
        });

        s.spawn_telemetry_task()?;

        Ok(TelemetryLoggerPtr(s))
    }

    fn spawn_telemetry_task(self: &Arc<Self>) -> Result<(), MultithreadedRuntimeError> {
        let client = reqwest::Client::new();
        let telemetry_url = format!("{}/{}", get_telemetry_endpoint(), TELEMETRY_SUFFIX);

        let s = self.clone();

        // Set up the task.
        let telemetry_send_task = async move {
            let mut interval = tokio::time::interval(Duration::from_millis(TELEMETRY_PERIOD_MS));

            loop {
                // Use tokio tick to run this at regular intervals
                interval.tick().await;

                let mut read_len: usize = 0;
                let mut http_header_map: HeaderMap = HeaderMap::new();

                {
                    let mut buffer = s.log_buffer.lock().unwrap();

                    if let Some(block) = buffer.read() {
                        read_len = block.len();
                        s.stats.bytes_read.fetch_add(read_len as u64, Ordering::Relaxed);

                        if let Ok(deserialized) = serde_json::from_slice::<SerializableHeaders>(block) {
                            if let Ok(http_header_map_deserialized) = deserialized.try_into() {
                                s.stats.records_read.fetch_add(1, Ordering::Relaxed);
                                http_header_map = http_header_map_deserialized;
                            } else {
                                s.stats.records_corrupted.fetch_add(1, Ordering::Relaxed);
                            }
                        } else {
                            s.stats.records_corrupted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                if read_len > 0 {
                    let mut buffer = s.log_buffer.lock().unwrap();
                    buffer.decommit(read_len);
                }

                if !http_header_map.is_empty() {
                    if let Ok(response) = client.head(telemetry_url.clone()).headers(http_header_map).send().await {
                        if response.status().is_success() {
                            s.stats.records_transmitted.fetch_add(1, Ordering::Relaxed);
                        } else {
                            debug!(
                                "Failed to transmit telemetry to {}: HTTP status {}",
                                telemetry_url,
                                response.status()
                            );
                            s.stats.records_dropped.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        debug!("Failed to send HEAD request to {}: Error occurred during transmission", telemetry_url);
                        s.stats.records_dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
                debug!("Stats from telemetry {:?}", s.stats);
            }
        };

        // Spawn the background telemetry task on it's own tokio runtime on the current thread; that way it will remain
        // isolated and not exist in a limbo state through spawns.  We can cleanly restart it in the child
        // process.

        // Create a oneshot token to send back the result of starting the runtime.
        let (rt_status_sender, rt_status) = tokio::sync::oneshot::channel();

        std::thread::spawn(move || {
            // Get the single threaded runtime to simply poll the log buffers and send them to python.
            match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                Ok(rt) => {
                    // Okay, runtime started successfully, start the telemetry send task.
                    if rt_status_sender.send(Ok(())).is_err() {
                        eprintln!("Error in reporting ok logging status; pipe closed");
                    }

                    // Now have this runtime simply run the telemetry task, which should just run in a loop.  This
                    rt.block_on(telemetry_send_task);
                },
                Err(e) => {
                    if let Err(e) = rt_status_sender.send(Err(MultithreadedRuntimeError::Other(format!(
                        "Initialization Error: Failed to create single threaded runtime for telemetry task {e:?}"
                    )))) {
                        eprintln!("Error in reporting Err logging status; pipe closed ({e:?})");
                    }
                },
            };
        });

        rt_status.blocking_recv().map_err(|e| {
            MultithreadedRuntimeError::Other(format!(
                "Initialization Error: Failed to connect with telemetry background thread: {e:?}"
            ))
        })?
    }
}

impl<S> Layer<S> for TelemetryLoggerPtr
where
    S: Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let tl = &self.0;

        let mut http_headers = HeaderMap::new();
        {
            let mut user_agent = tl.version_info.clone();
            let mut visitor = |field: &tracing::field::Field, value: &dyn std::fmt::Debug| {
                user_agent.push_str(&format!("{}/{:?}; ", field.name(), value));
            };
            event.record(&mut visitor);
            user_agent = user_agent.replace("\"", "");
            if let Ok(header_value) = HeaderValue::from_str(&user_agent) {
                http_headers.insert("User-Agent", header_value);
            } else {
                tl.stats.records_refused.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        let serializable: SerializableHeaders = (&http_headers).into();
        if let Ok(serialized_headers) = serde_json::to_string(&serializable) {
            let mut buffer = tl.log_buffer.lock().unwrap();
            if let Ok(reserved) = buffer.reserve(serialized_headers.len()) {
                if reserved.len() < serialized_headers.len() {
                    // log goes to /dev/null if not enough free space
                    tl.stats.records_refused.fetch_add(1, Ordering::Relaxed);
                    tl.stats
                        .bytes_refused
                        .fetch_add(serialized_headers.len() as u64, Ordering::Relaxed);
                    buffer.commit(0);
                } else {
                    tl.stats.records_written.fetch_add(1, Ordering::Relaxed);
                    tl.stats
                        .bytes_written
                        .fetch_add(serialized_headers.len() as u64, Ordering::Relaxed);
                    reserved[..serialized_headers.len()].copy_from_slice(serialized_headers.as_bytes());
                    buffer.commit(serialized_headers.len());
                }
            } else {
                tl.stats.records_refused.fetch_add(1, Ordering::Relaxed);
                tl.stats
                    .bytes_refused
                    .fetch_add(serialized_headers.len() as u64, Ordering::Relaxed);
            }
        } else {
            tl.stats.records_refused.fetch_add(1, Ordering::Relaxed);
        }
    }
}

lazy_static::lazy_static! {
    static ref global_telemetry_logger_info : OnceLock<Option<TelemetryLoggerPtr>> = OnceLock::default();
}

/// Restarts the telemetry background task after a spawn has been detected.
pub fn restart_telemetry_task_after_spawn() -> Result<(), MultithreadedRuntimeError> {
    if let Some(ref current_tl) = global_telemetry_logger_info.get_or_init(|| None) {
        current_tl.0.spawn_telemetry_task()?;
    }

    Ok(())
}

/// Initializes the telemetry logging; should be called only once.
pub fn init_telemetry_logging(version_info: String) -> Result<TelemetryLoggerPtr, MultithreadedRuntimeError> {
    let mut maybe_error = None;

    let tl = global_telemetry_logger_info.get_or_init(|| match TelemetryLogger::init(version_info) {
        Err(e) => {
            maybe_error = Some(e);
            None
        },
        Ok(tl) => Some(tl),
    });

    if let Some(e) = maybe_error {
        Err(e)
    } else {
        Ok(tl.clone().expect("Only None if no error."))
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
        let layer = TelemetryLoggerPtr(Arc::new(TelemetryLogger {
            log_buffer: Mutex::new(BipBuffer::new(50 * 2)),
            stats: LoggingStats::default(),
            version_info: "Testing".to_owned(),
        }));

        let subscriber = tracing_subscriber::registry().with(layer.clone());
        tracing::subscriber::with_default(subscriber, || {
            let stats = &layer.0.stats;

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
