use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use js_sys::Reflect;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_with_wasm::alias as tokio;
use tracing::info;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::spawn_local;
use web_time::Instant;

use crate::utils::TemplatedPathBuf;

/// Browser-side resource monitor. Same surface as the native version, but
/// the metric set is whatever the JS host actually exposes: see [`Metrics`].
///
/// Sampling runs on the current thread via `wasm_bindgen_futures::spawn_local`
/// and `tokio_with_wasm::time::sleep`, since wasm has no OS threads.
/// `log_path` is accepted for API parity with the native variant and is ignored.
#[derive(Debug)]
pub struct SystemMonitor {
    sample_interval: Duration,
    // `log_path` is accepted by the constructor for native API parity but
    // unused on wasm (output always goes through `tracing::info!`). The
    // PhantomData marker makes the parity intent explicit.
    _log_path: PhantomData<TemplatedPathBuf>,
    stop: Arc<AtomicBool>,
    running: Arc<AtomicBool>,
}

/// Errors that can occur during system monitoring on wasm.
#[derive(Error, Debug)]
pub enum SystemMonitorError {
    #[error("No JS global scope available (neither Window nor WorkerGlobalScope)")]
    NoGlobalScope,

    #[error("Serde Json error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}

type Result<T> = std::result::Result<T, SystemMonitorError>;

impl SystemMonitor {
    /// Creates and immediately starts a monitor for the current JS context.
    ///
    /// On wasm there is no notion of a process; `pid` arguments accepted by
    /// the native API are absent here. `log_path` is accepted for API
    /// parity with the native variant and is ignored — output is always
    /// emitted via `tracing::info!(system_usage = json)`.
    pub fn follow_process(sample_interval: Duration, _log_path: Option<TemplatedPathBuf>) -> Result<Self> {
        // Probe the global scope eagerly so callers see configuration errors at
        // construction time rather than from the background loop.
        let _ = global_scope()?;

        let ret = Self {
            sample_interval,
            _log_path: PhantomData,
            stop: Arc::new(AtomicBool::new(false)),
            running: Arc::new(AtomicBool::new(false)),
        };

        ret.start()?;
        Ok(ret)
    }

    /// Starts the sampling loop. No-op if already running.
    pub fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.stop.store(false, Ordering::Relaxed);

        let mut sampler = Sampler::new();
        // Take an initial sample so we have a baseline immediately.
        sampler.sample();

        let stop = self.stop.clone();
        let running = self.running.clone();
        let sample_interval = self.sample_interval;

        spawn_local(async move {
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                tokio::time::sleep(sample_interval).await;
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                sampler.sample();
                if let Ok(json) = serde_json::to_string(&sampler.last_sample) {
                    info!(system_usage = json);
                }
            }
            running.store(false, Ordering::Relaxed);
        });

        Ok(())
    }

    /// Signals the sampling loop to stop. Returns immediately; the loop
    /// exits the next time it wakes from `sleep`.
    pub fn stop(&self) -> Result<()> {
        self.stop.store(true, Ordering::Relaxed);
        Ok(())
    }
}

impl Drop for SystemMonitor {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// A snapshot of metrics the browser exposes to JS. Every field is optional
/// because browser coverage is uneven: `performance.memory` is Chrome-only,
/// `navigator.deviceMemory` and `navigator.connection` are Chromium-only.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct Metrics {
    /// Seconds since this monitor was constructed.
    run_time: u64,
    cpu: CpuUsage,
    memory: MemoryUsage,
    network: NetworkUsage,
    /// `navigator.userAgent`, useful for correlating logs to a browser/version.
    user_agent: Option<String>,
}

/// CPU-related info available to JS. The browser does not expose per-process
/// CPU% for the running tab; `hardware_concurrency` is the only signal.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct CpuUsage {
    /// `navigator.hardwareConcurrency` — logical processor count.
    hardware_concurrency: Option<u32>,
}

/// Memory-related info available to JS. `performance.memory.*` is Chrome-only.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct MemoryUsage {
    /// `performance.memory.usedJSHeapSize`.
    used_js_heap_bytes: Option<u64>,
    /// `performance.memory.totalJSHeapSize`.
    total_js_heap_bytes: Option<u64>,
    /// `performance.memory.jsHeapSizeLimit`.
    js_heap_size_limit_bytes: Option<u64>,
    /// Peak `usedJSHeapSize` observed since the monitor started.
    peak_used_js_heap_bytes: u64,
    /// `navigator.deviceMemory` — rough RAM bucket in GiB (Chromium-only).
    device_memory_gib: Option<f64>,
}

/// Network info from the NetworkInformation API (`navigator.connection`).
/// Chromium-only. Values are best-effort *hints*, not measurements.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct NetworkUsage {
    /// `effectiveType` — one of "slow-2g", "2g", "3g", "4g".
    effective_type: Option<String>,
    /// `downlink` — estimated downlink Mbps, rounded to nearest 25.
    downlink_mbps: Option<f64>,
    /// `rtt` — estimated round-trip in ms, rounded to nearest 25.
    rtt_ms: Option<f64>,
    /// `saveData` — user has requested reduced data usage.
    save_data: Option<bool>,
}

#[derive(Debug)]
struct Sampler {
    start_instant: Instant,
    peak_used_js_heap_bytes: u64,
    last_sample: Metrics,
}

impl Sampler {
    fn new() -> Self {
        Self {
            start_instant: Instant::now(),
            peak_used_js_heap_bytes: 0,
            last_sample: Metrics::default(),
        }
    }

    fn sample(&mut self) {
        let global = match global_scope() {
            Ok(g) => g,
            Err(_) => return,
        };

        let navigator = read_object(&global, "navigator");
        let performance = read_object(&global, "performance");
        let connection = navigator.as_ref().and_then(|n| read_object(n, "connection"));
        let perf_memory = performance.as_ref().and_then(|p| read_object(p, "memory"));

        let used_js_heap = perf_memory
            .as_ref()
            .and_then(|m| read_f64(m, "usedJSHeapSize"))
            .map(|f| f as u64);
        if let Some(used) = used_js_heap
            && used > self.peak_used_js_heap_bytes
        {
            self.peak_used_js_heap_bytes = used;
        }

        self.last_sample = Metrics {
            run_time: self.start_instant.elapsed().as_secs(),
            cpu: CpuUsage {
                hardware_concurrency: navigator
                    .as_ref()
                    .and_then(|n| read_f64(n, "hardwareConcurrency"))
                    .map(|f| f as u32),
            },
            memory: MemoryUsage {
                used_js_heap_bytes: used_js_heap,
                total_js_heap_bytes: perf_memory
                    .as_ref()
                    .and_then(|m| read_f64(m, "totalJSHeapSize"))
                    .map(|f| f as u64),
                js_heap_size_limit_bytes: perf_memory
                    .as_ref()
                    .and_then(|m| read_f64(m, "jsHeapSizeLimit"))
                    .map(|f| f as u64),
                peak_used_js_heap_bytes: self.peak_used_js_heap_bytes,
                device_memory_gib: navigator.as_ref().and_then(|n| read_f64(n, "deviceMemory")),
            },
            network: NetworkUsage {
                effective_type: connection.as_ref().and_then(|c| read_string(c, "effectiveType")),
                downlink_mbps: connection.as_ref().and_then(|c| read_f64(c, "downlink")),
                rtt_ms: connection.as_ref().and_then(|c| read_f64(c, "rtt")),
                save_data: connection.as_ref().and_then(|c| read_bool(c, "saveData")),
            },
            user_agent: navigator.as_ref().and_then(|n| read_string(n, "userAgent")),
        };
    }
}

/// Returns the JS global object (`globalThis`), which resolves to the Window
/// on the main thread and to a WorkerGlobalScope inside a Web Worker.
fn global_scope() -> Result<JsValue> {
    let global: JsValue = js_sys::global().into();
    if global.is_undefined() || global.is_null() {
        return Err(SystemMonitorError::NoGlobalScope);
    }
    Ok(global)
}

fn read_object(target: &JsValue, key: &str) -> Option<JsValue> {
    let v = Reflect::get(target, &JsValue::from_str(key)).ok()?;
    if v.is_undefined() || v.is_null() { None } else { Some(v) }
}

fn read_f64(target: &JsValue, key: &str) -> Option<f64> {
    Reflect::get(target, &JsValue::from_str(key)).ok()?.as_f64()
}

fn read_string(target: &JsValue, key: &str) -> Option<String> {
    Reflect::get(target, &JsValue::from_str(key)).ok()?.as_string()
}

fn read_bool(target: &JsValue, key: &str) -> Option<bool> {
    Reflect::get(target, &JsValue::from_str(key)).ok()?.as_bool()
}
