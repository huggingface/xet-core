//! Client-side transfer performance telemetry.
//!
//! One [`TransferTelemetry`] exists per transfer: [`RemoteClient`](crate::cas_client::RemoteClient)
//! builds one at construction, and `create_remote_client` builds a `RemoteClient` once per session
//! *and* per direction, so that scope is exactly right.
//!
//! This module owns identity, timing, and delivery. It does **not** own the metric definitions -
//! those live in `xet_data`, which is the only crate that can see `DeduplicationMetrics` and
//! `GroupProgressReport`. `xet_data` reads identity off this struct, builds the flat metrics
//! object, and hands it back to [`TransferTelemetry::emit_terminal`].
//!
//! Compiled out on wasm: [`XetRuntime`](xet_runtime::core::XetRuntime) has no `spawn` there, so
//! there is no way to send without blocking a transfer.

mod envelope;
mod sink;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use http::HeaderMap;
use http::header::USER_AGENT;
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use tokio::task::JoinHandle;
use tracing::debug;
use uuid::Uuid;
use xet_runtime::core::XetContext;

pub use self::envelope::TelemetryEnvelope;
use self::sink::{LOG_TARGET, TelemetrySink};

/// Fallback when the caller supplied no `User-Agent`. Real hf-xet traffic always carries one
/// (built in `hf_xet/src/headers.rs`), so this mostly shows up for `xtool` and tests.
const DEFAULT_USER_AGENT: &str = concat!("xet-client/", env!("CARGO_PKG_VERSION"));

/// Which half of the transfer a document describes.
///
/// Deliberately *not* stored on [`TransferTelemetry`]. `RemoteClient` has no notion of direction -
/// it is constructed identically for both - and threading one through would touch every one of its
/// construction sites for the benefit of two. Instead `xet_data`, which knows whether it holds an
/// upload or a download session, supplies the direction when it builds the payload.
///
/// It is carried as a metric in its own right because a single `XetSession` id can cover both an
/// upload commit and a download group; `direction` plus `transfer_id` are what separate them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Upload,
    Download,
}

impl Direction {
    /// Value of the `direction` metric.
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Upload => "upload",
            Direction::Download => "download",
        }
    }

    /// Value of the envelope's `event` field for this direction's terminal document.
    pub fn terminal_event(self) -> &'static str {
        match self {
            Direction::Upload => "xet_upload_summary",
            Direction::Download => "xet_download_summary",
        }
    }
}

/// Event name for non-terminal progress documents. Direction is carried in the metrics rather
/// than the event name so heartbeats have one shape regardless of direction.
pub const HEARTBEAT_EVENT: &str = "xet_transfer_heartbeat";

/// Per-transfer telemetry state and delivery.
pub struct TransferTelemetry {
    session_id: String,
    user_agent: String,
    /// Host component only - never a full URL, which could carry a path or query.
    endpoint_host: String,
    transfer_id: String,
    started_at: Instant,
    /// Highest concurrency observed, via `fetch_max` from the permit acquisition path.
    peak_concurrency: AtomicU64,
    /// Set by whichever of `finalize` or `Drop` gets there first, so exactly one terminal
    /// document is emitted per transfer.
    terminal_sent: AtomicBool,
    sink: TelemetrySink,
    final_flush_timeout: Duration,
    /// Handle to the heartbeat task, aborted when the terminal document goes out.
    heartbeat: Mutex<Option<JoinHandle<()>>>,
    heartbeat_after: Duration,
    heartbeat_interval: Duration,
}

impl TransferTelemetry {
    /// Builds a telemetry aggregator, or `None` when telemetry should not run at all.
    ///
    /// Returns `None` for: telemetry disabled by config, dry-run, and any endpoint that is not
    /// http/https (which covers `local://` and `memory://`, though in practice those never reach
    /// `RemoteClient` at all).
    pub(crate) fn maybe_new(
        ctx: &XetContext,
        endpoint: &str,
        session_id: &str,
        dry_run: bool,
        http: Arc<ClientWithMiddleware>,
        custom_headers: Option<&HeaderMap>,
    ) -> Option<Arc<Self>> {
        if !ctx.config.telemetry.enabled {
            return None;
        }
        if dry_run {
            return None;
        }

        let base = Url::parse(endpoint).ok()?;
        if !matches!(base.scheme(), "http" | "https") {
            return None;
        }
        let endpoint_host = base.host_str()?.to_owned();
        // Absolute path: `join` on a base with a path would otherwise resolve relative to it.
        let url = base.join("/v1/telemetry").ok()?;

        let user_agent = custom_headers
            .and_then(|h| h.get(USER_AGENT))
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT_USER_AGENT)
            .to_owned();

        Some(Arc::new(Self {
            session_id: session_id.to_owned(),
            user_agent,
            endpoint_host,
            transfer_id: Uuid::now_v7().to_string(),
            started_at: Instant::now(),
            peak_concurrency: AtomicU64::new(0),
            terminal_sent: AtomicBool::new(false),
            sink: TelemetrySink::new(ctx, url, http),
            final_flush_timeout: ctx.config.telemetry.final_flush_timeout,
            heartbeat: Mutex::new(None),
            heartbeat_after: ctx.config.telemetry.heartbeat_after,
            heartbeat_interval: ctx.config.telemetry.heartbeat_interval,
        }))
    }

    /// Starts emitting periodic progress documents once this transfer passes `heartbeat_after`.
    ///
    /// `snapshot` builds the metrics for one heartbeat from `session`, or returns `None` to skip
    /// this beat and try again at the next interval. Skipping must stay cheap: metrics are read
    /// with `try_lock` so a heartbeat never blocks a transfer, and losing that race is routine on
    /// exactly the long transfers this exists for. A skip costs one document, nothing more - the
    /// sequence number is not consumed either, so `seq` stays dense across the documents that do
    /// arrive.
    ///
    /// Ending the task is this loop's decision, not the closure's: `session` is held weakly here
    /// and the task returns once it is gone. That is also why the closure takes `&S` rather than
    /// capturing it - a strong capture would keep the session alive and its `Drop`-based terminal
    /// report would never fire.
    ///
    /// Short transfers - the overwhelming majority - never emit a heartbeat at all. The task
    /// itself is skipped entirely when `heartbeat_after` is zero.
    pub fn start_heartbeat<S, F>(self: &Arc<Self>, ctx: &XetContext, session: &Arc<S>, snapshot: F)
    where
        S: Send + Sync + 'static,
        F: Fn(&S, u64) -> Option<serde_json::Value> + Send + Sync + 'static,
    {
        if self.heartbeat_after.is_zero() {
            return;
        }

        // Weak, so the task cannot keep either of these alive past the transfer.
        let weak = Arc::downgrade(self);
        let weak_session = Arc::downgrade(session);
        let (after, interval) = (self.heartbeat_after, self.heartbeat_interval);

        let handle = ctx.runtime.spawn(async move {
            tokio::time::sleep(after).await;

            let mut seq = 1;
            loop {
                let (Some(telemetry), Some(session)) = (weak.upgrade(), weak_session.upgrade()) else {
                    return;
                };
                if telemetry.terminal_sent() {
                    return;
                }
                if let Some(metrics) = snapshot(&session, seq) {
                    telemetry.emit_heartbeat(metrics);
                    seq += 1;
                } else {
                    debug!(target: LOG_TARGET, transfer_id = %telemetry.transfer_id, seq, "skipping heartbeat");
                }
                // Dropped before sleeping so neither the transfer nor the session is held alive by
                // this task while it waits out the interval.
                drop((telemetry, session));

                tokio::time::sleep(interval).await;
            }
        });

        *self.heartbeat.lock().expect("telemetry heartbeat lock poisoned") = Some(handle);
    }

    /// Stops the heartbeat task, if one is running.
    fn stop_heartbeat(&self) {
        if let Ok(mut guard) = self.heartbeat.lock()
            && let Some(handle) = guard.take()
        {
            handle.abort();
        }
    }

    pub fn transfer_id(&self) -> &str {
        &self.transfer_id
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn endpoint_host(&self) -> &str {
        &self.endpoint_host
    }

    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    pub fn peak_concurrency(&self) -> u64 {
        self.peak_concurrency.load(Ordering::Relaxed)
    }

    /// Records an observed concurrency level, keeping the maximum.
    pub fn record_concurrency(&self, concurrency: usize) {
        self.peak_concurrency.fetch_max(concurrency as u64, Ordering::Relaxed);
    }

    /// Whether a terminal document has already been emitted.
    pub fn terminal_sent(&self) -> bool {
        self.terminal_sent.load(Ordering::Acquire)
    }

    /// Sends the terminal document, waiting up to `final_flush_timeout`.
    ///
    /// Pass [`Direction::terminal_event`] for `event`. No-ops if a terminal document was already
    /// sent, so a session that finalizes normally and is then dropped emits exactly one.
    pub async fn emit_terminal(&self, event: &'static str, metrics: serde_json::Value) {
        if self.terminal_sent.swap(true, Ordering::AcqRel) {
            return;
        }
        self.stop_heartbeat();
        let envelope = self.envelope(event, metrics);
        self.sink.submit_awaited(envelope, self.final_flush_timeout).await;
    }

    /// Sends the terminal document without waiting.
    ///
    /// For `Drop`, which is synchronous and cannot await. Delivery is materially less likely here
    /// than on the [`emit_terminal`](Self::emit_terminal) path - accepted, because the alternative
    /// is no visibility at all into aborted transfers.
    pub fn emit_terminal_detached(&self, event: &'static str, metrics: serde_json::Value) {
        if self.terminal_sent.swap(true, Ordering::AcqRel) {
            return;
        }
        self.stop_heartbeat();
        let envelope = self.envelope(event, metrics);
        self.sink.submit_detached(envelope);
    }

    /// Sends a non-terminal heartbeat, without waiting.
    ///
    /// Skipped once a terminal document has gone out, so a heartbeat racing with finalization
    /// cannot arrive after the summary.
    pub fn emit_heartbeat(&self, metrics: serde_json::Value) {
        if self.terminal_sent() {
            return;
        }
        let envelope = self.envelope(HEARTBEAT_EVENT, metrics);
        self.sink.submit_detached(envelope);
    }

    fn envelope(&self, event: &'static str, metrics: serde_json::Value) -> TelemetryEnvelope {
        debug!(target: LOG_TARGET, event, transfer_id = %self.transfer_id, "emitting telemetry");
        TelemetryEnvelope::new(event, self.session_id.clone(), self.user_agent.clone(), metrics)
    }
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;
    use xet_runtime::config::XetConfig;
    use xet_runtime::core::XetContext;

    use super::*;

    fn ctx_with(enabled: bool) -> XetContext {
        let mut config = XetConfig::default();
        config.telemetry.enabled = enabled;
        XetContext::with_config(config).unwrap()
    }

    fn http(ctx: &XetContext) -> Arc<ClientWithMiddleware> {
        Arc::new(crate::common::http_client::build_http_client(ctx, "sess", None, None).unwrap())
    }

    fn build(ctx: &XetContext, endpoint: &str, dry_run: bool) -> Option<Arc<TransferTelemetry>> {
        TransferTelemetry::maybe_new(ctx, endpoint, "sess-1", dry_run, http(ctx), None)
    }

    #[test]
    fn test_built_for_a_plain_https_endpoint() {
        let ctx = ctx_with(true);
        let t = build(&ctx, "https://cas.example.com", false).expect("should build");
        assert_eq!(t.endpoint_host(), "cas.example.com");
        assert!(!t.transfer_id().is_empty());
    }

    #[test]
    fn test_not_built_when_disabled() {
        let ctx = ctx_with(false);
        assert!(build(&ctx, "https://cas.example.com", false).is_none());
    }

    #[test]
    fn test_not_built_for_dry_run() {
        let ctx = ctx_with(true);
        assert!(build(&ctx, "https://cas.example.com", true).is_none());
    }

    #[test]
    fn test_not_built_for_non_http_endpoints() {
        let ctx = ctx_with(true);
        assert!(build(&ctx, "local:///tmp/cas", false).is_none());
        assert!(build(&ctx, "memory://", false).is_none());
        assert!(build(&ctx, "not a url", false).is_none());
    }

    /// The host must not carry a scheme, port, path, or query - those can be sensitive and are
    /// useless for grouping.
    #[test]
    fn test_endpoint_host_strips_everything_else() {
        let ctx = ctx_with(true);
        let t = build(&ctx, "https://cas.example.com:8443/v1/base?token=secret", false).unwrap();
        assert_eq!(t.endpoint_host(), "cas.example.com");
    }

    /// A base URL with a path must still produce `/v1/telemetry` at the root, not relative to it.
    #[test]
    fn test_telemetry_path_is_absolute() {
        let base = Url::parse("https://cas.example.com/some/base/").unwrap();
        assert_eq!(base.join("/v1/telemetry").unwrap().as_str(), "https://cas.example.com/v1/telemetry");
    }

    #[test]
    fn test_user_agent_comes_from_custom_headers() {
        let ctx = ctx_with(true);
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static("hf_xet/9.9.9"));
        let t =
            TransferTelemetry::maybe_new(&ctx, "https://cas.example.com", "sess-1", false, http(&ctx), Some(&headers))
                .unwrap();
        assert_eq!(t.user_agent, "hf_xet/9.9.9");
    }

    #[test]
    fn test_user_agent_falls_back_when_absent_or_empty() {
        let ctx = ctx_with(true);
        assert_eq!(build(&ctx, "https://cas.example.com", false).unwrap().user_agent, DEFAULT_USER_AGENT);

        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(""));
        let t = TransferTelemetry::maybe_new(&ctx, "https://cas.example.com", "s", false, http(&ctx), Some(&headers))
            .unwrap();
        assert_eq!(t.user_agent, DEFAULT_USER_AGENT);
    }

    #[test]
    fn test_peak_concurrency_keeps_the_maximum() {
        let ctx = ctx_with(true);
        let t = build(&ctx, "https://cas.example.com", false).unwrap();
        assert_eq!(t.peak_concurrency(), 0);
        t.record_concurrency(4);
        t.record_concurrency(9);
        t.record_concurrency(2);
        assert_eq!(t.peak_concurrency(), 9);
    }

    #[test]
    fn test_transfer_ids_are_unique_across_transfers() {
        let ctx = ctx_with(true);
        let a = build(&ctx, "https://cas.example.com", false).unwrap();
        let b = build(&ctx, "https://cas.example.com", false).unwrap();
        assert_ne!(a.transfer_id(), b.transfer_id());
    }

    /// A zero `heartbeat_after` means no task is spawned at all.
    #[test]
    fn test_heartbeat_disabled_when_after_is_zero() {
        let mut config = XetConfig::default();
        config.telemetry.heartbeat_after = Duration::ZERO;
        let ctx = XetContext::with_config(config).unwrap();

        let t = TransferTelemetry::maybe_new(&ctx, "https://cas.example.com", "s", false, http(&ctx), None).unwrap();
        t.start_heartbeat(&ctx, &Arc::new(()), |_, _| Some(serde_json::json!({})));

        assert!(t.heartbeat.lock().unwrap().is_none(), "no task should have been spawned");
    }

    /// A snapshot that cannot be taken *right now* must cost one beat and no more. Metrics are
    /// read with `try_lock`, so losing that race is routine on exactly the long transfers a
    /// heartbeat exists for - treating it as session death would silence the rest of the transfer.
    #[test]
    fn test_a_skipped_snapshot_does_not_end_the_heartbeat() {
        let mut config = XetConfig::default();
        config.telemetry.heartbeat_after = Duration::from_millis(5);
        config.telemetry.heartbeat_interval = Duration::from_millis(5);
        let ctx = XetContext::with_config(config).unwrap();
        let t = TransferTelemetry::maybe_new(&ctx, "https://cas.example.com", "s", false, http(&ctx), None).unwrap();

        let calls = Arc::new(AtomicU64::new(0));
        let highest_seq = Arc::new(AtomicU64::new(0));
        let (seen, seqs) = (Arc::clone(&calls), Arc::clone(&highest_seq));
        // Held for the whole test: a dropped session ends the task for a legitimate reason, which
        // would make the assertion below pass or fail for the wrong one.
        let live_session = Arc::new(());
        t.start_heartbeat(&ctx, &live_session, move |_, seq| {
            seen.fetch_add(1, Ordering::Relaxed);
            seqs.fetch_max(seq, Ordering::Relaxed);
            None
        });

        let observed = ctx
            .runtime
            .external_run_async_task(async move {
                let start = tokio::time::Instant::now();
                while calls.load(Ordering::Relaxed) < 3 && start.elapsed() < Duration::from_secs(5) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                calls.load(Ordering::Relaxed)
            })
            .unwrap();

        assert!(observed >= 3, "the task stopped after {observed} skipped snapshot(s)");
        // Skips consume no sequence number, so `seq` is dense across the documents that do arrive.
        assert_eq!(highest_seq.load(Ordering::Relaxed), 1);
    }

    /// The heartbeat task must not keep the transfer or its session alive. If it held a strong
    /// reference to either, the session's `Drop`-based terminal report would never fire.
    #[test]
    fn test_heartbeat_holds_only_weak_references() {
        let ctx = ctx_with(true);
        let t = build(&ctx, "https://cas.example.com", false).unwrap();
        let s = Arc::new(());
        t.start_heartbeat(&ctx, &s, |_, _| Some(serde_json::json!({})));

        let (weak_t, weak_s) = (Arc::downgrade(&t), Arc::downgrade(&s));
        drop((t, s));
        assert!(weak_t.upgrade().is_none(), "heartbeat task is keeping the telemetry alive");
        assert!(weak_s.upgrade().is_none(), "heartbeat task is keeping the session alive");
    }

    /// Emitting the terminal document stops the heartbeat, so no progress document can arrive
    /// after the summary.
    #[test]
    fn test_terminal_stops_the_heartbeat() {
        let ctx = ctx_with(true);
        let t = build(&ctx, "https://cas.example.com", false).unwrap();
        t.start_heartbeat(&ctx, &Arc::new(()), |_, _| Some(serde_json::json!({})));
        assert!(t.heartbeat.lock().unwrap().is_some());

        t.emit_terminal_detached(Direction::Upload.terminal_event(), serde_json::json!({}));
        assert!(t.heartbeat.lock().unwrap().is_none(), "terminal emit should have aborted the heartbeat");
    }

    /// A heartbeat after the summary would be indistinguishable from a stale document.
    #[test]
    fn test_heartbeat_suppressed_after_terminal() {
        let ctx = ctx_with(true);
        let t = build(&ctx, "https://cas.example.com", false).unwrap();
        t.emit_terminal_detached(Direction::Upload.terminal_event(), serde_json::json!({}));

        // No panic and no send; the guard is `terminal_sent`.
        t.emit_heartbeat(serde_json::json!({}));
        assert!(t.terminal_sent());
    }

    /// Only one terminal document per transfer, whichever path gets there first.
    #[test]
    fn test_terminal_emits_only_once() {
        let ctx = ctx_with(true);
        let t = build(&ctx, "https://cas.example.com", false).unwrap();
        assert!(!t.terminal_sent());

        t.emit_terminal_detached(Direction::Upload.terminal_event(), serde_json::json!({}));
        assert!(t.terminal_sent());

        // A second attempt is suppressed rather than producing a duplicate.
        t.emit_terminal_detached(Direction::Upload.terminal_event(), serde_json::json!({}));
        assert!(t.terminal_sent());
    }
}
