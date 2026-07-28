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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use http::HeaderMap;
use http::header::USER_AGENT;
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
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
    dry_run: bool,
    started_at: Instant,
    /// Highest concurrency observed, via `fetch_max` from the permit acquisition path.
    peak_concurrency: AtomicU64,
    /// Set by whichever of `finalize` or `Drop` gets there first, so exactly one terminal
    /// document is emitted per transfer.
    terminal_sent: AtomicBool,
    sink: TelemetrySink,
    final_flush_timeout: Duration,
}

impl TransferTelemetry {
    /// Builds a telemetry aggregator, or `None` when telemetry should not run at all.
    ///
    /// Returns `None` for: telemetry disabled by config or by the shared `HF_HUB_*` opt-outs,
    /// dry-run, and any endpoint that is not http/https (which covers `local://` and `memory://`,
    /// though in practice those never reach `RemoteClient` at all).
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
            dry_run,
            started_at: Instant::now(),
            peak_concurrency: AtomicU64::new(0),
            terminal_sent: AtomicBool::new(false),
            sink: TelemetrySink::new(ctx, url, http),
            final_flush_timeout: ctx.config.telemetry.final_flush_timeout,
        }))
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

    pub fn dry_run(&self) -> bool {
        self.dry_run
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
