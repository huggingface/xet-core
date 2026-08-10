use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use http::header::CONTENT_TYPE;
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use tracing::{debug, info};
use xet_runtime::core::XetContext;

use super::envelope::TelemetryEnvelope;
use crate::common::http_client::Api;

/// Log target for every message this module emits. Telemetry problems are never the user's
/// problem, so they are DEBUG-only and tagged for easy filtering.
pub(crate) const LOG_TARGET: &str = "xet_telemetry";

/// Tag attached to the outgoing request so it is distinguishable from real CAS traffic in
/// `LoggingMiddleware` output, and so future per-API counters can exclude it from their own
/// accounting.
const API_TAG: &str = "cas::telemetry";

/// Telemetry requests in flight across the whole process.
///
/// Deliberately global rather than per-sink. A sink belongs to one `TransferTelemetry`, i.e. one
/// transfer, so a per-sink counter bounds a single long transfer's heartbeats but not the aggregate:
/// a snapshot download fans out into many concurrent per-file transfers, each with its own sink, so
/// the process-wide total would be `max_in_flight × concurrent transfers` with no ceiling at all.
/// That put the backpressure in the wrong place - the heaviest telemetry moment, a wide fan-out, was
/// exactly the one with no limit.
///
/// One counter for the process means [`max_in_flight`](xet_runtime::config::TelemetryConfig) is a
/// real ceiling. Its default is sized for that: as a per-transfer number it would be far too large,
/// and the old per-transfer default would be far too small here, since a wide snapshot finalizing at
/// once would shed most of its terminal documents.
static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

/// Budget for [`flush_pending_telemetry`] when it runs from the runtime's pre-shutdown hook,
/// mirrored from `final_flush_timeout` whenever a sink is built.
///
/// Parked in a static because `XetRuntime`'s `Drop` is where the drain has to happen and it holds no
/// config. Zero disables the drain, which is the right reading of `final_flush_timeout = 0`: that
/// setting means "do not let telemetry delay anything".
static FLUSH_TIMEOUT_MS: AtomicU64 = AtomicU64::new(0);

/// Waits for outstanding telemetry POSTs to finish, up to `timeout`. Returns whether they drained.
///
/// Needed because [`submit_detached`](TelemetrySink::submit_detached) is fire-and-forget and runtime
/// shutdown *cancels* pending tasks rather than completing them. Measured against a CAS endpoint,
/// the detached terminal document arrived 0 times out of 8 once the POST took ~50ms, and adding a
/// single scheduler yield was enough to lose it against loopback - so without this, every abandoned
/// transfer's document and every heartbeat is lost in practice.
///
/// Sleep-polls rather than using a `Notify` or condvar: this runs once, at teardown, and the
/// alternative would put signalling on the guard-drop path for no benefit. 2ms granularity is
/// irrelevant against a network round trip.
///
/// Callable from any thread and needs no async context, so it also suits an embedder that wants to
/// flush without tearing its runtime down.
pub fn flush_pending_telemetry(timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if IN_FLIGHT.load(Ordering::Acquire) == 0 {
            return true;
        }
        if Instant::now() >= deadline {
            debug!(target: LOG_TARGET, "telemetry flush gave up with {} request(s) still in flight", IN_FLIGHT.load(Ordering::Acquire));
            return false;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// The hook the runtime calls before shutting down. Reads its budget from [`FLUSH_TIMEOUT_MS`].
fn drain_before_runtime_shutdown() {
    let ms = FLUSH_TIMEOUT_MS.load(Ordering::Relaxed);
    if ms == 0 {
        return;
    }
    flush_pending_telemetry(Duration::from_millis(ms));
}

/// Posts telemetry documents to `POST /v1/telemetry`.
///
/// Deliberately *not* built on [`RetryWrapper`](crate::cas_client::retry_wrapper::RetryWrapper):
/// telemetry must never retry. A 429 is the server shedding load and a 5xx means its ingestion
/// pipeline is unhappy - in both cases another attempt makes things worse, and a lost document
/// costs nothing.
///
/// The HTTP client is *cloned from* [`RemoteClient`](crate::cas_client::RemoteClient)'s
/// authenticated client rather than built fresh. Building a new one via `build_auth_http_client`
/// would construct a second `AuthMiddleware` with its own `TokenProvider`, giving telemetry an
/// independent token-refresh cycle against the Hub.
pub struct TelemetrySink {
    ctx: XetContext,
    url: Url,
    http: Arc<ClientWithMiddleware>,
    /// Backpressure. Documents submitted while this is at `max_in_flight` are dropped rather than
    /// queued, so a hanging endpoint cannot accumulate tasks.
    ///
    /// Always [`IN_FLIGHT`] in production - held as a field rather than referenced directly so tests
    /// can point a sink at an isolated counter instead of the process-wide one.
    in_flight: &'static AtomicUsize,
    max_in_flight: usize,
    request_timeout: Duration,
}

impl TelemetrySink {
    pub(crate) fn new(ctx: &XetContext, url: Url, http: Arc<ClientWithMiddleware>) -> Self {
        // Arm the drain. Done here rather than at some init entry point so it is impossible to have
        // a sink without it: registration is idempotent, and the budget tracks the live config.
        let flush = ctx.config.telemetry.final_flush_timeout;
        FLUSH_TIMEOUT_MS.store(u64::try_from(flush.as_millis()).unwrap_or(u64::MAX), Ordering::Relaxed);
        xet_runtime::core::register_pre_shutdown_drain(drain_before_runtime_shutdown);

        Self {
            ctx: ctx.clone(),
            url,
            http,
            in_flight: &IN_FLIGHT,
            max_in_flight: ctx.config.telemetry.max_in_flight,
            request_timeout: ctx.config.telemetry.request_timeout,
        }
    }

    /// Claims an in-flight slot, or `None` when the cap is reached.
    ///
    /// The cap is process-wide: the counter is shared by every sink, so this is where a wide fan-out
    /// of concurrent transfers gets bounded rather than multiplying.
    fn acquire_slot(&self) -> Option<InFlightGuard> {
        let max = self.max_in_flight;
        self.in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| (n < max).then_some(n + 1))
            .ok()?;
        Some(InFlightGuard(self.in_flight))
    }

    /// Sends without waiting. Used for heartbeats and for terminal documents emitted from `Drop`,
    /// where there is nothing to await on.
    ///
    /// If the runtime is already shutting down the spawned task simply never runs. That is
    /// acceptable: this path is best-effort by construction.
    pub fn submit_detached(&self, envelope: TelemetryEnvelope) {
        let Some(guard) = self.acquire_slot() else {
            debug!(target: LOG_TARGET, event = envelope.event, "dropping telemetry: {} requests already in flight", self.max_in_flight);
            return;
        };

        let (url, http, timeout) = (self.url.clone(), self.http.clone(), self.request_timeout);
        // Detached on purpose: dropping the JoinHandle leaves the task running, and nothing may
        // await it. Dropped explicitly rather than with `let _ =`, which clippy flags for futures.
        drop(self.ctx.runtime.spawn(async move {
            send(&http, &url, &envelope, timeout).await;
            drop(guard);
        }));
    }

    /// Sends and waits up to `budget` for the result.
    ///
    /// Used only for the terminal document. A fully detached terminal send is usually lost,
    /// because host processes routinely exit within milliseconds of a transfer returning. The wait
    /// happens after all transfer work has finished, so it delays no data movement - but it does
    /// delay `finalize()`, which is why it is bounded and configurable (a `budget` of zero degrades
    /// to [`submit_detached`](Self::submit_detached)).
    pub async fn submit_awaited(&self, envelope: TelemetryEnvelope, budget: Duration) {
        if budget.is_zero() {
            self.submit_detached(envelope);
            return;
        }

        let Some(_guard) = self.acquire_slot() else {
            debug!(target: LOG_TARGET, event = envelope.event, "dropping telemetry: {} requests already in flight", self.max_in_flight);
            return;
        };

        // The budget bounds the wait, and request_timeout bounds the request; whichever is
        // shorter wins, and neither can surface an error to the caller.
        if tokio::time::timeout(budget, send(&self.http, &self.url, &envelope, self.request_timeout))
            .await
            .is_err()
        {
            debug!(target: LOG_TARGET, event = envelope.event, "telemetry flush exceeded its {budget:?} budget; abandoning");
        }
    }
}

/// Performs one POST. Swallows every failure; the return type is `()` on purpose so no caller can
/// accidentally propagate a telemetry error into a transfer.
async fn send(http: &ClientWithMiddleware, url: &Url, envelope: &TelemetryEnvelope, timeout: Duration) {
    // Serialized by hand rather than with `.json()`, which `reqwest-middleware` only exposes under
    // its `json` feature. Every field is a string or a flat scalar object, so failure here is not
    // reachable in practice - but it must not panic if it ever becomes reachable.
    let body = match serde_json::to_vec(envelope) {
        Ok(body) => body,
        Err(e) => {
            debug!(target: LOG_TARGET, event = envelope.event, error = %e, "telemetry payload failed to serialize; dropping");
            return;
        },
    };

    let request = http
        .post(url.clone())
        .with_extension(Api(API_TAG))
        .header(CONTENT_TYPE, "application/json")
        .body(body)
        .timeout(timeout);

    match request.send().await {
        Ok(response) if response.status().is_success() => {
            // The one line at `info!`: a client log should show that telemetry is on and landing.
            info!(target: LOG_TARGET, event = envelope.event, status = %response.status(), "telemetry accepted");
        },
        Ok(response) => {
            // Includes 429 (ingestion saturated) and 5xx (ingestion failing). Not retried.
            debug!(target: LOG_TARGET, event = envelope.event, status = %response.status(), "telemetry rejected; dropping");
        },
        Err(e) => {
            debug!(target: LOG_TARGET, event = envelope.event, error = %e, "telemetry send failed; dropping");
        },
    }
}

/// Releases an in-flight slot on drop, including when the task is cancelled mid-request.
struct InFlightGuard(&'static AtomicUsize);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    /// A fresh `&'static AtomicUsize` so a test never contends with the process-wide [`IN_FLIGHT`]
    /// counter, which is shared by every other test in this binary.
    fn counter(start: usize) -> &'static AtomicUsize {
        Box::leak(Box::new(AtomicUsize::new(start)))
    }

    /// A sink whose slots come from `counter` rather than the process-wide one, so the shared-budget
    /// behaviour can be exercised without other tests in this binary interfering. The URL and HTTP
    /// client are never exercised: these tests only call `acquire_slot`.
    fn sink_with_counter(ctx: &XetContext, counter: &'static AtomicUsize, max: usize) -> TelemetrySink {
        let http = Arc::new(crate::common::http_client::build_http_client(ctx, "s", None, None).unwrap());
        let mut sink = TelemetrySink::new(ctx, Url::parse("https://example.invalid/v1/telemetry").unwrap(), http);
        sink.in_flight = counter;
        sink.max_in_flight = max;
        sink
    }

    /// Mirrors `acquire_slot` without needing a XetContext, so the cap logic can be tested alone.
    fn try_acquire(in_flight: &'static AtomicUsize, max: usize) -> Option<InFlightGuard> {
        in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| (n < max).then_some(n + 1))
            .ok()?;
        Some(InFlightGuard(in_flight))
    }

    #[test]
    fn test_slots_are_capped_and_released() {
        let in_flight = counter(0);

        let a = try_acquire(in_flight, 2);
        let b = try_acquire(in_flight, 2);
        assert!(a.is_some() && b.is_some());
        assert_eq!(in_flight.load(Ordering::Acquire), 2);

        // At the cap: the next document is dropped, not queued.
        assert!(try_acquire(in_flight, 2).is_none());

        drop(a);
        assert_eq!(in_flight.load(Ordering::Acquire), 1);
        assert!(try_acquire(in_flight, 2).is_some());
    }

    /// A zero cap disables sending outright rather than letting one request through.
    #[test]
    fn test_zero_cap_admits_nothing() {
        let in_flight = counter(0);
        assert!(try_acquire(in_flight, 0).is_none());
        assert_eq!(in_flight.load(Ordering::Acquire), 0);
    }

    /// Every sink in the process draws from one budget.
    ///
    /// This is the point of the counter being global. A sink belongs to a single transfer, so with a
    /// per-sink counter a snapshot download - many concurrent per-file transfers, each with its own
    /// sink - multiplied the cap by the transfer count instead of limiting anything. Two sinks
    /// sharing a counter must not be able to hold `2 × max` slots between them.
    #[test]
    fn test_sinks_share_one_process_wide_budget() {
        let ctx = xet_runtime::core::XetContext::default().unwrap();
        let shared = counter(0);
        let (a, b) = (sink_with_counter(&ctx, shared, 2), sink_with_counter(&ctx, shared, 2));

        let first = a.acquire_slot();
        let second = b.acquire_slot();
        assert!(first.is_some() && second.is_some(), "each sink should get one of the two slots");

        // The cap is now reached process-wide, so *neither* sink may acquire again - a per-sink
        // counter would happily hand each of them two more.
        assert!(a.acquire_slot().is_none(), "sink A must see the slot sink B took");
        assert!(b.acquire_slot().is_none(), "sink B must see the slot sink A took");
        assert_eq!(shared.load(Ordering::Acquire), 2);

        // Releasing through one sink frees capacity for the other.
        drop(first);
        assert_eq!(shared.load(Ordering::Acquire), 1);
        assert!(b.acquire_slot().is_some(), "sink B must see the slot sink A released");
    }

    /// The production sinks all point at the process-wide counter, which is what makes the cap real.
    #[test]
    fn test_new_sinks_use_the_global_counter() {
        let ctx = xet_runtime::core::XetContext::default().unwrap();
        let http = Arc::new(crate::common::http_client::build_http_client(&ctx, "s", None, None).unwrap());
        let sink = TelemetrySink::new(&ctx, Url::parse("https://example.invalid/v1/telemetry").unwrap(), http);

        assert!(std::ptr::eq(sink.in_flight, &IN_FLIGHT), "a real sink must share the process-wide counter");
    }

    mod against_a_server {
        use std::time::Instant;

        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        use xet_runtime::config::XetConfig;
        use xet_runtime::core::XetContext;

        use super::*;
        use crate::cas_client::telemetry::TelemetryEnvelope;

        fn envelope() -> TelemetryEnvelope {
            TelemetryEnvelope::new("xet_upload_summary", "s".into(), "ua".into(), serde_json::json!({"a": 1}))
        }

        fn sink(ctx: &XetContext, base: &str) -> TelemetrySink {
            let http = Arc::new(crate::common::http_client::build_http_client(ctx, "s", None, None).unwrap());
            TelemetrySink::new(ctx, Url::parse(&format!("{base}/v1/telemetry")).unwrap(), http)
        }

        fn ctx_with_flush(flush: Duration, request: Duration) -> XetContext {
            let mut config = XetConfig::default();
            config.telemetry.final_flush_timeout = flush;
            config.telemetry.request_timeout = request;
            XetContext::with_config(config).unwrap()
        }

        /// The whole point of the design: a telemetry endpoint that never answers must not hold a
        /// transfer open past its flush budget.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_awaited_flush_respects_its_budget_when_the_server_hangs() {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v1/telemetry"))
                // Far longer than any budget under test.
                .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
                .mount(&server)
                .await;

            let budget = Duration::from_millis(300);
            let ctx = ctx_with_flush(budget, Duration::from_secs(30));
            let sink = sink(&ctx, &server.uri());

            let started = Instant::now();
            sink.submit_awaited(envelope(), budget).await;
            let elapsed = started.elapsed();

            assert!(
                elapsed < budget + Duration::from_secs(2),
                "flush took {elapsed:?}, which is not bounded by the {budget:?} budget"
            );
        }

        /// A zero budget degrades to a detached send, so it returns essentially immediately.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_zero_budget_returns_immediately() {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v1/telemetry"))
                .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
                .mount(&server)
                .await;

            let ctx = ctx_with_flush(Duration::ZERO, Duration::from_secs(30));
            let sink = sink(&ctx, &server.uri());

            let started = Instant::now();
            sink.submit_awaited(envelope(), Duration::ZERO).await;
            assert!(started.elapsed() < Duration::from_secs(1), "a zero budget must not wait on the request");
        }

        /// A rejection is swallowed, never retried, and never surfaced.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_server_rejection_is_swallowed_and_not_retried() {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v1/telemetry"))
                .respond_with(ResponseTemplate::new(429))
                // A retry would make this fail: exactly one request is expected.
                .expect(1)
                .mount(&server)
                .await;

            let ctx = ctx_with_flush(Duration::from_secs(5), Duration::from_secs(5));
            sink(&ctx, &server.uri())
                .submit_awaited(envelope(), Duration::from_secs(5))
                .await;

            // `MockServer` asserts the expectation on drop.
            drop(server);
        }

        /// The body that goes over the wire is the envelope, unmodified.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_posts_the_envelope_as_json() {
            // Built once and reused: `TelemetryEnvelope::new` stamps `time` from the clock, so two
            // calls would never compare equal.
            let expected = envelope();

            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v1/telemetry"))
                .and(wiremock::matchers::header("content-type", "application/json"))
                .and(wiremock::matchers::body_json(serde_json::to_value(&expected).unwrap()))
                .respond_with(ResponseTemplate::new(200))
                .expect(1)
                .mount(&server)
                .await;

            let ctx = ctx_with_flush(Duration::from_secs(5), Duration::from_secs(5));
            sink(&ctx, &server.uri()).submit_awaited(expected, Duration::from_secs(5)).await;

            drop(server);
        }
    }
}
