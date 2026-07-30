use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use http::header::CONTENT_TYPE;
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use tracing::debug;
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
    in_flight: Arc<AtomicUsize>,
    max_in_flight: usize,
    request_timeout: Duration,
}

impl TelemetrySink {
    pub(crate) fn new(ctx: &XetContext, url: Url, http: Arc<ClientWithMiddleware>) -> Self {
        Self {
            ctx: ctx.clone(),
            url,
            http,
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_in_flight: ctx.config.telemetry.max_in_flight,
            request_timeout: ctx.config.telemetry.request_timeout,
        }
    }

    /// Claims an in-flight slot, or `None` when the cap is reached.
    fn acquire_slot(&self) -> Option<InFlightGuard> {
        let max = self.max_in_flight;
        self.in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| (n < max).then_some(n + 1))
            .ok()?;
        Some(InFlightGuard(self.in_flight.clone()))
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
            debug!(target: LOG_TARGET, event = envelope.event, status = %response.status(), "telemetry accepted");
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
struct InFlightGuard(Arc<AtomicUsize>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    fn counter(start: usize) -> Arc<AtomicUsize> {
        Arc::new(AtomicUsize::new(start))
    }

    /// Mirrors `acquire_slot` without needing a XetContext, so the cap logic can be tested alone.
    fn try_acquire(in_flight: &Arc<AtomicUsize>, max: usize) -> Option<InFlightGuard> {
        in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| (n < max).then_some(n + 1))
            .ok()?;
        Some(InFlightGuard(in_flight.clone()))
    }

    #[test]
    fn test_slots_are_capped_and_released() {
        let in_flight = counter(0);

        let a = try_acquire(&in_flight, 2);
        let b = try_acquire(&in_flight, 2);
        assert!(a.is_some() && b.is_some());
        assert_eq!(in_flight.load(Ordering::Acquire), 2);

        // At the cap: the next document is dropped, not queued.
        assert!(try_acquire(&in_flight, 2).is_none());

        drop(a);
        assert_eq!(in_flight.load(Ordering::Acquire), 1);
        assert!(try_acquire(&in_flight, 2).is_some());
    }

    /// A zero cap disables sending outright rather than letting one request through.
    #[test]
    fn test_zero_cap_admits_nothing() {
        let in_flight = counter(0);
        assert!(try_acquire(&in_flight, 0).is_none());
        assert_eq!(in_flight.load(Ordering::Acquire), 0);
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
