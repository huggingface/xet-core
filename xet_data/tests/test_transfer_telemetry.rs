//! End-to-end coverage for client transfer telemetry, against the simulation CAS server.
//!
//! These tests matter more than they look. `Client::transfer_telemetry` has a *default* body
//! returning `None`, so a `RemoteClient` override with a mistyped signature would compile cleanly
//! and silently never be called - no unit test in `xet_client` or `xet_data` would notice. Only
//! driving a real transfer through a real HTTP server catches that.
//!
//! They also pin the wire shape. What a consumer receives is the serialized document, not the Rust
//! struct, so the key set is asserted here as well as in the payload unit tests.
//!
//! Every test here is `#[serial(env)]`. One of them sets `HF_HUB_DISABLE_TELEMETRY`, which is
//! process-global: marking only that test serial does not help, because `serial` serializes a test
//! against other *serial* tests, not against the parallel ones it would otherwise poison.

#![cfg(feature = "simulation")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use xet_client::cas_client::LocalTestServerBuilder;
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::test_utils::TestEnvironment;
use xet_data::processing::{FileDownloadSession, FileUploadSession, Sha256Policy, XetFileInfo};
use xet_runtime::config::XetConfig;
use xet_runtime::core::XetContext;
use xet_runtime::utils::EnvVarGuard;

/// The exact key set an upload document must carry. Mirrors `UPLOAD_KEYS` in
/// `xet_data/src/telemetry/payload.rs`, asserted here against what actually crossed the wire.
const UPLOAD_KEYS: &[&str] = &[
    "arch",
    "client_version",
    "compression_ratio",
    "cpu_count",
    "dedup_bytes",
    "dedup_chunks",
    "dedup_ratio",
    "defrag_prevented_dedup_bytes",
    "defrag_prevented_dedup_chunks",
    "direction",
    "dry_run",
    "duration_ms",
    "endpoint_host",
    "error_class",
    "ewma_throughput_bps",
    "finalize_ms",
    "global_dedup_bytes",
    "global_dedup_chunks",
    "ingest_ms",
    "logical_throughput_bps",
    "n_files",
    "new_bytes",
    "new_chunks",
    "os",
    "outcome",
    "peak_concurrency",
    "schema_version",
    "seq",
    "shard_bytes_uploaded",
    "shard_validation_entries",
    "shards_completed",
    "shards_total",
    "terminal",
    "throughput_bps",
    "total_bytes",
    "total_bytes_completed",
    "total_chunks",
    "transfer_bytes",
    "transfer_bytes_completed",
    "transfer_id",
    "xorb_bytes_uploaded",
];

async fn upload_bytes(session: &Arc<FileUploadSession>, name: &str, data: &[u8]) -> XetFileInfo {
    let (_id, mut cleaner) = session
        .start_clean(Some(name.into()), Some(data.len() as u64), Sha256Policy::Compute)
        .unwrap();
    cleaner.add_data(data).await.unwrap();
    cleaner.finish().await.unwrap().0
}

/// Waits for at least `n` documents to arrive.
///
/// Terminal documents on the finalize path are awaited by the client, but the `Drop` path and
/// heartbeats are detached, so their arrival is inherently racy.
async fn wait_for_docs(fetch: impl Fn() -> Vec<Value>, n: usize) -> Vec<Value> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let docs = fetch();
        if docs.len() >= n {
            return docs;
        }
        if Instant::now() > deadline {
            panic!("timed out waiting for {n} telemetry document(s); got {}", docs.len());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn sorted_keys(metrics: &Value) -> Vec<String> {
    let mut keys: Vec<_> = metrics
        .as_object()
        .expect("metrics must be an object")
        .keys()
        .cloned()
        .collect();
    keys.sort();
    keys
}

/// Asserts the five-key envelope the server validates.
fn assert_envelope(doc: &Value, expected_event: &str) {
    let mut keys: Vec<_> = doc.as_object().unwrap().keys().cloned().collect();
    keys.sort();
    assert_eq!(keys, vec!["event", "metrics", "session_id", "time", "userAgent"]);

    assert_eq!(doc["event"], expected_event);
    assert!(doc["session_id"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(doc["userAgent"].as_str().is_some_and(|s| !s.is_empty()));
    chrono::DateTime::parse_from_rfc3339(doc["time"].as_str().unwrap()).expect("time must be RFC3339");

    // The server rejects a body carrying both spellings.
    assert!(doc.get("user_agent").is_none(), "must not send the snake_case spelling too");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn test_upload_emits_one_terminal_document() {
    let env = TestEnvironment::new().await;

    let session = FileUploadSession::new(env.config.clone()).await.unwrap();
    upload_bytes(&session, "a.bin", &vec![0xAB; 64 * 1024]).await;
    upload_bytes(&session, "b.bin", &vec![0xCD; 32 * 1024]).await;
    session.finalize().await.unwrap();

    let docs = env.telemetry_docs();
    assert_eq!(docs.len(), 1, "expected exactly one terminal document, got {docs:#?}");

    let doc = &docs[0];
    assert_envelope(doc, "xet_upload_summary");

    let metrics = &doc["metrics"];
    assert_eq!(sorted_keys(metrics), UPLOAD_KEYS, "the wire key set drifted from the payload definition");
    assert_eq!(metrics["direction"], "upload");
    assert_eq!(metrics["outcome"], "ok");
    assert_eq!(metrics["error_class"], "none");
    assert_eq!(metrics["terminal"], true);
    assert_eq!(metrics["seq"], 0);
    assert_eq!(metrics["n_files"], 2);
    assert_eq!(metrics["dry_run"], false);
    assert_eq!(metrics["total_bytes"], 96 * 1024);
    assert!(metrics["new_bytes"].as_u64().unwrap() > 0);
    assert!(metrics["xorb_bytes_uploaded"].as_u64().unwrap() > 0);
    assert!(metrics["endpoint_host"].as_str().unwrap().starts_with("127.0.0.1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn test_download_emits_one_terminal_document() {
    let env = TestEnvironment::new().await;

    let data = vec![0x5A; 128 * 1024];
    let upload = FileUploadSession::new(env.config.clone()).await.unwrap();
    let xfi = upload_bytes(&upload, "f.bin", &data).await;
    upload.finalize().await.unwrap();

    let download = FileDownloadSession::new(env.config.clone(), None).await.unwrap();
    let out = env.base_dir.join("out.bin");
    download.download_file(&xfi, &out).await.unwrap();
    download.finalize().await.unwrap();

    assert_eq!(std::fs::read(&out).unwrap(), data);

    let docs = env.telemetry_docs();
    assert_eq!(docs.len(), 2, "expected one upload and one download document, got {docs:#?}");

    let doc = docs
        .iter()
        .find(|d| d["event"] == "xet_download_summary")
        .expect("no download document");
    assert_envelope(doc, "xet_download_summary");

    let metrics = &doc["metrics"];
    assert_eq!(metrics["direction"], "download");
    assert_eq!(metrics["outcome"], "ok");
    assert_eq!(metrics["terminal"], true);
    assert_eq!(metrics["n_files"], 1);
    assert!(metrics.get("expansion_ratio").is_some(), "download-only key missing");
    // Upload-only keys must not appear on a download document.
    assert!(metrics.get("dedup_ratio").is_none());
    assert!(metrics.get("ingest_ms").is_none());
}

/// Upload and download in the same session share a `session_id` but must be distinguishable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn test_transfer_id_separates_directions() {
    let env = TestEnvironment::new().await;

    let upload = FileUploadSession::new(env.config.clone()).await.unwrap();
    let xfi = upload_bytes(&upload, "f.bin", &vec![0x11; 16 * 1024]).await;
    upload.finalize().await.unwrap();

    let download = FileDownloadSession::new(env.config.clone(), None).await.unwrap();
    download.download_file(&xfi, &env.base_dir.join("o.bin")).await.unwrap();
    download.finalize().await.unwrap();

    let docs = env.telemetry_docs();
    assert_eq!(docs.len(), 2);

    let ids: Vec<_> = docs.iter().map(|d| d["metrics"]["transfer_id"].as_str().unwrap()).collect();
    assert_ne!(ids[0], ids[1], "each transfer needs its own id");
    let directions: Vec<_> = docs.iter().map(|d| d["metrics"]["direction"].as_str().unwrap()).collect();
    assert!(directions.contains(&"upload") && directions.contains(&"download"));
}

/// A download that ended badly must say so. `finalize()` alone can only ever report `ok`, so a
/// failed transfer has to go through `finalize_with` or every document claims success and the
/// failure-rate signal is silently always zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn test_failed_download_reports_error_outcome_and_class() {
    let env = TestEnvironment::new().await;

    let download = FileDownloadSession::new(env.config.clone(), None).await.unwrap();
    download
        .finalize_with(xet_data::telemetry::Outcome::Error, "network")
        .await
        .unwrap();

    let docs = env.telemetry_docs();
    assert_eq!(docs.len(), 1, "expected one terminal document, got {docs:#?}");

    let doc = &docs[0];
    assert_envelope(doc, "xet_download_summary");
    assert_eq!(doc["metrics"]["outcome"], "error");
    assert_eq!(doc["metrics"]["error_class"], "network");
    assert_eq!(doc["metrics"]["terminal"], true);
}

/// Cancellation is a user action, so it must not land in the `error` bucket that failure-rate
/// alerts watch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn test_cancelled_download_is_not_counted_as_an_error() {
    let env = TestEnvironment::new().await;

    let outcome = xet_data::telemetry::outcome_for_class("cancelled");
    assert_eq!(outcome, xet_data::telemetry::Outcome::Cancelled);

    let download = FileDownloadSession::new(env.config.clone(), None).await.unwrap();
    download.finalize_with(outcome, "cancelled").await.unwrap();

    let docs = env.telemetry_docs();
    assert_eq!(docs[0]["metrics"]["outcome"], "cancelled");
    assert_eq!(docs[0]["metrics"]["error_class"], "cancelled");
}

/// The `Drop` fallback must fire even when the last reference is released from a thread that is
/// not inside a tokio runtime.
///
/// This is the shape every embedder produces - notably the Python bindings, where the interpreter
/// thread drops the session. A `Handle::try_current()` guard here silently disabled download
/// telemetry entirely in production while every in-process test still passed, because tests drop
/// inside an async block where a runtime context happens to exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn test_download_session_dropped_off_runtime_still_reports() {
    let env = TestEnvironment::new().await;

    let download = FileDownloadSession::new(env.config.clone(), None).await.unwrap();
    // A plain OS thread has no ambient tokio runtime, so `Handle::try_current()` fails there.
    std::thread::spawn(move || {
        assert!(
            tokio::runtime::Handle::try_current().is_err(),
            "this test is meaningless if the spawning thread has a runtime context"
        );
        drop(download);
    })
    .join()
    .unwrap();

    let docs = wait_for_docs(|| env.telemetry_docs(), 1).await;
    assert_envelope(&docs[0], "xet_download_summary");
    assert_eq!(docs[0]["metrics"]["outcome"], "dropped");
}

/// A download session dropped without `finalize()` still reports.
///
/// `dropped` rather than `ok` because this session never registered a download: the `Drop` path
/// infers its outcome from progress, and an empty session has completed nothing. A session that
/// transferred everything and skipped `finalize()` reports `ok` instead - see
/// `stream_group_without_finish_reports_ok_when_fully_read` in `xet_pkg`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn test_dropped_download_session_reports_as_dropped() {
    let env = TestEnvironment::new().await;

    {
        let download = FileDownloadSession::new(env.config.clone(), None).await.unwrap();
        drop(download);
    }

    let docs = wait_for_docs(|| env.telemetry_docs(), 1).await;
    let doc = &docs[0];
    assert_envelope(doc, "xet_download_summary");
    assert_eq!(doc["metrics"]["outcome"], "dropped");
    assert_eq!(doc["metrics"]["terminal"], true);
}

/// An upload session dropped without finalizing reports as aborted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn test_dropped_upload_session_reports_as_aborted() {
    let env = TestEnvironment::new().await;

    {
        let upload = FileUploadSession::new(env.config.clone()).await.unwrap();
        upload_bytes(&upload, "a.bin", &vec![0x22; 8 * 1024]).await;
        drop(upload);
    }

    let docs = wait_for_docs(|| env.telemetry_docs(), 1).await;
    let doc = &docs[0];
    assert_envelope(doc, "xet_upload_summary");
    assert_eq!(doc["metrics"]["outcome"], "aborted");
    assert_eq!(doc["metrics"]["finalize_ms"], 0, "an abandoned session never reached finalization");
}

/// A session that finalizes normally and is then dropped must not emit twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn test_finalize_then_drop_emits_once() {
    let env = TestEnvironment::new().await;

    {
        let session = FileUploadSession::new(env.config.clone()).await.unwrap();
        upload_bytes(&session, "a.bin", &vec![0x33; 8 * 1024]).await;
        session.finalize().await.unwrap();
        // `session` drops here, after finalize already reported.
    }

    // Give any (incorrect) detached Drop emission time to land.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(env.telemetry_docs().len(), 1, "finalize and Drop both emitted");
}

/// Builds an environment with an explicit config, for the gating tests.
async fn env_with_config(
    config: XetConfig,
) -> (xet_client::cas_client::LocalTestServer, Arc<TranslatorConfig>, tempfile::TempDir) {
    let temp = tempfile::TempDir::new().unwrap();
    let ctx = XetContext::with_config(config).unwrap();
    let server = LocalTestServerBuilder::new().start().await;
    let translator = Arc::new(TranslatorConfig::test_server_config(&ctx, server.http_endpoint(), temp.path()).unwrap());
    (server, translator, temp)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn test_disabled_emits_nothing() {
    let mut config = XetConfig::default();
    config.telemetry.enabled = false;
    let (server, translator, _temp) = env_with_config(config).await;

    let session = FileUploadSession::new(translator).await.unwrap();
    upload_bytes(&session, "a.bin", &vec![0x44; 16 * 1024]).await;
    session.finalize().await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(server.telemetry_docs().is_empty(), "telemetry was disabled but documents were sent");
}

/// The shared huggingface_hub opt-out must suppress reporting even with telemetry enabled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn test_hub_opt_out_emits_nothing() {
    let _enabled = EnvVarGuard::set("HF_XET_TELEMETRY_ENABLED", "1");
    let _disabled = EnvVarGuard::set("HF_HUB_DISABLE_TELEMETRY", "1");

    let (server, translator, _temp) = env_with_config(XetConfig::new()).await;

    let session = FileUploadSession::new(translator).await.unwrap();
    upload_bytes(&session, "a.bin", &vec![0x55; 16 * 1024]).await;
    session.finalize().await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(server.telemetry_docs().is_empty(), "HF_HUB_DISABLE_TELEMETRY did not suppress reporting");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn test_dry_run_emits_nothing() {
    let (server, translator, _temp) = env_with_config(XetConfig::default()).await;

    let session = FileUploadSession::dry_run(translator).await.unwrap();
    upload_bytes(&session, "a.bin", &vec![0x66; 16 * 1024]).await;
    session.finalize().await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(server.telemetry_docs().is_empty(), "a dry run must not report");
}

/// Every metric value must be a scalar, and none may be null. `serde_json` renders NaN and
/// infinity as null, and one such document poisons the field's type for a consumer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(env)]
async fn test_wire_values_are_all_non_null_scalars() {
    let env = TestEnvironment::new().await;

    let session = FileUploadSession::new(env.config.clone()).await.unwrap();
    // Zero-byte file: the degenerate case most likely to produce a division by zero.
    upload_bytes(&session, "empty.bin", &[]).await;
    session.finalize().await.unwrap();

    let docs = env.telemetry_docs();
    assert_eq!(docs.len(), 1);

    for (key, value) in docs[0]["metrics"].as_object().unwrap() {
        assert!(!value.is_null(), "{key} arrived as null");
        assert!(
            matches!(value, Value::Bool(_) | Value::String(_) | Value::Number(_)),
            "{key} arrived as a non-scalar: {value:?}"
        );
    }
}
