//! Telemetry coverage driven through the *public group API*, not the session underneath it.
//!
//! `xet_data`'s telemetry tests call `FileDownloadSession::finalize()` directly. That proves the
//! emit machinery works, but it cannot prove anything about whether a real caller ever reaches it -
//! and for a long time none did: `XetFileDownloadGroup::finish()` read the session's progress
//! report and returned without finalizing, so downloads through the Python bindings emitted no
//! telemetry at all while every test still passed.
//!
//! These tests therefore go through `finish_blocking()` / `finish()` and assert on what the server
//! received. Keep them that way: a test that calls `finalize()` itself re-opens the same gap.

#![cfg(feature = "simulation")]

use std::fs;
use std::time::{Duration, Instant};

use serde_json::Value;
use serial_test::serial;
use tempfile::{TempDir, tempdir};
use xet::xet_session::{Sha256Policy, XetFileInfo, XetSession, XetSessionBuilder};
use xet_client::cas_client::{LocalTestServer, LocalTestServerBuilder};

/// Starts a simulation CAS server on its own runtime.
///
/// The tests below are deliberately *not* `#[tokio::test]`: `finish_blocking` panics inside a
/// tokio runtime, and the whole point is to exercise the blocking path the bindings use. The
/// server still needs an async context to start, so it gets a dedicated one.
fn start_server() -> (LocalTestServer, tokio::runtime::Runtime) {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let server = rt.block_on(async { LocalTestServerBuilder::new().start().await });
    (server, rt)
}

fn upload_bytes_sync(session: &XetSession, endpoint: &str, data: &[u8], name: &str) -> XetFileInfo {
    let commit = session
        .new_upload_commit()
        .unwrap()
        .with_endpoint(endpoint)
        .build_blocking()
        .unwrap();
    let handle = commit
        .upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some(name.into()))
        .unwrap();
    let file_meta = handle.finalize_ingestion_blocking().unwrap();
    commit.commit_blocking().unwrap();
    file_meta.xet_info
}

/// Waits for at least `n` documents. The terminal send is awaited by the client, but a document
/// still has to cross a socket, so a bare read can race.
fn wait_for_docs(server: &LocalTestServer, n: usize) -> Vec<Value> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let docs = server.telemetry_docs();
        if docs.len() >= n {
            return docs;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {n} telemetry document(s); got {}", docs.len());
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn download_doc(docs: &[Value]) -> &Value {
    docs.iter()
        .find(|d| d["event"] == "xet_download_summary")
        .unwrap_or_else(|| panic!("no download document among {docs:#?}"))
}

/// The regression test for the gap itself: finishing a download group must emit.
#[test]
#[serial(env)]
fn finish_blocking_emits_a_download_document() {
    let temp: TempDir = tempdir().unwrap();
    let (server, _rt) = start_server();
    let endpoint = server.http_endpoint();

    let session = XetSessionBuilder::new().build().unwrap();
    let data = vec![0x5Au8; 96 * 1024];
    let file_info = upload_bytes_sync(&session, endpoint, &data, "f.bin");

    let dest = temp.path().join("f.out");
    let group = session
        .new_file_download_group()
        .unwrap()
        .with_endpoint(endpoint)
        .build_blocking()
        .unwrap();
    group.download_file_to_path_blocking(file_info, dest.clone()).unwrap();
    group.finish_blocking().unwrap();

    assert_eq!(fs::read(&dest).unwrap(), data, "the download itself must still work");

    let docs = wait_for_docs(&server, 2);
    let doc = download_doc(&docs);

    assert_eq!(doc["metrics"]["direction"], "download");
    assert_eq!(doc["metrics"]["outcome"], "ok");
    assert_eq!(doc["metrics"]["error_class"], "none");
    assert_eq!(doc["metrics"]["terminal"], true);
    assert_eq!(doc["metrics"]["n_files"], 1);
    assert_eq!(doc["metrics"]["total_bytes"], data.len());
}

/// Exactly one document per group: `finish` must not emit a second one on top of `Drop`.
#[test]
#[serial(env)]
fn finishing_then_dropping_emits_one_document() {
    let temp: TempDir = tempdir().unwrap();
    let (server, _rt) = start_server();
    let endpoint = server.http_endpoint();

    let session = XetSessionBuilder::new().build().unwrap();
    let data = vec![0x11u8; 32 * 1024];
    let file_info = upload_bytes_sync(&session, endpoint, &data, "g.bin");

    {
        let group = session
            .new_file_download_group()
            .unwrap()
            .with_endpoint(endpoint)
            .build_blocking()
            .unwrap();
        group
            .download_file_to_path_blocking(file_info, temp.path().join("g.out"))
            .unwrap();
        group.finish_blocking().unwrap();
    }

    let docs = wait_for_docs(&server, 2);
    // Give a stray Drop-path document time to show up before asserting there is none.
    std::thread::sleep(Duration::from_millis(300));

    let downloads: Vec<_> = server
        .telemetry_docs()
        .into_iter()
        .filter(|d| d["event"] == "xet_download_summary")
        .collect();
    assert_eq!(downloads.len(), 1, "expected exactly one download document, got {downloads:#?}");
    let _ = docs;
}

/// A stream group has no natural completion point, so it gets an explicit `finish`.
#[test]
#[serial(env)]
fn stream_group_finish_emits_a_clean_document() {
    let (server, _rt) = start_server();
    let endpoint = server.http_endpoint();

    let session = XetSessionBuilder::new().build().unwrap();
    let data = vec![0x77u8; 48 * 1024];
    let file_info = upload_bytes_sync(&session, endpoint, &data, "s.bin");

    let group = session
        .new_download_stream_group()
        .unwrap()
        .with_endpoint(endpoint)
        .build_blocking()
        .unwrap();

    let mut stream = group.download_stream_blocking(file_info, None).unwrap();
    let mut received = 0usize;
    while let Some(chunk) = stream.blocking_next().unwrap() {
        received += chunk.len();
    }
    assert_eq!(received, data.len());

    group.finish_blocking().unwrap();

    let docs = wait_for_docs(&server, 2);
    let doc = download_doc(&docs);
    assert_eq!(doc["metrics"]["outcome"], "ok", "an explicitly finished stream group is not 'dropped'");
}

/// `finish` is additive: a caller that never calls it keeps working exactly as before, and still
/// reports.
///
/// The outcome is `ok`, not `dropped`, because the transfer genuinely completed - every stream was
/// read to its end. Since `finish()` is new and existing embedders have not adopted it, this is the
/// common shape for stream-group downloads; reporting it as `dropped` would make that outcome mean
/// "probably fine" and leave a failure-rate dashboard with nothing to measure.
///
/// This is also the compatibility guarantee for existing embedders: `finish` must stay optional.
#[test]
#[serial(env)]
fn stream_group_without_finish_reports_ok_when_fully_read() {
    let (server, _rt) = start_server();
    let endpoint = server.http_endpoint();

    let session = XetSessionBuilder::new().build().unwrap();
    let data = vec![0x33u8; 48 * 1024];
    let file_info = upload_bytes_sync(&session, endpoint, &data, "n.bin");

    {
        let group = session
            .new_download_stream_group()
            .unwrap()
            .with_endpoint(endpoint)
            .build_blocking()
            .unwrap();

        let mut stream = group.download_stream_blocking(file_info, None).unwrap();
        let mut received = 0usize;
        while let Some(chunk) = stream.blocking_next().unwrap() {
            received += chunk.len();
        }
        assert_eq!(received, data.len(), "the data must still arrive without finish()");
        // Deliberately no `finish()`; the group and its session drop here.
    }

    let docs = wait_for_docs(&server, 2);
    let doc = download_doc(&docs);
    assert_eq!(doc["metrics"]["outcome"], "ok", "a fully-read group is not 'dropped' just for skipping finish()");
    assert_eq!(doc["metrics"]["terminal"], true);
}

/// The counterpart: a stream abandoned part-way must still report `dropped`, so the outcome keeps
/// distinguishing a real abandonment from a caller that merely skipped `finish()`.
///
/// The file is large enough to span several chunks, so stopping after the first leaves the transfer
/// genuinely incomplete.
#[test]
#[serial(env)]
fn stream_group_abandoned_part_way_reports_dropped() {
    let (server, _rt) = start_server();
    let endpoint = server.http_endpoint();

    let session = XetSessionBuilder::new().build().unwrap();
    let data = vec![0x5au8; 4 * 1024 * 1024];
    let file_info = upload_bytes_sync(&session, endpoint, &data, "partial.bin");

    {
        let group = session
            .new_download_stream_group()
            .unwrap()
            .with_endpoint(endpoint)
            .build_blocking()
            .unwrap();

        let mut stream = group.download_stream_blocking(file_info, None).unwrap();
        let first = stream
            .blocking_next()
            .unwrap()
            .expect("the stream must yield at least one chunk");
        assert!(first.len() < data.len(), "this test needs a file that does not arrive in a single chunk");
        // Abandon the stream and the group here, without reading the rest and without `finish()`.
    }

    let docs = wait_for_docs(&server, 2);
    let doc = download_doc(&docs);
    assert_eq!(doc["metrics"]["outcome"], "dropped", "an abandoned stream must still report as dropped");
    assert_eq!(doc["metrics"]["terminal"], true);
}

/// After `finish`, the group is closed: new streams cannot be started. Documents the sharp edge
/// that comes with the context-manager form.
#[test]
#[serial(env)]
fn stream_group_rejects_new_streams_after_finish() {
    let (server, _rt) = start_server();
    let endpoint = server.http_endpoint();

    let session = XetSessionBuilder::new().build().unwrap();
    let data = vec![0x44u8; 16 * 1024];
    let file_info = upload_bytes_sync(&session, endpoint, &data, "c.bin");

    let group = session
        .new_download_stream_group()
        .unwrap()
        .with_endpoint(endpoint)
        .build_blocking()
        .unwrap();
    group.finish_blocking().unwrap();

    assert!(
        group.download_stream_blocking(file_info, None).is_err(),
        "a finished group must not hand out new streams"
    );
}
