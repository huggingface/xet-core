#![cfg(feature = "console")]

use serial_test::serial;
use tempfile::tempdir;
use xet::xet_session::{Sha256Policy, XetSessionBuilder};

#[test]
#[serial]
fn session_appears_in_console_and_ends_on_drop() {
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    let session = XetSessionBuilder::new().build().unwrap();
    let addr = xet_runtime::console::server::bound_addr().expect("console server bound");
    let base = format!("http://{addr}");

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let sessions: serde_json::Value =
        rt.block_on(async { reqwest::get(format!("{base}/api/v1/sessions")).await.unwrap().json().await.unwrap() });
    assert!(
        sessions["sessions"].as_array().unwrap().iter().any(|s| s["state"] == "active"),
        "expected at least one active session, got: {sessions}"
    );

    drop(session);

    // Poll for up to ~2 s — absorbs the HTTP/snapshot round-trip latency after drop.
    // (In a real transfer, held ctx clones would legitimately delay the drop beyond any fixed poll.)
    let mut found_ended = false;
    for _ in 0..20 {
        let sessions: serde_json::Value = rt.block_on(async {
            reqwest::get(format!("{base}/api/v1/sessions")).await.unwrap().json().await.unwrap()
        });
        if sessions["ended_sessions"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .any(|s| s["state"] == "ended")
        {
            found_ended = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(found_ended, "expected an ended session within 2s after drop");
}

async fn get(url: String) -> serde_json::Value {
    reqwest::get(url).await.unwrap().json().await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn full_transfer_visible_over_http() {
    // Env must be set BEFORE the first session builds (that's what starts the server).
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    let temp = tempdir().unwrap();
    let session = XetSessionBuilder::new().build().unwrap();
    let endpoint = format!("local://{}", temp.path().join("cas").display());
    let addr = xet_runtime::console::server::bound_addr().unwrap();
    let base = format!("http://{addr}/api/v1");

    // Upload two files (call sequence mirrors xet_pkg/tests/test_xet_session.rs).
    let metas = {
        let commit = session
            .new_upload_commit()
            .unwrap()
            .with_endpoint(&endpoint)
            .build()
            .await
            .unwrap();
        let mut metas = Vec::new();
        for (name, data) in [("big.bin", vec![0xABu8; 4 << 20]), ("small.bin", b"small content".to_vec())] {
            let h = commit.upload_bytes(data, Sha256Policy::Compute, Some(name.into())).await.unwrap();
            metas.push(h.finalize_ingestion().await.unwrap());
        }
        commit.commit().await.unwrap();
        metas
    };

    // Pick the active session (robust: there will be exactly one under #[serial]).
    let sid = {
        let sessions = get(format!("{base}/sessions")).await;
        sessions["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["state"] == "active")
            .expect("expected an active session")["id"]
            .as_str()
            .unwrap()
            .to_string()
    };

    // /uploads returns an envelope with as_of and commits array.
    // The commit has already finished, so the list may be empty — just assert the envelope is valid.
    let uploads = get(format!("{base}/sessions/{sid}/uploads")).await;
    assert!(uploads["as_of"].as_u64().unwrap() > 0, "uploads response missing as_of, got: {uploads}");
    assert!(uploads["commits"].as_array().is_some(), "uploads response missing commits array, got: {uploads}");

    // Commit finished -> it lives in the session detail's ended_upload_commits.
    let detail = get(format!("{base}/sessions/{sid}")).await;
    let ended = detail["ended_upload_commits"].as_array().unwrap();
    assert_eq!(ended.len(), 1);
    assert_eq!(ended[0]["state"], "completed");
    assert!(ended[0]["dedup"]["total_bytes"].as_u64().unwrap() > 0);
    let files = ended[0]["completed_files"].as_array().unwrap();
    assert_eq!(files.len(), 2);
    for entry in files {
        let f = &entry[1]; // completed_files entries are [epoch_ms, snapshot] pairs
        assert_eq!(f["state"], "complete");
        assert!(f["file_hash"].is_string());
        assert_eq!(f["shard_uploaded"], true);
    }

    // Start download group; check concurrency WHILE the group (and its controller) is alive.
    let dest = temp.path().join("dest.bin");
    let group = session
        .new_file_download_group()
        .unwrap()
        .with_endpoint(&endpoint)
        .build()
        .await
        .unwrap();
    group
        .download_file_to_path(metas[0].xet_info.clone(), dest.clone())
        .await
        .unwrap();

    // Query concurrency BEFORE finish() — monitors are weak-ref'd and die with their controllers.
    let conc = get(format!("{base}/sessions/{sid}/concurrency")).await;
    let monitors = conc["monitors"].as_array().unwrap();
    assert!(!monitors.is_empty(), "local client registers at least the upload monitor");
    assert!(monitors.iter().any(|m| m["total_permits"].as_u64().unwrap() > 0));

    group.finish().await.unwrap();

    // After finish, the download group should appear as ended/finished.
    let detail = get(format!("{base}/sessions/{sid}")).await;
    assert_eq!(detail["ended_download_groups"].as_array().unwrap().len(), 1);
    assert_eq!(detail["ended_download_groups"][0]["state"], "finished");

    let snapshot = get(format!("{base}/snapshot")).await;
    assert_eq!(snapshot["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["process"]["pid"], std::process::id());
}
