#![cfg(feature = "console")]

use serial_test::serial;
use xet_runtime::console::registry::registry;
use xet_runtime::console::server;

#[test]
#[serial]
fn server_serves_index_process_and_sessions() {
    // SAFETY-NOTE: env mutation is why this test is #[serial].
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") }; // ephemeral for tests
    server::ensure_started();
    let addr = server::bound_addr().expect("server should have bound an ephemeral port");

    let _session = registry().register_session("itest-session".into(), vec![]);

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let base = format!("http://{addr}");
        let index: serde_json::Value = reqwest::get(format!("{base}/")).await.unwrap().json().await.unwrap();
        assert_eq!(index["service"], "xet-console");
        assert!(index["endpoints"].as_array().unwrap().len() >= 9);

        let process: serde_json::Value = reqwest::get(format!("{base}/api/v1/process"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(process["pid"], std::process::id());
        assert!(process["as_of"].as_u64().unwrap() > 0);

        let sessions: serde_json::Value = reqwest::get(format!("{base}/api/v1/sessions"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let listed = sessions["sessions"].as_array().unwrap();
        assert!(listed.iter().any(|s| s["id"] == "itest-session"));

        let detail = reqwest::get(format!("{base}/api/v1/sessions/itest-session")).await.unwrap();
        assert!(detail.status().is_success());

        let uploads: serde_json::Value = reqwest::get(format!("{base}/api/v1/sessions/itest-session/uploads"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(uploads["as_of"].as_u64().unwrap() > 0, "uploads response missing as_of");
        assert!(uploads["commits"].as_array().is_some(), "uploads response missing commits array");

        let downloads: serde_json::Value = reqwest::get(format!("{base}/api/v1/sessions/itest-session/downloads"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(downloads["as_of"].as_u64().unwrap() > 0, "downloads response missing as_of");
        assert!(downloads["groups"].as_array().is_some(), "downloads response missing groups array");

        let missing = reqwest::get(format!("{base}/api/v1/sessions/nope")).await.unwrap();
        assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    });

    // Multi-session listing + ?session= snapshot filter.
    let _second = registry().register_session("itest-session-2".into(), vec![]);
    rt.block_on(async {
        let base = format!("http://{addr}");
        let sessions: serde_json::Value = reqwest::get(format!("{base}/api/v1/sessions"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let listed = sessions["sessions"].as_array().unwrap();
        assert!(listed.iter().any(|s| s["id"] == "itest-session"));
        assert!(listed.iter().any(|s| s["id"] == "itest-session-2"));

        let filtered: serde_json::Value = reqwest::get(format!("{base}/api/v1/snapshot?session=itest-session-2"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let snap_sessions = filtered["sessions"].as_array().unwrap();
        assert_eq!(snap_sessions.len(), 1);
        assert_eq!(snap_sessions[0]["detail"]["id"], "itest-session-2");

        let unfiltered: serde_json::Value = reqwest::get(format!("{base}/api/v1/snapshot"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(unfiltered["sessions"].as_array().unwrap().len() >= 2);
    });
}
