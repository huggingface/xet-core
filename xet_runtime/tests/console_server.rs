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
        let index: serde_json::Value =
            reqwest::get(format!("{base}/")).await.unwrap().json().await.unwrap();
        assert_eq!(index["service"], "xet-console");
        assert!(index["endpoints"].as_array().unwrap().len() >= 9);

        let process: serde_json::Value =
            reqwest::get(format!("{base}/api/v1/process")).await.unwrap().json().await.unwrap();
        assert_eq!(process["pid"], std::process::id());
        assert!(process["as_of"].as_u64().unwrap() > 0);

        let sessions: serde_json::Value =
            reqwest::get(format!("{base}/api/v1/sessions")).await.unwrap().json().await.unwrap();
        let listed = sessions["sessions"].as_array().unwrap();
        assert!(listed.iter().any(|s| s["id"] == "itest-session"));

        let detail = reqwest::get(format!("{base}/api/v1/sessions/itest-session")).await.unwrap();
        assert!(detail.status().is_success());

        let missing = reqwest::get(format!("{base}/api/v1/sessions/nope")).await.unwrap();
        assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    });
}
