#![cfg(feature = "console")]

use serial_test::serial;
use xet::xet_session::XetSessionBuilder;

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
