#![cfg(test)]

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crossterm::event::KeyCode;
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use serial_test::serial;
use xet_runtime::console::model::FileUploadState;
use xet_runtime::console::registry::registry;
use xet_runtime::console::server;
use xet_runtime::console::state::UploadCommitConsole;

use crate::app::{App, overview_entries};
use crate::client::ConsoleClient;
use crate::poll::{Shared, spawn_poller};
use crate::ui;
use crate::ui::widgets::buffer_text;

#[test]
#[serial]
fn end_to_end_render_from_live_server() {
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    server::ensure_started();
    let addr = server::bound_addr().expect("server bound");

    // Fabricate a session with one commit holding one chunking file.
    let handle = registry().register_session("tui-smoke".into(), vec![]);
    let commit = UploadCommitConsole::new(Some(&handle.scope), Some("local://smoke".into()));
    let file = commit.new_file(1, "smoke.bin", Some(1024));
    file.set_state(FileUploadState::Chunking);
    file.add_chunked_bytes(512, 4);

    // Real client + real poller.
    let shared = Arc::new(Shared::default());
    let shutdown = Arc::new(AtomicBool::new(false));
    let poller = spawn_poller(
        ConsoleClient::new(format!("http://{addr}")),
        shared.clone(),
        Duration::from_millis(50),
        shutdown.clone(),
    );
    for _ in 0..40 {
        if shared.lock().snapshot.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Drive the App like the run loop does: drill into the commit and render.
    let mut app = App::default();
    {
        let state = shared.lock();
        let snap = state.snapshot.as_ref().expect("poller populated");
        let entries = overview_entries(snap, &HashSet::new());
        // Row 0 is our session; row 1 is the commit (this process may carry
        // sessions from sibling #[serial] tests — find OUR commit row).
        let row = entries
            .iter()
            .position(|e| {
                matches!(e, crate::app::OverviewEntry::Commit { session_idx, .. }
                    if snap.sessions[*session_idx].detail.id == "tui-smoke")
            })
            .expect("our commit row exists");
        app.overview_row = row;
        app.handle_key(KeyCode::Enter, &entries);
    }

    let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
    {
        let state = shared.lock();
        terminal.draw(|f| ui::draw(f, &app, &state)).unwrap();
    }
    let text = buffer_text(terminal.backend());
    assert!(text.contains("smoke.bin"), "{text}");
    assert!(text.contains("chunking"), "{text}");
    assert!(text.contains("50%"), "512/1024 chunked: {text}");

    shutdown.store(true, Ordering::Relaxed);
    poller.join().unwrap();
    drop(file);
    drop(commit);
}
