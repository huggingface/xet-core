use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use xet_runtime::console::model::{SessionSummary, SnapshotResponse};

use crate::client::ConsoleClient;

#[derive(Default)]
pub struct PollState {
    pub snapshot: Option<SnapshotResponse>,
    /// Ended sessions (from /sessions — /snapshot carries live sessions only).
    pub ended_sessions: Vec<SessionSummary>,
    /// Epoch ms of the last successful poll (drives the staleness indicator).
    pub last_success_ms: Option<u64>,
    /// Last fetch error; cleared on the next success.
    pub last_error: Option<String>,
    /// Set by the UI ('p'); the poller skips fetches while true.
    pub paused: bool,
}

#[derive(Default)]
pub struct Shared(Mutex<PollState>);

impl Shared {
    /// Poison-tolerant lock: a panicked holder must not take the UI down.
    pub fn lock(&self) -> MutexGuard<'_, PollState> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn spawn_poller(
    client: ConsoleClient,
    shared: Arc<Shared>,
    interval: Duration,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("xet-console-poller".into())
        .spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                let paused = shared.lock().paused;
                if !paused {
                    match client.snapshot() {
                        Ok(snap) => {
                            // Best-effort companion fetch; the ended ring is cosmetic.
                            let ended = client.sessions().map(|s| s.ended_sessions).unwrap_or_default();
                            let mut st = shared.lock();
                            st.snapshot = Some(snap);
                            st.ended_sessions = ended;
                            st.last_success_ms = Some(now_ms());
                            st.last_error = None;
                        },
                        Err(e) => {
                            shared.lock().last_error = Some(format!("{e:#}"));
                        },
                    }
                }
                // Sleep in slices so shutdown stays responsive.
                let mut remaining = interval;
                while !shutdown.load(Ordering::Relaxed) && remaining > Duration::ZERO {
                    let slice = remaining.min(Duration::from_millis(50));
                    std::thread::sleep(slice);
                    remaining = remaining.saturating_sub(slice);
                }
            }
        })
        .expect("spawn poller thread")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use serial_test::serial;
    use xet_runtime::console::registry::registry;
    use xet_runtime::console::server;

    use super::*;
    use crate::client::ConsoleClient;

    #[test]
    #[serial]
    fn poller_populates_and_refreshes_shared_state() {
        unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
        server::ensure_started();
        let addr = server::bound_addr().expect("server bound");
        let _session = registry().register_session("tui-poll-test".into(), vec![]);

        let shared = Arc::new(Shared::default());
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handle = spawn_poller(
            ConsoleClient::new(format!("http://{addr}")),
            shared.clone(),
            Duration::from_millis(50),
            shutdown.clone(),
        );

        let mut first_as_of = 0;
        for _ in 0..40 {
            if let Some(snap) = &shared.lock().snapshot {
                first_as_of = snap.as_of;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(first_as_of > 0, "poller never populated a snapshot");

        // A later poll refreshes as_of.
        let mut refreshed = false;
        for _ in 0..40 {
            std::thread::sleep(Duration::from_millis(50));
            if shared.lock().snapshot.as_ref().is_some_and(|s| s.as_of > first_as_of) {
                refreshed = true;
                break;
            }
        }
        assert!(refreshed, "as_of never advanced");
        assert!(shared.lock().last_error.is_none());

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn poller_records_errors_and_keeps_running() {
        let shared = Arc::new(Shared::default());
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handle = spawn_poller(
            ConsoleClient::new("http://127.0.0.1:1".into()),
            shared.clone(),
            Duration::from_millis(20),
            shutdown.clone(),
        );
        let mut got_error = false;
        for _ in 0..100 {
            std::thread::sleep(Duration::from_millis(20));
            if shared.lock().last_error.is_some() {
                got_error = true;
                break;
            }
        }
        assert!(got_error, "connection failure must surface as last_error");
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }
}
