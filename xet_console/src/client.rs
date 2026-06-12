use std::time::Duration;

use anyhow::{Context, Result};
use xet_runtime::console::model::{IndexResponse, SessionsResponse, SnapshotResponse};

/// Blocking HTTP client for the xet-console API. The TUI polls /api/v1/snapshot
/// (everything the four pages render about LIVE sessions) plus /api/v1/sessions
/// (the only endpoint carrying the ended-sessions ring).
pub struct ConsoleClient {
    base: String,
    http: reqwest::blocking::Client,
}

impl ConsoleClient {
    pub fn new(base: String) -> Self {
        let http = reqwest::blocking::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .build()
            .expect("static client config");
        Self { base, http }
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    #[allow(dead_code)]
    pub fn index(&self) -> Result<IndexResponse> {
        self.get_json(&format!("{}/", self.base))
    }

    #[allow(dead_code)]
    pub fn snapshot(&self) -> Result<SnapshotResponse> {
        self.get_json(&format!("{}/api/v1/snapshot", self.base))
    }

    #[allow(dead_code)]
    pub fn sessions(&self) -> Result<SessionsResponse> {
        self.get_json(&format!("{}/api/v1/sessions", self.base))
    }

    fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T> {
        let resp = self.http.get(url).send().with_context(|| format!("GET {url}"))?;
        let resp = resp.error_for_status().with_context(|| format!("GET {url}"))?;
        resp.json::<T>().with_context(|| format!("decoding {url}"))
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use xet_runtime::console::registry::registry;
    use xet_runtime::console::server;

    use super::*;

    #[test]
    #[serial]
    fn client_fetches_index_and_snapshot_from_live_server() {
        unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
        server::ensure_started();
        let addr = server::bound_addr().expect("server bound");
        let _session = registry().register_session("tui-client-test".into(), vec![]);

        let client = ConsoleClient::new(format!("http://{addr}"));
        let index = client.index().expect("index");
        assert_eq!(index.service, "xet-console");
        assert_eq!(index.pid, std::process::id());

        let snap = client.snapshot().expect("snapshot");
        assert!(snap.as_of > 0);
        assert!(snap.sessions.iter().any(|s| s.detail.id == "tui-client-test"));

        // /sessions carries the ended ring, which /snapshot doesn't.
        let sessions = client.sessions().expect("sessions");
        assert!(sessions.as_of > 0);
        assert!(sessions.sessions.iter().any(|s| s.id == "tui-client-test"));
    }

    #[test]
    fn connection_refused_is_an_error_not_a_panic() {
        // Port 1 on loopback is essentially never listening.
        let client = ConsoleClient::new("http://127.0.0.1:1".into());
        assert!(client.snapshot().is_err());
    }
}
