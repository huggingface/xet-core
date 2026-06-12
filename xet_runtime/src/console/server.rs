use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::OnceLock;

use axum::extract::{Path, Query};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use tracing::{info, warn};

use super::model::*;
use super::now_ms;
use super::registry::registry;

pub const DEFAULT_CONSOLE_PORT: u16 = 6660;

static STARTED: OnceLock<()> = OnceLock::new();
static BOUND_ADDR: OnceLock<Option<SocketAddr>> = OnceLock::new();
static SERVER_START_MS: OnceLock<u64> = OnceLock::new();

/// Idempotent. Spawns the console server on a dedicated thread with its own
/// current-thread runtime, deliberately independent of any session's runtime.
/// Strict port policy: bind XET_CONSOLE_PORT (default 6660, 0 = ephemeral);
/// on failure, warn once and run without a console. Loopback only.
///
/// May block the CALLING thread up to ~2s on first invocation (startup
/// handshake; normally microseconds). Call it from session construction only.
/// Rule for instrumentation hooks: code inside async fns must be pure
/// non-blocking cell updates — never call ensure_started there.
pub fn ensure_started() {
    STARTED.get_or_init(|| {
        SERVER_START_MS.get_or_init(now_ms);
        let port = match std::env::var("XET_CONSOLE_PORT") {
            Ok(v) if v.trim().is_empty() => DEFAULT_CONSOLE_PORT,
            Ok(v) => match v.parse::<u16>() {
                Ok(p) => p,
                Err(_) => {
                    warn!("xet-console: invalid XET_CONSOLE_PORT={v}, console disabled");
                    let _ = BOUND_ADDR.set(None);
                    return;
                },
            },
            Err(_) => DEFAULT_CONSOLE_PORT,
        };
        let std_listener =
            match std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)) {
                Ok(l) => l,
                Err(e) => {
                    warn!("xet-console: failed to bind 127.0.0.1:{port}: {e}; console disabled");
                    let _ = BOUND_ADDR.set(None);
                    return;
                },
            };
        let addr = match std_listener.local_addr() {
            Ok(a) => a,
            Err(e) => {
                warn!("xet-console: no local addr: {e}; console disabled");
                let _ = BOUND_ADDR.set(None);
                return;
            },
        };
        if std_listener.set_nonblocking(true).is_err() {
            warn!("xet-console: failed to set nonblocking; console disabled");
            let _ = BOUND_ADDR.set(None);
            return;
        }
        let (tx, rx) = std::sync::mpsc::channel::<Result<(), String>>();
        match std::thread::Builder::new()
            .name("xet-console-server".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => {
                        warn!("xet-console: runtime build failed: {e}");
                        let _ = tx.send(Err(format!("runtime build failed: {e}")));
                        return;
                    },
                };
                rt.block_on(async move {
                    let listener = match tokio::net::TcpListener::from_std(std_listener) {
                        Ok(l) => l,
                        Err(e) => {
                            warn!("xet-console: listener conversion failed: {e}");
                            let _ = tx.send(Err(format!("listener conversion failed: {e}")));
                            return;
                        },
                    };
                    let _ = tx.send(Ok(()));
                    if let Err(e) = axum::serve(listener, router()).await {
                        warn!("xet-console: server exited: {e}");
                    }
                });
            }) {
            Ok(_) => {
                match rx.recv_timeout(std::time::Duration::from_secs(2)) {
                    Ok(Ok(())) => {
                        let _ = BOUND_ADDR.set(Some(addr));
                        info!("xet-console listening on http://{addr}");
                    },
                    Ok(Err(e)) => {
                        warn!("xet-console: startup failed: {e}; console disabled");
                        let _ = BOUND_ADDR.set(None);
                    },
                    Err(e) => {
                        warn!("xet-console: startup handshake failed: {e}; console disabled");
                        let _ = BOUND_ADDR.set(None);
                    },
                }
            },
            Err(e) => {
                warn!("xet-console: thread spawn failed: {e}; console disabled");
                let _ = BOUND_ADDR.set(None);
            },
        }
    });
}

/// Some(addr) once bound; None if startup failed or never attempted.
pub fn bound_addr() -> Option<SocketAddr> {
    BOUND_ADDR.get().copied().flatten()
}

fn router() -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/v1/process", get(process_info))
        .route("/api/v1/sessions", get(sessions))
        .route("/api/v1/sessions/{sid}", get(session_detail))
        .route("/api/v1/sessions/{sid}/uploads", get(uploads))
        .route("/api/v1/sessions/{sid}/uploads/{cid}", get(upload_detail))
        .route("/api/v1/sessions/{sid}/downloads", get(downloads))
        .route("/api/v1/sessions/{sid}/downloads/{gid}", get(download_detail))
        .route("/api/v1/sessions/{sid}/concurrency", get(concurrency))
        .route("/api/v1/snapshot", get(snapshot))
}

// ---- helpers ----

fn not_found(error: String) -> (StatusCode, Json<ErrorResponse>) {
    (StatusCode::NOT_FOUND, Json(ErrorResponse { error }))
}

fn make_process_info() -> ProcessInfo {
    ProcessInfo {
        as_of: now_ms(),
        pid: std::process::id(),
        argv: std::env::args().collect(),
        start_time_ms: *SERVER_START_MS.get().unwrap_or(&0),
        version: env!("CARGO_PKG_VERSION").into(),
        n_active_sessions: registry().live_sessions().len(),
    }
}

fn with_session<T: serde::Serialize>(
    sid: &str,
    f: impl FnOnce(std::sync::Arc<super::state::SessionConsole>) -> Option<T>,
) -> Result<Json<T>, (StatusCode, Json<ErrorResponse>)> {
    registry()
        .session(sid)
        .and_then(f)
        .map(Json)
        .ok_or_else(|| not_found(format!("not found under session {sid}")))
}

// ---- handlers ----

static ENDPOINTS: &[&str] = &[
    "/",
    "/api/v1/process",
    "/api/v1/sessions",
    "/api/v1/sessions/{sid}",
    "/api/v1/sessions/{sid}/uploads",
    "/api/v1/sessions/{sid}/uploads/{cid}",
    "/api/v1/sessions/{sid}/downloads",
    "/api/v1/sessions/{sid}/downloads/{gid}",
    "/api/v1/sessions/{sid}/concurrency",
    "/api/v1/snapshot",
];

async fn index() -> Json<IndexResponse> {
    Json(IndexResponse {
        service: "xet-console".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        pid: std::process::id(),
        argv: std::env::args().collect(),
        start_time_ms: *SERVER_START_MS.get().unwrap_or(&0),
        endpoints: ENDPOINTS.iter().map(|s| s.to_string()).collect(),
    })
}

async fn process_info() -> Json<ProcessInfo> {
    Json(make_process_info())
}

async fn sessions() -> Json<SessionsResponse> {
    let (active, ended) = registry().session_summaries();
    Json(SessionsResponse { as_of: now_ms(), sessions: active, ended_sessions: ended })
}

async fn session_detail(
    Path(sid): Path<String>,
) -> Result<Json<SessionDetail>, (StatusCode, Json<ErrorResponse>)> {
    with_session(&sid, |s| Some(s.detail(SessionState::Active)))
}

async fn uploads(
    Path(sid): Path<String>,
) -> Result<Json<Vec<UploadCommitSummary>>, (StatusCode, Json<ErrorResponse>)> {
    with_session(&sid, |s| {
        Some(s.live_upload_commits().iter().map(|c| c.summary()).collect())
    })
}

async fn upload_detail(
    Path((sid, cid)): Path<(String, u64)>,
    // ?files=all accepted for API stability; currently equivalent to the default (rings are the bound)
    Query(_files_param): Query<HashMap<String, String>>,
) -> Result<Json<UploadCommitDetail>, (StatusCode, Json<ErrorResponse>)> {
    let session = registry().session(&sid).ok_or_else(|| not_found(format!("not found under session {sid}")))?;
    // Search live commits first; prefer live over ended if both present momentarily.
    if let Some(commit) = session.live_upload_commits().into_iter().find(|c| c.id == cid) {
        return Ok(Json(commit.snapshot(true)));
    }
    // Fall back to ended ring.
    let ended = session.ended_upload_commits();
    if let Some(detail) = ended.into_iter().find(|d| d.id == cid) {
        return Ok(Json(detail));
    }
    Err(not_found(format!("no upload commit {cid} in session {sid}")))
}

async fn downloads(
    Path(sid): Path<String>,
) -> Result<Json<Vec<DownloadGroupSummary>>, (StatusCode, Json<ErrorResponse>)> {
    with_session(&sid, |s| {
        Some(s.live_download_groups().iter().map(|g| g.summary()).collect())
    })
}

async fn download_detail(
    Path((sid, gid)): Path<(String, u64)>,
    // ?files=all accepted for API stability; currently equivalent to the default (rings are the bound)
    Query(_files_param): Query<HashMap<String, String>>,
) -> Result<Json<DownloadGroupDetail>, (StatusCode, Json<ErrorResponse>)> {
    let session = registry().session(&sid).ok_or_else(|| not_found(format!("not found under session {sid}")))?;
    // Search live groups first.
    if let Some(group) = session.live_download_groups().into_iter().find(|g| g.id == gid) {
        return Ok(Json(group.snapshot(true)));
    }
    // Fall back to ended ring.
    let ended = session.ended_download_groups();
    if let Some(detail) = ended.into_iter().find(|d| d.id == gid) {
        return Ok(Json(detail));
    }
    Err(not_found(format!("no download group {gid} in session {sid}")))
}

async fn concurrency(
    Path(sid): Path<String>,
) -> Result<Json<ConcurrencyResponse>, (StatusCode, Json<ErrorResponse>)> {
    with_session(&sid, |s| {
        Some(ConcurrencyResponse {
            as_of: now_ms(),
            session_id: sid.clone(),
            monitors: s.monitor_snapshots(),
        })
    })
}

async fn snapshot(
    Query(params): Query<HashMap<String, String>>,
) -> Json<SnapshotResponse> {
    let filter = params.get("session").map(|s| s.as_str());
    let all_sessions = registry().live_sessions();
    let sessions = all_sessions
        .iter()
        .filter(|s| filter.is_none_or(|id| s.id == id))
        .map(|s| s.full(SessionState::Active))
        .collect();
    Json(SnapshotResponse { as_of: now_ms(), process: make_process_info(), sessions })
}
