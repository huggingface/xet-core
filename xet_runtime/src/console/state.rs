use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use super::model::*;
use super::now_ms;
use super::ring::TimestampedRing;

pub const RECENT_RING_CAPACITY: usize = 256;
pub const ENDED_COMMITS_CAPACITY: usize = 64;
pub const ENDED_GROUPS_CAPACITY: usize = 64;

pub type ProgressProvider = Box<dyn Fn() -> ProgressSnapshot + Send + Sync>;
/// id -> (total_bytes, bytes_completed); installed by xet_data, reads GroupProgress.
pub type ItemBytesProvider = Box<dyn Fn() -> HashMap<u64, (u64, u64)> + Send + Sync>;

pub struct SessionConsole {
    pub id: String,
    pub created_at: u64,
    pub config: Vec<(String, String)>,
    upload_commits: Mutex<Vec<Weak<UploadCommitConsole>>>,
    ended_upload_commits: Mutex<TimestampedRing<UploadCommitDetail>>,
    ended_download_groups: Mutex<TimestampedRing<DownloadGroupDetail>>,
    monitors: Mutex<Vec<Weak<MonitorConsole>>>,
    download_groups: Mutex<Vec<Weak<DownloadGroupConsole>>>,
}

impl SessionConsole {
    pub fn new(id: String, config: Vec<(String, String)>) -> Arc<Self> {
        Arc::new(Self {
            id,
            created_at: now_ms(),
            config,
            upload_commits: Mutex::new(Vec::new()),
            ended_upload_commits: Mutex::new(TimestampedRing::new(ENDED_COMMITS_CAPACITY)),
            ended_download_groups: Mutex::new(TimestampedRing::new(ENDED_GROUPS_CAPACITY)),
            monitors: Mutex::new(Vec::new()),
            download_groups: Mutex::new(Vec::new()),
        })
    }

    /// Returns live (non-dropped) upload commits; prunes dead weaks in place.
    pub fn live_upload_commits(&self) -> Vec<Arc<UploadCommitConsole>> {
        let Ok(mut commits) = self.upload_commits.lock() else {
            return Vec::new();
        };
        let mut result = Vec::new();
        commits.retain(|w| {
            if let Some(arc) = w.upgrade() {
                result.push(arc);
                true
            } else {
                false
            }
        });
        result
    }

    /// Returns a snapshot of all ended upload commit details.
    pub fn ended_upload_commits(&self) -> Vec<UploadCommitDetail> {
        let Ok(ring) = self.ended_upload_commits.lock() else {
            return Vec::new();
        };
        ring.snapshot().into_iter().map(|(_, d)| d).collect()
    }

    pub fn record_ended_commit(&self, detail: UploadCommitDetail) {
        let Ok(ring) = self.ended_upload_commits.lock() else {
            return;
        };
        ring.push(detail);
    }

    pub fn ended_download_groups(&self) -> Vec<DownloadGroupDetail> {
        let Ok(ring) = self.ended_download_groups.lock() else {
            return Vec::new();
        };
        ring.snapshot().into_iter().map(|(_, d)| d).collect()
    }

    pub fn record_ended_download_group(&self, detail: DownloadGroupDetail) {
        let Ok(ring) = self.ended_download_groups.lock() else {
            return;
        };
        ring.push(detail);
    }

    fn register_upload_commit(&self, commit: &Arc<UploadCommitConsole>) {
        let Ok(mut commits) = self.upload_commits.lock() else {
            return;
        };
        commits.push(Arc::downgrade(commit));
    }

    fn register_download_group(&self, group: &Arc<DownloadGroupConsole>) {
        let Ok(mut groups) = self.download_groups.lock() else { return; };
        groups.push(Arc::downgrade(group));
    }

    /// Returns live (non-dropped) download groups; prunes dead weaks in place.
    pub fn live_download_groups(&self) -> Vec<Arc<DownloadGroupConsole>> {
        let Ok(mut groups) = self.download_groups.lock() else { return Vec::new(); };
        let mut result = Vec::new();
        groups.retain(|w| {
            if let Some(arc) = w.upgrade() {
                result.push(arc);
                true
            } else {
                false
            }
        });
        result
    }

    pub fn new_monitor(
        self: &Arc<Self>,
        tag: String,
        semaphore: Arc<crate::utils::adjustable_semaphore::AdjustableSemaphore>,
        bounds: (usize, usize),
        adjustment_enabled: bool,
    ) -> Arc<MonitorConsole> {
        let m = MonitorConsole::new(tag, semaphore, bounds, adjustment_enabled);
        if let Ok(mut monitors) = self.monitors.lock() {
            monitors.push(Arc::downgrade(&m));
        }
        m
    }

    pub fn monitor_snapshots(&self) -> Vec<MonitorSnapshot> {
        let live: Vec<Arc<MonitorConsole>> = {
            let Ok(mut monitors) = self.monitors.lock() else { return Vec::new(); };
            let mut result = Vec::new();
            monitors.retain(|w| {
                if let Some(arc) = w.upgrade() {
                    result.push(arc);
                    true
                } else {
                    false
                }
            });
            result
        };
        live.iter().map(|m| m.snapshot()).collect()
    }

    pub fn summary(&self, state: SessionState) -> SessionSummary {
        let n_upload_commits = self.live_upload_commits().len();
        let n_download_groups = self.live_download_groups().len();
        let n_monitors = self
            .monitors
            .lock()
            .map(|mut m| {
                m.retain(|w| w.upgrade().is_some());
                m.len()
            })
            .unwrap_or(0);
        SessionSummary {
            id: self.id.clone(),
            state,
            created_at: self.created_at,
            n_upload_commits,
            n_download_groups,
            n_monitors,
        }
    }

    pub fn detail(&self, state: SessionState) -> SessionDetail {
        let monitors = self.monitor_snapshots();
        let upload_commits = self
            .live_upload_commits()
            .iter()
            .map(|c| c.summary())
            .collect();
        let ended_upload_commits = self.ended_upload_commits();
        let download_groups = self
            .live_download_groups()
            .iter()
            .map(|g| g.summary())
            .collect();
        let ended_download_groups = self.ended_download_groups();
        SessionDetail {
            as_of: now_ms(),
            id: self.id.clone(),
            state,
            created_at: self.created_at,
            config: self.config.clone(),
            monitors,
            upload_commits,
            ended_upload_commits,
            download_groups,
            ended_download_groups,
        }
    }

    pub fn full(&self, state: SessionState) -> SessionFull {
        let detail = self.detail(state);
        let upload_commit_details = self
            .live_upload_commits()
            .iter()
            .map(|c| c.snapshot(true))
            .collect();
        let download_group_details = self
            .live_download_groups()
            .iter()
            .map(|g| g.snapshot(true))
            .collect();
        SessionFull {
            detail,
            upload_commit_details,
            download_group_details,
        }
    }
}

pub struct UploadCommitConsole {
    pub id: u64,
    pub created_at: u64,
    pub endpoint: Option<String>,
    state: Mutex<UploadCommitState>,
    progress_provider: Mutex<Option<ProgressProvider>>,
    dedup: Mutex<DedupSnapshot>,
    files: Mutex<HashMap<u64, Arc<FileUploadConsole>>>,
    completed_files: TimestampedRing<FileUploadSnapshot>,
    file_counts: Mutex<FileCounts>,
    // ct_file_id (CompletionTracker id) -> console file id, set by instrumentation
    ct_file_map: Mutex<HashMap<u64, u64>>,
    xorbs: Mutex<HashMap<String, Arc<XorbConsole>>>,
    xorb_counts: Mutex<XorbCounts>,
    recent_xorbs: TimestampedRing<XorbSnapshot>,
    // xorb_hash -> file ids depending on it (for n_files + dep completion fan-out)
    xorb_files: Mutex<HashMap<String, Vec<u64>>>,
    shards: Mutex<Vec<ShardSnapshot>>,
    staging: Mutex<(usize, u64)>, // (n_xorbs staged, approx size)
    session: Weak<SessionConsole>,
    finalized: Mutex<bool>,
}

impl UploadCommitConsole {
    pub fn new(scope: Option<&Arc<SessionConsole>>, endpoint: Option<String>) -> Arc<Self> {
        let arc = Arc::new(Self {
            id: crate::utils::UniqueId::new().0,
            created_at: now_ms(),
            endpoint,
            state: Mutex::new(UploadCommitState::Active),
            progress_provider: Mutex::new(None),
            dedup: Mutex::new(DedupSnapshot::default()),
            files: Mutex::new(HashMap::new()),
            completed_files: TimestampedRing::new(RECENT_RING_CAPACITY),
            file_counts: Mutex::new(FileCounts::default()),
            ct_file_map: Mutex::new(HashMap::new()),
            xorbs: Mutex::new(HashMap::new()),
            xorb_counts: Mutex::new(XorbCounts::default()),
            recent_xorbs: TimestampedRing::new(RECENT_RING_CAPACITY),
            xorb_files: Mutex::new(HashMap::new()),
            shards: Mutex::new(Vec::new()),
            staging: Mutex::new((0, 0)),
            session: scope.map(Arc::downgrade).unwrap_or_default(),
            finalized: Mutex::new(false),
        });
        if let Some(s) = scope {
            s.register_upload_commit(&arc);
        }
        arc
    }

    pub fn set_state(&self, s: UploadCommitState) {
        let Ok(mut state) = self.state.lock() else { return; };
        // terminal states are sticky: late idempotent hooks must not regress them
        if matches!(*state, UploadCommitState::Completed | UploadCommitState::Aborted) {
            return;
        }
        *state = s;
    }

    pub fn set_progress_provider(&self, p: ProgressProvider) {
        let Ok(mut pp) = self.progress_provider.lock() else { return; };
        *pp = Some(p);
    }

    pub fn set_dedup(&self, d: DedupSnapshot) {
        let Ok(mut dedup) = self.dedup.lock() else { return; };
        *dedup = d;
    }

    pub fn new_file(&self, id: u64, name: &str, size: Option<u64>) -> Arc<FileUploadConsole> {
        let file = Arc::new(FileUploadConsole {
            id,
            name: name.to_string(),
            created_at: now_ms(),
            size: Mutex::new(size),
            state: Mutex::new(FileUploadState::Queued),
            bytes_chunked: AtomicU64::new(0),
            n_chunks: AtomicU64::new(0),
            file_hash: Mutex::new(None),
            sha256: Mutex::new(None),
            dedup: Mutex::new(None),
            xorb_deps: Mutex::new(Vec::new()),
            shard_uploaded: Mutex::new(false),
            finished_at: Mutex::new(None),
        });
        let Ok(mut files) = self.files.lock() else { return file; };
        files.insert(id, file.clone());
        if let Ok(mut counts) = self.file_counts.lock() {
            counts.in_flight += 1;
        }
        file
    }

    pub fn map_ct_file(&self, ct_id: u64, file_id: u64) {
        let Ok(mut map) = self.ct_file_map.lock() else { return; };
        map.insert(ct_id, file_id);
    }

    /// Resolve ct_or_file_id through ct_file_map, falling back to treating it
    /// as a console file id directly.
    fn resolve_file_id(&self, ct_or_file_id: u64) -> u64 {
        let Ok(map) = self.ct_file_map.lock() else { return ct_or_file_id; };
        *map.get(&ct_or_file_id).unwrap_or(&ct_or_file_id)
    }

    pub fn register_file_xorb_dep(
        &self,
        ct_or_file_id: u64,
        xorb_hash: String,
        n_bytes: u64,
        is_external: bool,
    ) {
        let file_id = self.resolve_file_id(ct_or_file_id);
        // canonical per-file inner lock order: xorb_deps before state (never invert)
        // Merge dep into file (or push new) and potentially advance state
        {
            let Ok(files) = self.files.lock() else { return; };
            if let Some(file) = files.get(&file_id) {
                let Ok(mut deps) = file.xorb_deps.lock() else { return; };
                if let Some(existing) = deps.iter_mut().find(|d| d.xorb_hash == xorb_hash) {
                    // Duplicate registration: merge rather than double-count
                    existing.n_bytes += n_bytes;
                    existing.uploaded |= is_external;
                } else {
                    deps.push(XorbDepSnapshot {
                        xorb_hash: xorb_hash.clone(),
                        n_bytes,
                        uploaded: is_external,
                    });
                    // After appending a dep to a file in Processed state, advance to AwaitingXorbs
                    if let Ok(mut state) = file.state.lock() && *state == FileUploadState::Processed {
                        *state = FileUploadState::AwaitingXorbs;
                    }
                }
            }
        }
        // Record this file id in xorb_files only on first registration for this (xorb, file) pair
        let first_for_file = {
            let Ok(mut xf) = self.xorb_files.lock() else { return; };
            let file_ids = xf.entry(xorb_hash.clone()).or_default();
            if !file_ids.contains(&file_id) {
                file_ids.push(file_id);
                true
            } else {
                false
            }
        };
        // Bump xorb's n_files only on first registration for this (xorb, file) pair
        if first_for_file {
            let Ok(xorbs) = self.xorbs.lock() else { return; };
            if let Some(xorb) = xorbs.get(&xorb_hash) {
                xorb.n_files.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn xorb_formed(&self, hash: String, raw_bytes: u64, serialized_bytes: u64) {
        // Initialize n_files from deps that may have registered before xorb_formed was called
        let existing_file_count = {
            let Ok(xf) = self.xorb_files.lock() else { return; };
            xf.get(&hash).map(|v| v.len()).unwrap_or(0) as u64
        };
        let xorb = Arc::new(XorbConsole {
            hash: hash.clone(),
            created_at: now_ms(),
            state: Mutex::new(XorbState::Formed),
            raw_bytes,
            serialized_bytes,
            bytes_transferred: AtomicU64::new(0),
            n_files: AtomicU64::new(existing_file_count),
            finished_at: Mutex::new(None),
        });
        let Ok(mut xorbs) = self.xorbs.lock() else { return; };
        xorbs.insert(hash, xorb);
        if let Ok(mut counts) = self.xorb_counts.lock() {
            counts.formed += 1;
        }
    }

    pub fn xorb_state(&self, hash: &str, state: XorbState) {
        let Ok(xorbs) = self.xorbs.lock() else { return; };
        if let Some(xorb) = xorbs.get(hash) {
            let Ok(mut s) = xorb.state.lock() else { return; };
            *s = state;
        }
    }

    /// Stores bytes_transferred as monotonic max.
    pub fn xorb_transfer(&self, hash: &str, completed_bytes: u64) {
        let Ok(xorbs) = self.xorbs.lock() else { return; };
        if let Some(xorb) = xorbs.get(hash) {
            xorb.bytes_transferred.fetch_max(completed_bytes, Ordering::Relaxed);
        }
    }

    pub fn xorb_uploaded(&self, hash: &str, success: bool) {
        // Remove xorb from live map (may be None if xorb_formed was never called)
        let xorb_arc = {
            let Ok(mut xorbs) = self.xorbs.lock() else { return; };
            xorbs.remove(hash)
        };

        if let Some(xorb) = xorb_arc {
            // Set finished_at and final state on the xorb
            {
                let Ok(mut finished_at) = xorb.finished_at.lock() else { return; };
                *finished_at = Some(now_ms());
            }
            {
                let Ok(mut s) = xorb.state.lock() else { return; };
                *s = if success { XorbState::Uploaded } else { XorbState::Failed };
            }
            // Push to recent ring and update counts
            self.recent_xorbs.push(xorb.snapshot());
            {
                let Ok(mut counts) = self.xorb_counts.lock() else { return; };
                if success {
                    counts.uploaded += 1;
                } else {
                    counts.failed += 1;
                }
            }
        }

        // Remove hash from reverse map now that the xorb is done (both success and failure)
        // I1: prevent unbounded growth of xorb_files
        let file_ids = {
            let Ok(mut xf) = self.xorb_files.lock() else { return; };
            xf.remove(hash).unwrap_or_default()
        };

        // I2: only mark deps and advance file state on success
        if !success {
            return;
        }

        let Ok(files) = self.files.lock() else { return; };
        for file_id in &file_ids {
            if let Some(file) = files.get(file_id) {
                // canonical per-file inner lock order: xorb_deps before state (never invert)
                let all_uploaded = {
                    let Ok(mut deps) = file.xorb_deps.lock() else { continue; };
                    for dep in deps.iter_mut() {
                        if dep.xorb_hash == hash {
                            dep.uploaded = true;
                        }
                    }
                    deps.iter().all(|d| d.uploaded)
                };
                // Advance file from AwaitingXorbs to AwaitingShard if all deps uploaded
                {
                    let Ok(mut state) = file.state.lock() else { continue; };
                    if *state == FileUploadState::AwaitingXorbs && all_uploaded {
                        *state = FileUploadState::AwaitingShard;
                    }
                }
            }
        }
    }

    pub fn shard_staging(&self, n_xorbs: usize, size: u64) {
        let Ok(mut staging) = self.staging.lock() else { return; };
        *staging = (n_xorbs, size);
    }

    pub fn shard_discovered(&self, hash: String, size: u64) {
        let Ok(mut shards) = self.shards.lock() else { return; };
        shards.push(ShardSnapshot {
            hash: Some(hash),
            state: ShardState::Uploading,
            n_xorbs: 0,
            size,
        });
    }

    pub fn shard_uploaded(&self, hash: &str) {
        let Ok(mut shards) = self.shards.lock() else { return; };
        for shard in shards.iter_mut() {
            if shard.hash.as_deref() == Some(hash) {
                shard.state = ShardState::Uploaded;
            }
        }
    }

    pub fn all_shards_uploaded(&self) {
        // Flip every shard to Uploaded
        {
            let Ok(mut shards) = self.shards.lock() else { return; };
            for shard in shards.iter_mut() {
                shard.state = ShardState::Uploaded;
            }
        }
        // Set shard_uploaded and state Complete on all in-flight files, then retire them
        let file_ids: Vec<u64> = {
            let Ok(files) = self.files.lock() else { return; };
            files.keys().cloned().collect()
        };
        for id in file_ids {
            {
                let Ok(files) = self.files.lock() else { continue; };
                if let Some(file) = files.get(&id) {
                    if let Ok(mut su) = file.shard_uploaded.lock() {
                        *su = true;
                    }
                    if let Ok(mut s) = file.state.lock() {
                        *s = FileUploadState::Complete;
                    }
                    if let Ok(mut fa) = file.finished_at.lock() {
                        *fa = Some(now_ms());
                    }
                }
            }
            self.retire_file(id);
        }
    }

    pub fn retire_file(&self, id: u64) {
        let file = {
            let Ok(mut files) = self.files.lock() else { return; };
            files.remove(&id)
        };
        let Some(file) = file else { return; };
        let snap = file.snapshot();
        self.completed_files.push(snap.clone());
        // I1: prune ct_file_map entries that pointed at this file id
        if let Ok(mut map) = self.ct_file_map.lock() {
            map.retain(|_, v| *v != id);
        }
        let Ok(mut counts) = self.file_counts.lock() else { return; };
        counts.in_flight = counts.in_flight.saturating_sub(1);
        match snap.state {
            FileUploadState::Complete => counts.completed += 1,
            FileUploadState::Failed => counts.failed += 1,
            FileUploadState::Aborted => counts.aborted += 1,
            _ => {}
        }
    }

    #[cfg(test)]
    pub(crate) fn debug_reverse_map_sizes(&self) -> (usize, usize) {
        let ct_len = self.ct_file_map.lock().map(|m| m.len()).unwrap_or(0);
        let xf_len = self.xorb_files.lock().map(|m| m.len()).unwrap_or(0);
        (ct_len, xf_len)
    }

    pub fn finalize(&self, state: UploadCommitState) {
        // Check and set finalized atomically
        {
            let Ok(mut finalized) = self.finalized.lock() else { return; };
            if *finalized {
                return;
            }
            *finalized = true;
        }
        self.set_state(state);
        let detail = self.snapshot(true);
        if let Some(session) = self.session.upgrade() {
            session.record_ended_commit(detail);
        }
    }

    pub fn snapshot(&self, include_completed: bool) -> UploadCommitDetail {
        let state = self.state.lock().map(|s| *s).unwrap_or(UploadCommitState::Aborted);
        let dedup = self.dedup.lock().map(|d| d.clone()).unwrap_or_default();
        let progress = self
            .progress_provider
            .lock()
            .ok()
            .and_then(|pp| pp.as_ref().map(|f| f()));
        let files: Vec<FileUploadSnapshot> = self
            .files
            .lock()
            .map(|f| f.values().map(|fc| fc.snapshot()).collect())
            .unwrap_or_default();
        let completed_files = if include_completed {
            self.completed_files.snapshot()
        } else {
            Vec::new()
        };
        let file_counts = self.file_counts.lock().map(|c| c.clone()).unwrap_or_default();
        let in_flight_xorbs: Vec<XorbSnapshot> = self
            .xorbs
            .lock()
            .map(|x| x.values().map(|xc| xc.snapshot()).collect())
            .unwrap_or_default();
        let xorb_counts = self.xorb_counts.lock().map(|c| c.clone()).unwrap_or_default();
        let recent_xorbs = self.recent_xorbs.snapshot();
        let shards = self.shards.lock().map(|s| s.clone()).unwrap_or_default();
        let (n_xorbs_staged, staged_size) =
            self.staging.lock().map(|s| *s).unwrap_or((0, 0));
        let mut all_shards = shards;
        if n_xorbs_staged > 0 || staged_size > 0 {
            all_shards.insert(
                0,
                ShardSnapshot {
                    hash: None,
                    state: ShardState::Staging,
                    n_xorbs: n_xorbs_staged,
                    size: staged_size,
                },
            );
        }
        UploadCommitDetail {
            as_of: now_ms(),
            id: self.id,
            state,
            created_at: self.created_at,
            endpoint: self.endpoint.clone(),
            progress,
            dedup,
            files,
            completed_files,
            file_counts,
            xorbs: XorbsSnapshot {
                in_flight: in_flight_xorbs,
                counts: xorb_counts,
                recent: recent_xorbs,
            },
            shards: all_shards,
        }
    }

    pub fn summary(&self) -> UploadCommitSummary {
        let state = self.state.lock().map(|s| *s).unwrap_or(UploadCommitState::Aborted);
        let counts = self.file_counts.lock().map(|c| c.clone()).unwrap_or_default();
        let progress = self
            .progress_provider
            .lock()
            .ok()
            .and_then(|pp| pp.as_ref().map(|f| f()));
        UploadCommitSummary {
            id: self.id,
            state,
            created_at: self.created_at,
            endpoint: self.endpoint.clone(),
            n_files_in_flight: counts.in_flight,
            n_files_completed: counts.completed,
            progress,
        }
    }
}

impl Drop for UploadCommitConsole {
    fn drop(&mut self) {
        let finalized = self.finalized.lock().map(|f| *f).unwrap_or(true);
        if !finalized {
            self.finalize(UploadCommitState::Aborted);
        }
    }
}

pub struct FileUploadConsole {
    pub id: u64,
    pub name: String,
    pub created_at: u64,
    size: Mutex<Option<u64>>,
    state: Mutex<FileUploadState>,
    bytes_chunked: AtomicU64,
    n_chunks: AtomicU64,
    file_hash: Mutex<Option<String>>,
    sha256: Mutex<Option<String>>,
    dedup: Mutex<Option<DedupSnapshot>>,
    xorb_deps: Mutex<Vec<XorbDepSnapshot>>,
    shard_uploaded: Mutex<bool>,
    finished_at: Mutex<Option<u64>>,
}

impl FileUploadConsole {
    pub fn set_state(&self, s: FileUploadState) {
        let Ok(mut state) = self.state.lock() else { return; };
        // terminal states are sticky: late idempotent hooks must not regress them
        if matches!(*state, FileUploadState::Complete | FileUploadState::Failed | FileUploadState::Aborted) {
            return;
        }
        *state = s;
        match s {
            FileUploadState::Complete
            | FileUploadState::Failed
            | FileUploadState::Aborted => {
                let Ok(mut fa) = self.finished_at.lock() else { return; };
                if fa.is_none() {
                    *fa = Some(now_ms());
                }
            }
            _ => {}
        }
    }

    pub fn add_chunked_bytes(&self, n: u64, chunks: u64) {
        self.bytes_chunked.fetch_add(n, Ordering::Relaxed);
        self.n_chunks.fetch_add(chunks, Ordering::Relaxed);
    }

    pub fn set_hash(&self, hash: String, sha256: Option<String>) {
        if let Ok(mut h) = self.file_hash.lock() {
            *h = Some(hash);
        }
        if let Ok(mut s) = self.sha256.lock() {
            *s = sha256;
        }
    }

    pub fn set_dedup(&self, d: DedupSnapshot) {
        let Ok(mut dedup) = self.dedup.lock() else { return; };
        *dedup = Some(d);
    }

    pub fn set_size(&self, size: u64) {
        let Ok(mut s) = self.size.lock() else { return; };
        *s = Some(size);
    }

    pub fn snapshot(&self) -> FileUploadSnapshot {
        let size = self.size.lock().map(|s| *s).unwrap_or(None);
        let state = self.state.lock().map(|s| *s).unwrap_or(FileUploadState::Aborted);
        let file_hash = self.file_hash.lock().map(|h| h.clone()).unwrap_or(None);
        let sha256 = self.sha256.lock().map(|s| s.clone()).unwrap_or(None);
        let dedup = self.dedup.lock().map(|d| d.clone()).unwrap_or(None);
        let xorb_deps = self.xorb_deps.lock().map(|d| d.clone()).unwrap_or_default();
        let shard_uploaded = self.shard_uploaded.lock().map(|s| *s).unwrap_or(false);
        let finished_at = self.finished_at.lock().map(|f| *f).unwrap_or(None);
        FileUploadSnapshot {
            id: self.id,
            name: self.name.clone(),
            size,
            state,
            bytes_chunked: self.bytes_chunked.load(Ordering::Relaxed),
            n_chunks: self.n_chunks.load(Ordering::Relaxed),
            file_hash,
            sha256,
            dedup,
            xorb_deps,
            shard_uploaded,
            created_at: self.created_at,
            finished_at,
        }
    }
}

pub struct XorbConsole {
    pub hash: String,
    pub created_at: u64,
    state: Mutex<XorbState>,
    pub raw_bytes: u64,
    pub serialized_bytes: u64,
    bytes_transferred: AtomicU64,
    n_files: AtomicU64,
    finished_at: Mutex<Option<u64>>,
}

impl XorbConsole {
    pub fn snapshot(&self) -> XorbSnapshot {
        let state = self.state.lock().map(|s| *s).unwrap_or(XorbState::Failed);
        let finished_at = self.finished_at.lock().map(|f| *f).unwrap_or(None);
        XorbSnapshot {
            hash: self.hash.clone(),
            state,
            raw_bytes: self.raw_bytes,
            serialized_bytes: self.serialized_bytes,
            bytes_transferred: self.bytes_transferred.load(Ordering::Relaxed),
            n_files: self.n_files.load(Ordering::Relaxed) as usize,
            created_at: self.created_at,
            finished_at,
        }
    }
}

// ---- concurrency monitor ----

pub struct MonitorConsole {
    pub tag: String,
    semaphore: Arc<crate::utils::adjustable_semaphore::AdjustableSemaphore>,
    bounds: (usize, usize),
    adjustment_enabled: bool,
    bytes_sent: AtomicU64,
    success: Mutex<Option<SuccessModelSnapshot>>,
    latency: Mutex<Option<LatencyModelSnapshot>>,
    limit_history: TimestampedRing<usize>,
}

impl MonitorConsole {
    pub fn new(
        tag: String,
        semaphore: Arc<crate::utils::adjustable_semaphore::AdjustableSemaphore>,
        bounds: (usize, usize),
        adjustment_enabled: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            tag,
            semaphore,
            bounds,
            adjustment_enabled,
            bytes_sent: AtomicU64::new(0),
            success: Mutex::new(None),
            latency: Mutex::new(None),
            limit_history: TimestampedRing::new(256),
        })
    }

    pub fn record_limit(&self, limit: usize) {
        self.limit_history.push(limit);
    }

    pub fn set_success_model(&self, s: SuccessModelSnapshot) {
        let Ok(mut success) = self.success.lock() else { return; };
        *success = Some(s);
    }

    pub fn set_latency_model(&self, l: LatencyModelSnapshot) {
        let Ok(mut latency) = self.latency.lock() else { return; };
        *latency = Some(l);
    }

    pub fn set_bytes_sent(&self, n: u64) {
        self.bytes_sent.store(n, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MonitorSnapshot {
        let total_permits = self.semaphore.total_permits() as usize;
        let available_permits = self.semaphore.available_permits() as usize;
        let active_permits = self.semaphore.active_permits() as usize;
        let success = self.success.lock().map(|s| s.clone()).unwrap_or(None);
        let latency = self.latency.lock().map(|l| l.clone()).unwrap_or(None);
        let limit_history = self.limit_history.snapshot();
        MonitorSnapshot {
            tag: self.tag.clone(),
            total_permits,
            active_permits,
            available_permits,
            bounds: PermitBounds { min: self.bounds.0, max: self.bounds.1 },
            adjustment_enabled: self.adjustment_enabled,
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            success,
            latency,
            limit_history,
        }
    }
}

// ---- download side ----

pub struct DownloadGroupConsole {
    pub id: u64,
    pub created_at: u64,
    pub endpoint: Option<String>,
    kind: Mutex<DownloadGroupKind>,
    state: Mutex<DownloadGroupState>,
    progress_provider: Mutex<Option<ProgressProvider>>,
    items_provider: Mutex<Option<ItemBytesProvider>>,
    files: Mutex<HashMap<u64, Arc<DownloadFileConsole>>>,
    completed_files: TimestampedRing<FileDownloadSnapshot>,
    file_counts: Mutex<FileCounts>,
    session: Weak<SessionConsole>,
    finalized: Mutex<bool>,
}

impl DownloadGroupConsole {
    pub fn new(
        scope: Option<&Arc<SessionConsole>>,
        kind: DownloadGroupKind,
        endpoint: Option<String>,
    ) -> Arc<Self> {
        let arc = Arc::new(Self {
            id: crate::utils::UniqueId::new().0,
            created_at: now_ms(),
            endpoint,
            kind: Mutex::new(kind),
            state: Mutex::new(DownloadGroupState::Active),
            progress_provider: Mutex::new(None),
            items_provider: Mutex::new(None),
            files: Mutex::new(HashMap::new()),
            completed_files: TimestampedRing::new(RECENT_RING_CAPACITY),
            file_counts: Mutex::new(FileCounts::default()),
            session: scope.map(Arc::downgrade).unwrap_or_default(),
            finalized: Mutex::new(false),
        });
        if let Some(s) = scope {
            s.register_download_group(&arc);
        }
        arc
    }

    pub fn set_kind(&self, kind: DownloadGroupKind) {
        let Ok(mut k) = self.kind.lock() else { return; };
        *k = kind;
    }

    pub fn set_state(&self, s: DownloadGroupState) {
        let Ok(mut state) = self.state.lock() else { return; };
        // terminal states are sticky
        if matches!(*state, DownloadGroupState::Finished | DownloadGroupState::Aborted) {
            return;
        }
        *state = s;
    }

    pub fn set_progress_provider(&self, p: ProgressProvider) {
        let Ok(mut pp) = self.progress_provider.lock() else { return; };
        *pp = Some(p);
    }

    pub fn set_items_provider(&self, p: ItemBytesProvider) {
        let Ok(mut ip) = self.items_provider.lock() else { return; };
        *ip = Some(p);
    }

    pub fn new_file(
        &self,
        id: u64,
        name: &str,
        hash: Option<String>,
        range: Option<(u64, u64)>,
    ) -> Arc<DownloadFileConsole> {
        let f = Arc::new(DownloadFileConsole {
            id,
            name: name.to_string(),
            created_at: now_ms(),
            file_hash: Mutex::new(hash),
            requested_range: Mutex::new(range),
            state: Mutex::new(FileDownloadState::Queued),
            queue_depth: AtomicUsize::new(0),
            prefetched_pos: AtomicU64::new(0),
            active_pos: AtomicU64::new(0),
            completion_rate: Mutex::new(None),
            term_blocks: Mutex::new(HashMap::new()),
            consumed_blocks: AtomicU64::new(0),
            finished_at: Mutex::new(None),
        });
        if let Ok(mut files) = self.files.lock() {
            files.insert(id, Arc::clone(&f));
        }
        if let Ok(mut counts) = self.file_counts.lock() {
            counts.in_flight = counts.in_flight.saturating_add(1);
        }
        f
    }

    pub fn retire_file(&self, id: u64) {
        let removed = self.files.lock().ok().and_then(|mut files| files.remove(&id));
        if let Some(fc) = removed {
            let snap = {
                let items = self
                    .items_provider
                    .lock()
                    .ok()
                    .and_then(|ip| ip.as_ref().map(|f| f()))
                    .unwrap_or_default();
                fc.snapshot(&items)
            };
            let state = snap.state;
            self.completed_files.push(snap);
            if let Ok(mut counts) = self.file_counts.lock() {
                counts.in_flight = counts.in_flight.saturating_sub(1);
                match state {
                    FileDownloadState::Complete => counts.completed += 1,
                    FileDownloadState::Failed => counts.failed += 1,
                    FileDownloadState::Aborted => counts.aborted += 1,
                    _ => {}
                }
            }
        }
    }

    pub fn finalize(&self, state: DownloadGroupState) {
        {
            let Ok(mut finalized) = self.finalized.lock() else { return; };
            if *finalized {
                return;
            }
            *finalized = true;
        }
        self.set_state(state);
        let detail = self.snapshot(true);
        if let Some(session) = self.session.upgrade() {
            session.record_ended_download_group(detail);
        }
    }

    pub fn snapshot(&self, include_completed: bool) -> DownloadGroupDetail {
        let state = self.state.lock().map(|s| *s).unwrap_or(DownloadGroupState::Aborted);
        let kind = self.kind.lock().map(|k| *k).unwrap_or(DownloadGroupKind::Files);
        let progress = self
            .progress_provider
            .lock()
            .ok()
            .and_then(|pp| pp.as_ref().map(|f| f()));
        let items = self
            .items_provider
            .lock()
            .ok()
            .and_then(|ip| ip.as_ref().map(|f| f()))
            .unwrap_or_default();
        let files: Vec<FileDownloadSnapshot> = self
            .files
            .lock()
            .map(|f| f.values().map(|fc| fc.snapshot(&items)).collect())
            .unwrap_or_default();
        let completed_files = if include_completed {
            self.completed_files.snapshot()
        } else {
            Vec::new()
        };
        let file_counts = self.file_counts.lock().map(|c| c.clone()).unwrap_or_default();
        DownloadGroupDetail {
            as_of: now_ms(),
            id: self.id,
            kind,
            state,
            created_at: self.created_at,
            endpoint: self.endpoint.clone(),
            progress,
            files,
            completed_files,
            file_counts,
        }
    }

    pub fn summary(&self) -> DownloadGroupSummary {
        let state = self.state.lock().map(|s| *s).unwrap_or(DownloadGroupState::Aborted);
        let kind = self.kind.lock().map(|k| *k).unwrap_or(DownloadGroupKind::Files);
        let counts = self.file_counts.lock().map(|c| c.clone()).unwrap_or_default();
        let progress = self
            .progress_provider
            .lock()
            .ok()
            .and_then(|pp| pp.as_ref().map(|f| f()));
        DownloadGroupSummary {
            id: self.id,
            kind,
            state,
            created_at: self.created_at,
            endpoint: self.endpoint.clone(),
            n_files_in_flight: counts.in_flight,
            n_files_completed: counts.completed,
            progress,
        }
    }
}

impl Drop for DownloadGroupConsole {
    fn drop(&mut self) {
        let finalized = self.finalized.lock().map(|f| *f).unwrap_or(true);
        if !finalized {
            self.finalize(DownloadGroupState::Aborted);
        }
    }
}

pub struct DownloadFileConsole {
    pub id: u64,
    pub name: String,
    pub created_at: u64,
    file_hash: Mutex<Option<String>>,
    requested_range: Mutex<Option<(u64, u64)>>,
    state: Mutex<FileDownloadState>,
    queue_depth: AtomicUsize,
    prefetched_pos: AtomicU64,
    active_pos: AtomicU64,
    completion_rate: Mutex<Option<f64>>,
    term_blocks: Mutex<HashMap<u64, Arc<TermBlockConsole>>>,
    consumed_blocks: AtomicU64,
    finished_at: Mutex<Option<u64>>,
}

impl DownloadFileConsole {
    pub fn set_state(&self, s: FileDownloadState) {
        let Ok(mut state) = self.state.lock() else { return; };
        // terminal states are sticky
        if matches!(*state, FileDownloadState::Complete | FileDownloadState::Failed | FileDownloadState::Aborted) {
            return;
        }
        *state = s;
        match s {
            FileDownloadState::Complete
            | FileDownloadState::Failed
            | FileDownloadState::Aborted => {
                let Ok(mut fa) = self.finished_at.lock() else { return; };
                if fa.is_none() {
                    *fa = Some(now_ms());
                }
            }
            _ => {}
        }
    }

    pub fn set_prefetch(
        &self,
        queue_depth: usize,
        prefetched: u64,
        active: u64,
        rate: Option<f64>,
    ) {
        self.queue_depth.store(queue_depth, Ordering::Relaxed);
        self.prefetched_pos.store(prefetched, Ordering::Relaxed);
        self.active_pos.store(active, Ordering::Relaxed);
        if let Ok(mut r) = self.completion_rate.lock() {
            *r = rate;
        }
    }

    pub fn new_term_block(&self, block_id: u64, byte_range: (u64, u64)) -> Arc<TermBlockConsole> {
        let b = Arc::new(TermBlockConsole {
            block_id,
            byte_range,
            created_at: now_ms(),
            state: Mutex::new(TermState::Enqueued),
            terms: Mutex::new(Vec::new()),
            fetched_at: Mutex::new(None),
        });
        if let Ok(mut blocks) = self.term_blocks.lock() {
            blocks.insert(block_id, Arc::clone(&b));
        }
        b
    }

    /// Remove the block from the map and bump consumed_blocks counter.
    pub fn consume_term_block(&self, block_id: u64) {
        let removed = self.term_blocks.lock().ok().and_then(|mut m| m.remove(&block_id));
        if let Some(b) = removed {
            b.set_state(TermState::Consumed);
            self.consumed_blocks.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self, items: &HashMap<u64, (u64, u64)>) -> FileDownloadSnapshot {
        let (total_bytes, bytes_completed) = items.get(&self.id).copied().unwrap_or((0, 0));
        let state = self.state.lock().map(|s| *s).unwrap_or(FileDownloadState::Aborted);
        let file_hash = self.file_hash.lock().map(|h| h.clone()).unwrap_or(None);
        let requested_range = self.requested_range.lock().map(|r| *r).unwrap_or(None);
        let completion_rate = self.completion_rate.lock().map(|r| *r).unwrap_or(None);
        let term_blocks: Vec<TermBlockSnapshot> = self
            .term_blocks
            .lock()
            .map(|m| m.values().map(|b| b.snapshot()).collect())
            .unwrap_or_default();
        let finished_at = self.finished_at.lock().map(|f| *f).unwrap_or(None);
        let queue_depth = self.queue_depth.load(Ordering::Relaxed);
        let prefetch = if queue_depth > 0
            || self.prefetched_pos.load(Ordering::Relaxed) > 0
            || self.active_pos.load(Ordering::Relaxed) > 0
            || completion_rate.is_some()
        {
            Some(PrefetchSnapshot {
                queue_depth,
                prefetched_byte_position: self.prefetched_pos.load(Ordering::Relaxed),
                active_byte_position: self.active_pos.load(Ordering::Relaxed),
                completion_rate_bps: completion_rate,
            })
        } else {
            None
        };
        FileDownloadSnapshot {
            id: self.id,
            name: self.name.clone(),
            file_hash,
            requested_range,
            total_bytes,
            bytes_completed,
            state,
            prefetch,
            term_blocks,
            consumed_blocks: self.consumed_blocks.load(Ordering::Relaxed),
            created_at: self.created_at,
            finished_at,
        }
    }
}

pub struct TermBlockConsole {
    pub block_id: u64,
    pub byte_range: (u64, u64),
    pub created_at: u64,
    state: Mutex<TermState>,
    terms: Mutex<Vec<TermInfo>>,
    fetched_at: Mutex<Option<u64>>,
}

impl TermBlockConsole {
    pub fn set_state(&self, s: TermState) {
        let Ok(mut state) = self.state.lock() else { return; };
        // Consumed is terminal
        if matches!(*state, TermState::Consumed) {
            return;
        }
        *state = s;
    }

    /// Mark block as fetched; stores the resolved terms and stamps fetched_at.
    pub fn resolved(&self, term_list: Vec<TermInfo>) {
        // consumed blocks are frozen
        {
            let Ok(state) = self.state.lock() else { return; };
            if matches!(*state, TermState::Consumed) {
                return;
            }
        }
        if let Ok(mut terms) = self.terms.lock() {
            *terms = term_list;
        }
        if let Ok(mut fa) = self.fetched_at.lock() && fa.is_none() {
            *fa = Some(now_ms());
        }
        self.set_state(TermState::Fetched);
    }

    pub fn snapshot(&self) -> TermBlockSnapshot {
        let state = self.state.lock().map(|s| *s).unwrap_or(TermState::Enqueued);
        let terms = self.terms.lock().map(|t| t.clone()).unwrap_or_default();
        let fetched_at = self.fetched_at.lock().map(|f| *f).unwrap_or(None);
        TermBlockSnapshot {
            block_id: self.block_id,
            byte_range: self.byte_range,
            state,
            terms,
            created_at: self.created_at,
            fetched_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> Arc<SessionConsole> {
        SessionConsole::new("test-session".to_string(), vec![])
    }

    #[test]
    fn file_walks_the_upload_state_machine() {
        let commit = UploadCommitConsole::new(Some(&scope()), Some("local://x".into()));
        let f = commit.new_file(12, "model.bin", Some(4096));
        assert_eq!(f.snapshot().state, FileUploadState::Queued);
        f.set_state(FileUploadState::Chunking);
        f.add_chunked_bytes(1024, 2);
        f.set_hash("abcd".into(), None);
        f.set_state(FileUploadState::Processed);
        let snap = f.snapshot();
        assert_eq!(snap.bytes_chunked, 1024);
        assert_eq!(snap.n_chunks, 2);
        assert_eq!(snap.file_hash.as_deref(), Some("abcd"));
    }

    #[test]
    fn xorb_dep_completion_moves_file_to_awaiting_shard() {
        let commit = UploadCommitConsole::new(Some(&scope()), None);
        let f = commit.new_file(1, "a", None);
        commit.register_file_xorb_dep(1, "x1".into(), 100, false);
        commit.register_file_xorb_dep(1, "x2".into(), 100, true); // external = already uploaded
        f.set_state(FileUploadState::AwaitingXorbs);
        commit.xorb_uploaded("x1", true);
        let snap = f.snapshot();
        assert!(snap.xorb_deps.iter().all(|d| d.uploaded));
        assert_eq!(snap.state, FileUploadState::AwaitingShard);
    }

    #[test]
    fn completed_files_fold_into_ring_and_counts() {
        let commit = UploadCommitConsole::new(Some(&scope()), None);
        for i in 0..5 {
            let f = commit.new_file(i, "f", None);
            f.set_state(FileUploadState::Complete);
            commit.retire_file(i);
        }
        let detail = commit.snapshot(true);
        assert_eq!(detail.files.len(), 0);
        assert_eq!(detail.file_counts.completed, 5);
        assert_eq!(detail.completed_files.len(), 5);
    }

    #[test]
    fn xorb_lifecycle_and_recent_ring() {
        let commit = UploadCommitConsole::new(Some(&scope()), None);
        commit.xorb_formed("h1".into(), 1000, 900);
        commit.xorb_state("h1", XorbState::Uploading);
        commit.xorb_transfer("h1", 450);
        commit.xorb_uploaded("h1", true);
        let detail = commit.snapshot(false);
        assert!(detail.xorbs.in_flight.is_empty());
        assert_eq!(detail.xorbs.counts.uploaded, 1);
        assert_eq!(detail.xorbs.recent.len(), 1);
    }

    #[test]
    fn drop_of_active_commit_records_aborted_summary_in_session() {
        let s = scope();
        {
            let commit = UploadCommitConsole::new(Some(&s), None);
            commit.new_file(1, "f", None);
            drop(commit);
        }
        let ended = s.ended_upload_commits();
        assert_eq!(ended.len(), 1);
        assert_eq!(ended[0].state, UploadCommitState::Aborted);
    }

    #[test]
    fn finalized_commit_records_completed_summary() {
        let s = scope();
        let commit = UploadCommitConsole::new(Some(&s), None);
        commit.set_state(UploadCommitState::Committing);
        commit.finalize(UploadCommitState::Completed);
        drop(commit);
        let ended = s.ended_upload_commits();
        assert_eq!(ended.len(), 1);
        assert_eq!(ended[0].state, UploadCommitState::Completed);
    }

    #[test]
    fn failed_xorb_does_not_mark_deps_uploaded() {
        let commit = UploadCommitConsole::new(Some(&scope()), None);
        let f = commit.new_file(1, "a", None);
        commit.register_file_xorb_dep(1, "x1".into(), 100, false);
        f.set_state(FileUploadState::AwaitingXorbs);
        commit.xorb_formed("x1".into(), 100, 100);
        commit.xorb_uploaded("x1", false);
        let snap = f.snapshot();
        assert!(snap.xorb_deps.iter().all(|d| !d.uploaded));
        assert_eq!(snap.state, FileUploadState::AwaitingXorbs);
        let detail = commit.snapshot(false);
        assert_eq!(detail.xorbs.counts.failed, 1);
    }

    #[test]
    fn duplicate_dep_registration_merges() {
        let commit = UploadCommitConsole::new(Some(&scope()), None);
        let f = commit.new_file(1, "a", None);
        commit.register_file_xorb_dep(1, "x1".into(), 100, false);
        commit.register_file_xorb_dep(1, "x1".into(), 50, false);
        commit.xorb_formed("x1".into(), 100, 100);
        let snap = f.snapshot();
        assert_eq!(snap.xorb_deps.len(), 1);
        assert_eq!(snap.xorb_deps[0].n_bytes, 150);
        let detail = commit.snapshot(false);
        assert_eq!(detail.xorbs.in_flight[0].n_files, 1);
    }

    #[test]
    fn terminal_file_state_is_sticky() {
        let commit = UploadCommitConsole::new(Some(&scope()), None);
        let f = commit.new_file(1, "a", None);
        f.set_state(FileUploadState::Complete);
        f.set_state(FileUploadState::Chunking);
        assert_eq!(f.snapshot().state, FileUploadState::Complete);
    }

    #[test]
    fn reverse_maps_shrink_as_work_retires() {
        let commit = UploadCommitConsole::new(Some(&scope()), None);
        let f = commit.new_file(1, "a", None);
        commit.map_ct_file(77, 1);
        commit.register_file_xorb_dep(77, "x1".into(), 100, false);
        f.set_state(FileUploadState::AwaitingXorbs);
        commit.xorb_uploaded("x1", true);
        commit.retire_file(1);
        assert_eq!(commit.debug_reverse_map_sizes(), (0, 0));
    }

    #[test]
    fn term_block_lifecycle() {
        let group = DownloadGroupConsole::new(Some(&scope()), DownloadGroupKind::Files, None);
        let f = group.new_file(3, "out.bin", Some("ffff".into()), None);
        let b = f.new_term_block(0, (0, 1 << 20));
        assert_eq!(b.snapshot().state, TermState::Enqueued);
        b.set_state(TermState::Fetching);
        b.resolved(vec![TermInfo { xorb_hash: "aa".into(), chunk_range: (0, 4), byte_range: (0, 65536) }]);
        assert_eq!(b.snapshot().state, TermState::Fetched);
        f.consume_term_block(0);
        let snap = f.snapshot(&HashMap::new());
        assert_eq!(snap.consumed_blocks, 1);
        assert!(snap.term_blocks.is_empty());
    }

    #[test]
    fn download_file_bytes_come_from_items_provider() {
        let group = DownloadGroupConsole::new(Some(&scope()), DownloadGroupKind::Files, None);
        let f = group.new_file(9, "x", None, None);
        f.set_state(FileDownloadState::Reconstructing);
        let mut items = HashMap::new();
        items.insert(9u64, (1000u64, 250u64));
        let snap = f.snapshot(&items);
        assert_eq!(snap.total_bytes, 1000);
        assert_eq!(snap.bytes_completed, 250);
    }

    #[test]
    fn group_drop_while_active_is_aborted() {
        let s = scope();
        {
            let g = DownloadGroupConsole::new(Some(&s), DownloadGroupKind::Stream, None);
            g.new_file(1, "f", None, None);
        }
        let ended = s.ended_download_groups();
        assert_eq!(ended.len(), 1);
        assert_eq!(ended[0].state, DownloadGroupState::Aborted);
        assert_eq!(ended[0].kind, DownloadGroupKind::Stream);
    }

    #[test]
    fn monitor_snapshot_reads_semaphore_gauges_and_history() {
        let sem = crate::utils::adjustable_semaphore::AdjustableSemaphore::new(4, (1, 16));
        let m = MonitorConsole::new("upload".into(), sem, (1, 16), true);
        m.record_limit(5);
        m.record_limit(6);
        m.set_success_model(SuccessModelSnapshot {
            success_ratio: 0.9,
            thresholds: Thresholds { increase: 0.8, decrease: 0.5 },
            recommended_adjustment: AdjustmentRecommendation::Increase,
        });
        m.set_bytes_sent(1234);
        let snap = m.snapshot();
        assert_eq!(snap.tag, "upload");
        assert_eq!(snap.limit_history.len(), 2);
        assert_eq!(snap.bytes_sent, 1234);
        assert!(snap.success.is_some());
        assert!(snap.total_permits >= 1);
    }
}
