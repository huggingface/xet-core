use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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

    pub fn summary(&self, state: SessionState) -> SessionSummary {
        let n_upload_commits = self.live_upload_commits().len();
        SessionSummary {
            id: self.id.clone(),
            state,
            created_at: self.created_at,
            n_upload_commits,
            n_download_groups: 0,  // Task 5 makes this real
            n_monitors: 0,         // Task 5 makes this real
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
}
