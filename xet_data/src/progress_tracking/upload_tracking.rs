use std::collections::BTreeSet;
use std::collections::hash_map::Entry as HashMapEntry;
use std::mem::take;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use more_asserts::{debug_assert_ge, debug_assert_le};
#[cfg(target_family = "wasm")]
use tokio_with_wasm::alias as tokio;
use xet_client::cas_types::ShardUploadEvent;
use xet_core_structures::MerkleHashMap;
use xet_core_structures::merklehash::MerkleHash;
use xet_runtime::utils::UniqueId;

use super::progress_types::{ItemProgressUpdater, UploadGroupProgress};
use crate::error::{DataError, Result};

pub struct FileXorbDependency {
    pub file_id: u64,
    pub xorb_hash: MerkleHash,
    pub n_bytes: u64,
    pub is_external: bool,
}

/// A type with with to track a File ID; reporting is done by Arc<str>, but
/// this ensures the bookkeeping is correct across duplicates and speeds up the
/// updates.
pub type CompletionTrackerFileId = u64;

/// Keeps track of which files depend on a given xorb.
#[derive(Default)]
struct XorbDependency {
    /// List of file indices that need this xorb.
    file_indices: BTreeSet<usize>,

    /// Number of bytes completed so far
    completed_bytes: u64,

    /// Number of bytes in that xorb.
    xorb_size: u64,

    /// True if the xorb has already been updated successfully.
    is_completed: bool,
}

#[derive(Default, Debug)]
struct XorbPartCompletionStats {
    completed_bytes: u64,
    n_bytes: u64,
}

/// Represents a file that depends on one or more xorbs.
struct FileDependency {
    tracking_id: UniqueId,
    updater: Arc<ItemProgressUpdater>,
    name: Arc<str>,
    total_bytes: u64,
    is_final_size_known: bool,
    completed_bytes: u64,
    remaining_xorbs_parts: MerkleHashMap<XorbPartCompletionStats>,
    /// Set once the file is given up before it finished; see `CompletionTracker::abandon_file`.
    is_abandoned: bool,
}

/// Tracks all files and all xorbs, allowing you to register file
/// dependencies on xorbs and then mark xorbs as completed when they
/// are fully uploaded.
#[derive(Default)]
struct CompletionTrackerImpl {
    files: Vec<FileDependency>,
    xorbs: MerkleHashMap<XorbDependency>,

    total_upload_bytes: u64,
    total_upload_bytes_completed: u64,

    total_bytes: u64,
    total_bytes_completed: u64,
}

pub struct CompletionTracker {
    inner: Mutex<CompletionTrackerImpl>,
    group_progress: Arc<UploadGroupProgress>,
}

impl CompletionTrackerImpl {
    fn register_new_file(
        &mut self,
        updater: Arc<ItemProgressUpdater>,
        n_bytes: Option<u64>,
    ) -> CompletionTrackerFileId {
        let (total_bytes, is_final_size_known) = match n_bytes {
            Some(size) => (size, true),
            None => (0, false),
        };

        updater.update_item_size(total_bytes, n_bytes.is_some());

        let file_id = self.files.len() as CompletionTrackerFileId;
        let tracking_id = updater.item().id;
        let name = updater.item().name.clone();

        let file_dependency = FileDependency {
            tracking_id,
            updater,
            name,
            total_bytes,
            is_final_size_known,
            completed_bytes: 0,
            remaining_xorbs_parts: MerkleHashMap::new(),
            is_abandoned: false,
        };

        self.files.push(file_dependency);
        self.total_bytes += total_bytes;

        file_id
    }

    fn increment_file_size(&mut self, file_id: CompletionTrackerFileId, size_increment: u64) {
        let file_entry = &mut self.files[file_id as usize];

        if file_entry.is_final_size_known {
            return;
        }

        file_entry.total_bytes += size_increment;
        self.total_bytes += size_increment;

        debug_assert_ge!(file_entry.total_bytes, file_entry.completed_bytes);
        debug_assert_ge!(self.total_bytes, self.total_bytes_completed);

        file_entry.updater.update_item_size(file_entry.total_bytes, false);
    }

    fn register_dependencies(&mut self, dependencies: &[FileXorbDependency]) {
        let mut file_bytes_processed = 0;

        for dep in dependencies {
            let file_entry = &mut self.files[dep.file_id as usize];

            // An abandoned file's leftover background work can still report dependencies; they no
            // longer count toward progress.
            if file_entry.is_abandoned {
                continue;
            }

            if dep.is_external {
                file_entry.completed_bytes += dep.n_bytes;
                debug_assert_le!(file_entry.completed_bytes, file_entry.total_bytes);

                file_entry.updater.report_bytes_completed(dep.n_bytes);
                file_bytes_processed += dep.n_bytes;
            } else {
                debug_assert_ne!(dep.xorb_hash, MerkleHash::marker());

                let entry = self.xorbs.entry(dep.xorb_hash).or_default();

                if entry.is_completed {
                    file_entry.completed_bytes += dep.n_bytes;
                    debug_assert_le!(file_entry.completed_bytes, file_entry.total_bytes);

                    file_entry.updater.report_bytes_completed(dep.n_bytes);
                    file_bytes_processed += dep.n_bytes;
                } else {
                    entry.file_indices.insert(dep.file_id as usize);
                    file_entry.remaining_xorbs_parts.entry(dep.xorb_hash).or_default().n_bytes += dep.n_bytes;
                }
            }
        }

        self.total_bytes_completed += file_bytes_processed;
        debug_assert_le!(self.total_bytes_completed, self.total_bytes);
    }

    fn abandon_file(&mut self, tracking_id: UniqueId) {
        let Some(file_id) = self.files.iter().position(|f| f.tracking_id == tracking_id) else {
            return;
        };
        let file_entry = &mut self.files[file_id];
        if file_entry.is_abandoned {
            return;
        }
        file_entry.is_abandoned = true;
        // Also makes `increment_file_size` ignore the file from now on.
        file_entry.is_final_size_known = true;

        // Detach the file from the xorbs it still waits on. Those xorbs are still uploaded (other files may
        // reference them), so the transfer totals do not change.
        for xorb_hash in take(&mut file_entry.remaining_xorbs_parts).keys() {
            if let Some(xorb_entry) = self.xorbs.get_mut(xorb_hash) {
                xorb_entry.file_indices.remove(&file_id);
            }
        }

        self.total_bytes -= file_entry.total_bytes - file_entry.completed_bytes;
        file_entry.total_bytes = file_entry.completed_bytes;
        file_entry.updater.abandon();
    }

    fn register_new_xorb(&mut self, group: &Arc<UploadGroupProgress>, xorb_hash: MerkleHash, xorb_size: u64) -> bool {
        match self.xorbs.entry(xorb_hash) {
            HashMapEntry::Occupied(mut occupied_entry) => {
                let entry = occupied_entry.get_mut();
                if entry.xorb_size == 0 {
                    entry.xorb_size = xorb_size;
                    self.total_upload_bytes += xorb_size;
                    group.file_data.total_transfer_bytes.fetch_add(xorb_size, Ordering::Release);
                    true
                } else {
                    debug_assert_eq!(entry.xorb_size, xorb_size);
                    false
                }
            },
            HashMapEntry::Vacant(vacant_entry) => {
                vacant_entry.insert(XorbDependency {
                    file_indices: Default::default(),
                    xorb_size,
                    completed_bytes: 0,
                    is_completed: false,
                });

                self.total_upload_bytes += xorb_size;
                group.file_data.total_transfer_bytes.fetch_add(xorb_size, Ordering::Release);
                true
            },
        }
    }

    fn register_xorb_upload_completion(&mut self, group: &Arc<UploadGroupProgress>, xorb_hash: MerkleHash) {
        let (file_indices, byte_completion_increment) = {
            let entry = self.xorbs.entry(xorb_hash).or_default();

            if entry.is_completed {
                return;
            }

            let new_byte_increment = entry.xorb_size - entry.completed_bytes;
            entry.is_completed = true;

            (take(&mut entry.file_indices), new_byte_increment)
        };

        let mut file_bytes_processed = 0;

        for file_id in file_indices {
            let file_entry = &mut self.files[file_id];

            debug_assert!(file_entry.remaining_xorbs_parts.contains_key(&xorb_hash));

            let xorb_part = file_entry.remaining_xorbs_parts.remove(&xorb_hash).unwrap_or_default();
            debug_assert_le!(xorb_part.completed_bytes, xorb_part.n_bytes);

            let n_bytes_remaining = xorb_part.n_bytes - xorb_part.completed_bytes;

            if n_bytes_remaining > 0 {
                file_entry.completed_bytes += n_bytes_remaining;
                file_entry.updater.report_bytes_completed(n_bytes_remaining);
                file_bytes_processed += n_bytes_remaining;
            }
        }

        debug_assert_le!(self.total_upload_bytes_completed + byte_completion_increment, self.total_upload_bytes);
        self.total_upload_bytes_completed += byte_completion_increment;
        group
            .file_data
            .total_transfer_bytes_completed
            .fetch_add(byte_completion_increment, Ordering::Release);

        self.total_bytes_completed += file_bytes_processed;
        debug_assert_le!(self.total_bytes_completed, self.total_bytes);
    }

    fn register_xorb_upload_progress(
        &mut self,
        group: &Arc<UploadGroupProgress>,
        xorb_hash: MerkleHash,
        new_byte_progress: u64,
        check_ordering: bool,
    ) {
        debug_assert!(self.xorbs.contains_key(&xorb_hash));

        let entry = self.xorbs.entry(xorb_hash).or_default();

        if !check_ordering && entry.is_completed {
            return;
        }

        debug_assert!(!entry.is_completed);
        debug_assert_le!(entry.completed_bytes + new_byte_progress, entry.xorb_size);

        entry.completed_bytes += new_byte_progress;

        let new_completion_ratio = (entry.completed_bytes as f64) / (entry.xorb_size as f64);

        let mut file_bytes_processed = 0;

        for &file_id in entry.file_indices.iter() {
            let file_entry = &mut self.files[file_id];

            debug_assert!(file_entry.remaining_xorbs_parts.contains_key(&xorb_hash));

            let incremental_update = 'update: {
                let Some(xorb_part) = file_entry.remaining_xorbs_parts.get_mut(&xorb_hash) else {
                    break 'update 0;
                };
                debug_assert_le!(xorb_part.completed_bytes, xorb_part.n_bytes);

                let new_completion_bytes = ((xorb_part.n_bytes as f64) * new_completion_ratio).floor() as u64;

                debug_assert_ge!(new_completion_bytes, xorb_part.completed_bytes);

                let incremental_update = new_completion_bytes.saturating_sub(xorb_part.completed_bytes);
                xorb_part.completed_bytes += incremental_update;

                debug_assert_le!(xorb_part.completed_bytes, xorb_part.n_bytes);

                incremental_update
            };

            if incremental_update != 0 {
                file_entry.completed_bytes += incremental_update;
                file_entry.updater.report_bytes_completed(incremental_update);
                file_bytes_processed += incremental_update;
            }
        }

        self.total_upload_bytes_completed += new_byte_progress;
        debug_assert_le!(self.total_upload_bytes_completed, self.total_upload_bytes);

        group
            .file_data
            .total_transfer_bytes_completed
            .fetch_add(new_byte_progress, Ordering::Release);

        self.total_bytes_completed += file_bytes_processed;
        debug_assert_le!(self.total_bytes_completed, self.total_bytes);
    }

    fn status(&self) -> (u64, u64) {
        let (mut sum_completed, mut sum_total) = (0, 0);
        for file in &self.files {
            sum_completed += file.completed_bytes;
            sum_total += file.total_bytes;
        }
        (sum_completed, sum_total)
    }

    fn is_complete(&self) -> bool {
        let (done, total) = self.status();

        #[cfg(debug_assertions)]
        {
            if done == total {
                self.assert_complete();
            }
        }

        done == total
    }

    fn validate_complete(&self) -> Result<()> {
        for (idx, file) in self.files.iter().enumerate() {
            if file.completed_bytes != file.total_bytes || !file.remaining_xorbs_parts.is_empty() {
                return Err(DataError::InternalError(format!(
                    "File #{idx} ({}, {}) is not fully completed: {}/{} bytes, {} pending xorb parts",
                    file.name,
                    file.tracking_id,
                    file.completed_bytes,
                    file.total_bytes,
                    file.remaining_xorbs_parts.len()
                )));
            }
        }

        for (hash, xorb_dep) in self.xorbs.iter() {
            if !xorb_dep.is_completed || !xorb_dep.file_indices.is_empty() {
                return Err(DataError::InternalError(format!("Xorb {hash:?} is not fully uploaded")));
            }
        }
        Ok(())
    }

    fn assert_complete(&self) {
        self.validate_complete().unwrap();
    }
}

impl CompletionTracker {
    pub fn new(group: UploadGroupProgress) -> Self {
        Self {
            inner: Mutex::new(CompletionTrackerImpl::default()),
            group_progress: Arc::new(group),
        }
    }

    pub fn register_new_file(
        &self,
        id: UniqueId,
        file_name: impl Into<Arc<str>>,
        file_byte_size: Option<u64>,
    ) -> CompletionTrackerFileId {
        let updater = self.group_progress.new_item(id, file_name);
        let mut update_lock = self.inner.lock().unwrap();
        update_lock.register_new_file(updater, file_byte_size)
    }

    pub fn increment_file_size(&self, file_id: CompletionTrackerFileId, size_increment: u64) {
        let mut update_lock = self.inner.lock().unwrap();
        update_lock.increment_file_size(file_id, size_increment);
    }

    /// Give up on the file registered as `tracking_id`, whose upload will not finish.
    ///
    /// The file counts as complete at the bytes already processed: its total shrinks to that, and any
    /// dependency or size update reported for it afterwards is ignored, so the group can still reach 100%.
    /// A no-op for an unknown id or a file already abandoned.
    pub fn abandon_file(&self, tracking_id: UniqueId) {
        let mut update_lock = self.inner.lock().unwrap();
        update_lock.abandon_file(tracking_id);
    }

    pub fn register_new_xorb(&self, xorb_hash: MerkleHash, xorb_size: u64) -> bool {
        let mut update_lock = self.inner.lock().unwrap();
        update_lock.register_new_xorb(&self.group_progress, xorb_hash, xorb_size)
    }

    pub fn register_dependencies(&self, dependencies: &[FileXorbDependency]) {
        let mut update_lock = self.inner.lock().unwrap();
        update_lock.register_dependencies(dependencies);
    }

    pub fn register_xorb_upload_completion(&self, xorb_hash: MerkleHash) {
        let mut update_lock = self.inner.lock().unwrap();
        update_lock.register_xorb_upload_completion(&self.group_progress, xorb_hash);
    }

    pub fn register_xorb_upload_progress(&self, xorb_hash: MerkleHash, new_byte_progress: u64) {
        self.register_xorb_upload_progress_impl(xorb_hash, new_byte_progress, true);
    }

    pub fn register_xorb_upload_progress_background(self: Arc<Self>, xorb_hash: MerkleHash, new_byte_progress: u64) {
        tokio::spawn(async move {
            self.register_xorb_upload_progress_impl(xorb_hash, new_byte_progress, false);
        });
    }

    fn register_xorb_upload_progress_impl(&self, xorb_hash: MerkleHash, new_byte_progress: u64, check_ordering: bool) {
        let mut update_lock = self.inner.lock().unwrap();
        update_lock.register_xorb_upload_progress(&self.group_progress, xorb_hash, new_byte_progress, check_ordering);
    }

    pub fn status(&self) -> (u64, u64) {
        self.inner.lock().unwrap().status()
    }

    pub fn is_complete(&self) -> bool {
        self.inner.lock().unwrap().is_complete()
    }

    pub fn assert_complete(&self) {
        self.inner.lock().unwrap().assert_complete();
        self.group_progress.assert_complete();
    }

    pub(crate) fn validate_complete(&self) -> Result<()> {
        self.inner.lock().unwrap().validate_complete()
    }

    pub fn register_shard_transfer(&self, shard_size: u64) -> UniqueId {
        self.group_progress.register_shard_transfer(shard_size)
    }

    pub fn increment_shard_transfer_progress(&self, id: UniqueId, nbytes: u64) {
        self.group_progress.increment_shard_transfer_progress(id, nbytes);
    }

    pub fn decrement_shard_transfer_progress(&self, id: UniqueId, nbytes: u64) {
        self.group_progress.decrement_shard_transfer_progress(id, nbytes);
    }

    pub fn register_shard_upload_progress(&self, id: UniqueId, event: &ShardUploadEvent) {
        self.group_progress.register_shard_upload_progress(id, event);
    }

    pub fn progress(&self) -> &Arc<UploadGroupProgress> {
        &self.group_progress
    }
}

#[cfg(test)]
mod tests {
    use xet_core_structures::merklehash::MerkleHash;

    use super::*;

    #[test]
    fn test_status_and_is_complete() {
        let group = UploadGroupProgress::new();
        let tracker = CompletionTracker::new(group);

        let file_a = tracker.register_new_file(UniqueId::new(), "fileA", Some(100));

        let file_b = tracker.register_new_file(UniqueId::new(), "fileB", Some(50));

        let (done, total) = tracker.status();
        assert_eq!(done, 0);
        assert_eq!(total, 150);
        assert!(!tracker.is_complete());

        let x = MerkleHash::random_from_seed(1);
        tracker.register_dependencies(&[FileXorbDependency {
            file_id: file_a,
            xorb_hash: x,
            n_bytes: 100,
            is_external: true,
        }]);

        let (done, total) = tracker.status();
        assert_eq!(done, 100);
        assert_eq!(total, 150);
        assert!(!tracker.is_complete());

        let y = MerkleHash::random_from_seed(2);
        tracker.register_dependencies(&[FileXorbDependency {
            file_id: file_b,
            xorb_hash: y,
            n_bytes: 50,
            is_external: false,
        }]);

        let (done, total) = tracker.status();
        assert_eq!(done, 100);
        assert_eq!(total, 150);

        tracker.register_new_xorb(y, 50);
        tracker.register_xorb_upload_completion(y);

        let (done, total) = tracker.status();
        assert_eq!(done, 150);
        assert_eq!(total, 150);
        assert!(tracker.is_complete());

        tracker.assert_complete();
        tracker.progress().assert_complete();
    }

    #[test]
    fn test_multiple_files_one_shared_xorb() {
        let group = UploadGroupProgress::new();
        let tracker = CompletionTracker::new(group);

        let file_a = tracker.register_new_file(UniqueId::new(), "fileA", Some(200));

        let file_b = tracker.register_new_file(UniqueId::new(), "fileB", Some(300));

        let (done, total) = tracker.status();
        assert_eq!(done, 0);
        assert_eq!(total, 500);

        let xhash = MerkleHash::random_from_seed(1);

        tracker.register_new_xorb(xhash, 1000);

        tracker.register_dependencies(&[
            FileXorbDependency {
                file_id: file_a,
                xorb_hash: xhash,
                n_bytes: 100,
                is_external: false,
            },
            FileXorbDependency {
                file_id: file_b,
                xorb_hash: xhash,
                n_bytes: 200,
                is_external: true,
            },
        ]);

        let (done, total) = tracker.status();
        assert_eq!(done, 200);
        assert_eq!(total, 500);
        assert!(!tracker.is_complete());

        tracker.register_xorb_upload_completion(xhash);

        let (done, total) = tracker.status();
        assert_eq!(done, 300);
        assert_eq!(total, 500);

        let x2 = MerkleHash::random_from_seed(2);

        tracker.register_new_xorb(x2, 1000);

        tracker.register_dependencies(&[FileXorbDependency {
            file_id: file_a,
            xorb_hash: x2,
            n_bytes: 100,
            is_external: true,
        }]);

        let (done, total) = tracker.status();
        assert_eq!(done, 400);
        assert_eq!(total, 500);

        tracker.register_dependencies(&[FileXorbDependency {
            file_id: file_b,
            xorb_hash: x2,
            n_bytes: 100,
            is_external: false,
        }]);

        let (done, total) = tracker.status();
        assert_eq!(done, 400);
        assert_eq!(total, 500);
        assert!(!tracker.is_complete());

        tracker.register_xorb_upload_completion(x2);
        let (done, total) = tracker.status();
        assert_eq!(done, 500);
        assert_eq!(total, 500);
        assert!(tracker.is_complete());

        tracker.assert_complete();
        tracker.progress().assert_complete();
    }

    #[test]
    fn test_single_file_multiple_xorbs() {
        let group = UploadGroupProgress::new();
        let tracker = CompletionTracker::new(group);

        let f = tracker.register_new_file(UniqueId::new(), "bigFile", Some(300));

        let x1 = MerkleHash::random_from_seed(1);
        let x2 = MerkleHash::random_from_seed(2);
        let x3 = MerkleHash::random_from_seed(3);

        tracker.register_new_xorb(x1, 100);
        tracker.register_new_xorb(x3, 100);

        tracker.register_dependencies(&[
            FileXorbDependency {
                file_id: f,
                xorb_hash: x1,
                n_bytes: 100,
                is_external: false,
            },
            FileXorbDependency {
                file_id: f,
                xorb_hash: x2,
                n_bytes: 100,
                is_external: true,
            },
            FileXorbDependency {
                file_id: f,
                xorb_hash: x3,
                n_bytes: 100,
                is_external: false,
            },
        ]);

        let (done, total) = tracker.status();
        assert_eq!(done, 100);
        assert_eq!(total, 300);
        assert!(!tracker.is_complete());

        tracker.register_xorb_upload_completion(x1);
        let (done, total) = tracker.status();
        assert_eq!(done, 200);
        assert_eq!(total, 300);
        assert!(!tracker.is_complete());

        tracker.register_xorb_upload_completion(x3);
        let (done, total) = tracker.status();
        assert_eq!(done, 300);
        assert_eq!(total, 300);
        assert!(tracker.is_complete());

        tracker.assert_complete();
        tracker.progress().assert_complete();
    }

    #[test]
    fn test_xorb_completed_before_dependencies() {
        let group = UploadGroupProgress::new();
        let tracker = CompletionTracker::new(group);

        let file_id = tracker.register_new_file(UniqueId::new(), "lateFile", Some(50));

        let x = MerkleHash::random_from_seed(999);
        tracker.register_new_xorb(x, 1000);

        tracker.register_xorb_upload_completion(x);

        tracker.register_dependencies(&[FileXorbDependency {
            file_id,
            xorb_hash: x,
            n_bytes: 50,
            is_external: false,
        }]);

        let (done, total) = tracker.status();
        assert_eq!(done, 50);
        assert_eq!(total, 50);
        assert!(tracker.is_complete());

        tracker.assert_complete();
        tracker.progress().assert_complete();
    }

    #[test]
    fn test_contradictory_logic_with_completed_xorb() {
        let group = UploadGroupProgress::new();
        let tracker = CompletionTracker::new(group);

        let file_id = tracker.register_new_file(UniqueId::new(), "someFile", Some(100));
        let x = MerkleHash::random_from_seed(123);

        tracker.register_new_xorb(x, 1000);

        tracker.register_xorb_upload_completion(x);

        tracker.register_dependencies(&[FileXorbDependency {
            file_id,
            xorb_hash: x,
            n_bytes: 100,
            is_external: false,
        }]);

        let (done, total) = tracker.status();
        assert_eq!(done, 100);
        assert_eq!(total, 100);
        assert!(tracker.is_complete());

        tracker.assert_complete();
        tracker.progress().assert_complete();
    }

    #[test]
    fn test_increment_file_size_basic() {
        let group = UploadGroupProgress::new();
        let tracker = CompletionTracker::new(group);

        let file_id = tracker.register_new_file(UniqueId::new(), "growingFile", None);

        let (done, total) = tracker.status();
        assert_eq!(done, 0);
        assert_eq!(total, 0);

        tracker.increment_file_size(file_id, 100);
        let (done, total) = tracker.status();
        assert_eq!(done, 0);
        assert_eq!(total, 100);

        tracker.increment_file_size(file_id, 150);
        let (done, total) = tracker.status();
        assert_eq!(done, 0);
        assert_eq!(total, 250);

        tracker.increment_file_size(file_id, 50);
        let (done, total) = tracker.status();
        assert_eq!(done, 0);
        assert_eq!(total, 300);

        let x = MerkleHash::random_from_seed(1);
        tracker.register_dependencies(&[FileXorbDependency {
            file_id,
            xorb_hash: x,
            n_bytes: 300,
            is_external: true,
        }]);

        let (done, total) = tracker.status();
        assert_eq!(done, 300);
        assert_eq!(total, 300);
        assert!(tracker.is_complete());

        tracker.assert_complete();
        tracker.progress().assert_complete();
    }

    #[test]
    fn test_increment_file_size_with_xorb_uploads() {
        let group = UploadGroupProgress::new();
        let tracker = CompletionTracker::new(group);

        let file_id = tracker.register_new_file(UniqueId::new(), "streamFile", None);

        let x1 = MerkleHash::random_from_seed(10);
        let x2 = MerkleHash::random_from_seed(20);

        tracker.register_new_xorb(x1, 500);
        tracker.register_new_xorb(x2, 500);

        tracker.increment_file_size(file_id, 200);
        tracker.register_dependencies(&[FileXorbDependency {
            file_id,
            xorb_hash: x1,
            n_bytes: 200,
            is_external: false,
        }]);

        let (done, total) = tracker.status();
        assert_eq!(done, 0);
        assert_eq!(total, 200);

        tracker.register_xorb_upload_completion(x1);
        let (done, total) = tracker.status();
        assert_eq!(done, 200);
        assert_eq!(total, 200);

        tracker.increment_file_size(file_id, 300);
        tracker.register_dependencies(&[FileXorbDependency {
            file_id,
            xorb_hash: x2,
            n_bytes: 300,
            is_external: false,
        }]);

        let (done, total) = tracker.status();
        assert_eq!(done, 200);
        assert_eq!(total, 500);

        tracker.register_xorb_upload_completion(x2);
        let (done, total) = tracker.status();
        assert_eq!(done, 500);
        assert_eq!(total, 500);
        assert!(tracker.is_complete());

        tracker.assert_complete();
        tracker.progress().assert_complete();
    }

    #[test]
    fn test_increment_file_size_mixed_known_unknown() {
        let group = UploadGroupProgress::new();
        let tracker = CompletionTracker::new(group);

        let file_a = tracker.register_new_file(UniqueId::new(), "fileA", Some(100));

        let file_b = tracker.register_new_file(UniqueId::new(), "fileB", None);

        let (done, total) = tracker.status();
        assert_eq!(done, 0);
        assert_eq!(total, 100);

        let xa = MerkleHash::random_from_seed(1);
        tracker.register_dependencies(&[FileXorbDependency {
            file_id: file_a,
            xorb_hash: xa,
            n_bytes: 100,
            is_external: true,
        }]);

        let (done, total) = tracker.status();
        assert_eq!(done, 100);
        assert_eq!(total, 100);

        tracker.increment_file_size(file_b, 200);
        let (done, total) = tracker.status();
        assert_eq!(done, 100);
        assert_eq!(total, 300);

        let xb = MerkleHash::random_from_seed(2);
        tracker.register_new_xorb(xb, 200);
        tracker.register_dependencies(&[FileXorbDependency {
            file_id: file_b,
            xorb_hash: xb,
            n_bytes: 200,
            is_external: false,
        }]);

        tracker.register_xorb_upload_completion(xb);

        let (done, total) = tracker.status();
        assert_eq!(done, 300);
        assert_eq!(total, 300);
        assert!(tracker.is_complete());

        tracker.assert_complete();
        tracker.progress().assert_complete();
    }

    #[test]
    fn test_increment_file_size_ignored_when_already_final() {
        let group = UploadGroupProgress::new();
        let tracker = CompletionTracker::new(group);

        let file_id = tracker.register_new_file(UniqueId::new(), "fixedFile", Some(100));

        tracker.increment_file_size(file_id, 999);
        let (_, total) = tracker.status();
        assert_eq!(total, 100);

        let x = MerkleHash::random_from_seed(1);
        tracker.register_dependencies(&[FileXorbDependency {
            file_id,
            xorb_hash: x,
            n_bytes: 100,
            is_external: true,
        }]);

        assert!(tracker.is_complete());
        tracker.assert_complete();
        tracker.progress().assert_complete();
    }

    #[test]
    fn test_increment_file_size_with_partial_xorb_progress() {
        let group = UploadGroupProgress::new();
        let tracker = CompletionTracker::new(group);

        let file_id = tracker.register_new_file(UniqueId::new(), "partialFile", None);

        let x = MerkleHash::random_from_seed(42);
        tracker.register_new_xorb(x, 1000);

        tracker.increment_file_size(file_id, 400);
        tracker.register_dependencies(&[FileXorbDependency {
            file_id,
            xorb_hash: x,
            n_bytes: 400,
            is_external: false,
        }]);

        tracker.register_xorb_upload_progress(x, 500);
        let (done, total) = tracker.status();
        assert_eq!(total, 400);
        assert!(done > 0);
        assert!(done < 400);

        tracker.increment_file_size(file_id, 200);
        let (_, total) = tracker.status();
        assert_eq!(total, 600);

        tracker.register_dependencies(&[FileXorbDependency {
            file_id,
            xorb_hash: MerkleHash::random_from_seed(99),
            n_bytes: 200,
            is_external: true,
        }]);

        tracker.register_xorb_upload_completion(x);

        let (done, total) = tracker.status();
        assert_eq!(done, 600);
        assert_eq!(total, 600);
        assert!(tracker.is_complete());

        tracker.assert_complete();
        tracker.progress().assert_complete();
    }

    #[test]
    fn test_abandon_file_with_pending_xorb() {
        let group = UploadGroupProgress::new();
        let tracker = CompletionTracker::new(group);

        // File A streams 100 bytes of unknown final size; 40 of them wait on xorb `x`.
        let id_a = UniqueId::new();
        let file_a = tracker.register_new_file(id_a, "fileA", None);
        tracker.increment_file_size(file_a, 100);
        let file_b = tracker.register_new_file(UniqueId::new(), "fileB", Some(50));

        let x = MerkleHash::random_from_seed(1);
        tracker.register_new_xorb(x, 40);
        tracker.register_dependencies(&[FileXorbDependency {
            file_id: file_a,
            xorb_hash: x,
            n_bytes: 40,
            is_external: false,
        }]);
        tracker.register_xorb_upload_progress(x, 20);
        assert_eq!(tracker.status(), (20, 150));

        // Abandoned, file A counts as complete at the 20 bytes already done. A second call is a no-op.
        tracker.abandon_file(id_a);
        tracker.abandon_file(id_a);
        assert_eq!(tracker.status(), (20, 70));

        // Leftover background work for file A no longer counts: neither size growth nor new dependencies.
        let y = MerkleHash::random_from_seed(2);
        tracker.register_new_xorb(y, 30);
        tracker.increment_file_size(file_a, 10);
        tracker.register_dependencies(&[
            FileXorbDependency {
                file_id: file_a,
                xorb_hash: MerkleHash::random_from_seed(3),
                n_bytes: 30,
                is_external: true,
            },
            FileXorbDependency {
                file_id: file_a,
                xorb_hash: y,
                n_bytes: 30,
                is_external: false,
            },
        ]);
        assert_eq!(tracker.status(), (20, 70));

        // The xorbs still upload, so the transfer totals are unchanged.
        tracker.register_xorb_upload_completion(x);
        tracker.register_xorb_upload_completion(y);
        assert_eq!(tracker.status(), (20, 70));

        tracker.register_dependencies(&[FileXorbDependency {
            file_id: file_b,
            xorb_hash: MerkleHash::random_from_seed(4),
            n_bytes: 50,
            is_external: true,
        }]);
        assert_eq!(tracker.status(), (70, 70));
        assert!(tracker.is_complete());

        let report = tracker.progress().file_data.report();
        assert_eq!((report.total_bytes_completed, report.total_bytes), (70, 70));
        assert_eq!((report.total_transfer_bytes_completed, report.total_transfer_bytes), (70, 70));
        let item_a = tracker.progress().item_report(id_a).unwrap();
        assert_eq!((item_a.bytes_completed, item_a.total_bytes), (20, 20));

        tracker.assert_complete();
        tracker.progress().assert_complete();
    }

    #[test]
    fn test_abandon_finished_file_changes_nothing() {
        let group = UploadGroupProgress::new();
        let tracker = CompletionTracker::new(group);

        let id = UniqueId::new();
        let file_id = tracker.register_new_file(id, "fileA", Some(100));
        tracker.register_dependencies(&[FileXorbDependency {
            file_id,
            xorb_hash: MerkleHash::random_from_seed(1),
            n_bytes: 100,
            is_external: true,
        }]);

        tracker.abandon_file(id);

        assert_eq!(tracker.status(), (100, 100));
        tracker.assert_complete();
        tracker.progress().assert_complete();
    }

    /// Drives the tracker through seeded random interleavings of every operation, abandoning files at
    /// arbitrary points. After each step the tracker's totals, the group counters, and the per-item reports
    /// must agree, active files must keep their size, and abandoned files must stay frozen at the bytes
    /// completed when they were abandoned. At the end, finishing every remaining file must complete everything.
    #[test]
    fn test_random_interleavings_with_abandon_keep_totals_consistent() {
        use rand::rngs::StdRng;
        use rand::{RngExt, SeedableRng};

        struct ModelFile {
            id: UniqueId,
            file_id: CompletionTrackerFileId,
            total: u64,
            /// Bytes covered by a dependency so far; never more than `total` while active.
            assigned: u64,
            size_known: bool,
            /// `Some(bytes completed at abandonment)` once abandoned.
            abandoned_at: Option<u64>,
        }
        struct ModelXorb {
            hash: MerkleHash,
            size: u64,
            progressed: u64,
            completed: bool,
        }

        fn check(tracker: &CompletionTracker, files: &[ModelFile]) {
            let (done, total) = tracker.status();
            let group = tracker.progress().file_data.report();
            assert_eq!((group.total_bytes_completed, group.total_bytes), (done, total));
            assert!(group.total_transfer_bytes_completed <= group.total_transfer_bytes);

            let items = tracker.progress().item_reports();
            assert_eq!(items.values().map(|r| r.bytes_completed).sum::<u64>(), done);
            assert_eq!(items.values().map(|r| r.total_bytes).sum::<u64>(), total);
            for f in files {
                let item = &items[&f.id];
                assert!(item.bytes_completed <= item.total_bytes);
                match f.abandoned_at {
                    Some(at) => assert_eq!((item.bytes_completed, item.total_bytes), (at, at)),
                    None => assert_eq!(item.total_bytes, f.total),
                }
            }
        }

        for seed in 0..200 {
            let mut rng = StdRng::seed_from_u64(seed);
            let tracker = CompletionTracker::new(UploadGroupProgress::new());
            let mut files: Vec<ModelFile> = Vec::new();
            let mut xorbs: Vec<ModelXorb> = Vec::new();

            for _ in 0..120 {
                match rng.random_range(0..7) {
                    0 => {
                        let id = UniqueId::new();
                        let size_known = rng.random_bool(0.5);
                        let total = if size_known { rng.random_range(0..1000) } else { 0 };
                        let file_id = tracker.register_new_file(id, "file", size_known.then_some(total));
                        files.push(ModelFile {
                            id,
                            file_id,
                            total,
                            assigned: 0,
                            size_known,
                            abandoned_at: None,
                        });
                    },
                    1 if !files.is_empty() => {
                        let i = rng.random_range(0..files.len());
                        let f = &mut files[i];
                        let increment = rng.random_range(1..200);
                        tracker.increment_file_size(f.file_id, increment);
                        if !f.size_known && f.abandoned_at.is_none() {
                            f.total += increment;
                        }
                    },
                    2 => {
                        let hash = MerkleHash::random_from_seed(rng.random());
                        let size = rng.random_range(1..500);
                        assert!(tracker.register_new_xorb(hash, size));
                        xorbs.push(ModelXorb {
                            hash,
                            size,
                            progressed: 0,
                            completed: false,
                        });
                    },
                    3 if !files.is_empty() => {
                        let i = rng.random_range(0..files.len());
                        let f = &mut files[i];
                        // An abandoned file may still receive any dependency; it must be ignored.
                        let n_bytes = match f.abandoned_at {
                            Some(_) => rng.random_range(1..1000),
                            None if f.assigned < f.total => rng.random_range(1..=f.total - f.assigned),
                            None => continue,
                        };
                        let internal = (!xorbs.is_empty() && rng.random_bool(0.7))
                            .then(|| xorbs[rng.random_range(0..xorbs.len())].hash);
                        tracker.register_dependencies(&[FileXorbDependency {
                            file_id: f.file_id,
                            xorb_hash: internal.unwrap_or_else(|| MerkleHash::random_from_seed(rng.random())),
                            n_bytes,
                            is_external: internal.is_none(),
                        }]);
                        if f.abandoned_at.is_none() {
                            f.assigned += n_bytes;
                        }
                    },
                    4 if xorbs.iter().any(|x| !x.completed && x.progressed < x.size) => {
                        let pending: Vec<_> =
                            xorbs.iter_mut().filter(|x| !x.completed && x.progressed < x.size).collect();
                        let n = pending.len();
                        let x = pending.into_iter().nth(rng.random_range(0..n)).unwrap();
                        let delta = rng.random_range(1..=x.size - x.progressed);
                        tracker.register_xorb_upload_progress(x.hash, delta);
                        x.progressed += delta;
                    },
                    5 if !xorbs.is_empty() => {
                        let i = rng.random_range(0..xorbs.len());
                        let x = &mut xorbs[i];
                        tracker.register_xorb_upload_completion(x.hash);
                        x.completed = true;
                    },
                    6 if !files.is_empty() => {
                        let i = rng.random_range(0..files.len());
                        let f = &mut files[i];
                        let before = tracker.progress().item_report(f.id).unwrap().bytes_completed;
                        tracker.abandon_file(f.id);
                        f.abandoned_at.get_or_insert(before);
                    },
                    _ => continue,
                }
                check(&tracker, &files);
            }

            // Finish: every xorb uploads, and every active file gets the rest of its bytes from dedup.
            for x in &xorbs {
                tracker.register_xorb_upload_completion(x.hash);
            }
            for f in files.iter().filter(|f| f.abandoned_at.is_none() && f.assigned < f.total) {
                tracker.register_dependencies(&[FileXorbDependency {
                    file_id: f.file_id,
                    xorb_hash: MerkleHash::random_from_seed(rng.random()),
                    n_bytes: f.total - f.assigned,
                    is_external: true,
                }]);
            }
            check(&tracker, &files);
            assert!(tracker.is_complete(), "seed {seed}: tracker not complete");
            tracker.assert_complete();
            tracker.progress().assert_complete();
        }
    }
}
