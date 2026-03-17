use std::collections::BTreeSet;
use std::collections::hash_map::Entry as HashMapEntry;
use std::mem::take;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use more_asserts::{debug_assert_ge, debug_assert_le};
use xet_core_structures::MerkleHashMap;
use xet_core_structures::merklehash::MerkleHash;

use super::progress_types::{GroupProgress, ItemProgressUpdater};
use super::unique_id::UniqueID;

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
    tracking_id: UniqueID,
    updater: Arc<ItemProgressUpdater>,
    name: Arc<str>,
    total_bytes: u64,
    is_final_size_known: bool,
    completed_bytes: u64,
    remaining_xorbs_parts: MerkleHashMap<XorbPartCompletionStats>,
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
    group: Arc<GroupProgress>,
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

    fn register_new_xorb(&mut self, group: &Arc<GroupProgress>, xorb_hash: MerkleHash, xorb_size: u64) -> bool {
        match self.xorbs.entry(xorb_hash) {
            HashMapEntry::Occupied(mut occupied_entry) => {
                let entry = occupied_entry.get_mut();
                if entry.xorb_size == 0 {
                    entry.xorb_size = xorb_size;
                    self.total_upload_bytes += xorb_size;
                    group.total_transfer_bytes.fetch_add(xorb_size, Ordering::Release);
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
                group.total_transfer_bytes.fetch_add(xorb_size, Ordering::Release);
                true
            },
        }
    }

    fn register_xorb_upload_completion(&mut self, group: &Arc<GroupProgress>, xorb_hash: MerkleHash) {
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
            .total_transfer_bytes_completed
            .fetch_add(byte_completion_increment, Ordering::Release);

        self.total_bytes_completed += file_bytes_processed;
        debug_assert_le!(self.total_bytes_completed, self.total_bytes);
    }

    fn register_xorb_upload_progress(
        &mut self,
        group: &Arc<GroupProgress>,
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

    fn assert_complete(&self) {
        for (idx, file) in self.files.iter().enumerate() {
            assert_eq!(
                file.completed_bytes, file.total_bytes,
                "File #{} ({}, {}) is not fully completed: {}/{} bytes",
                idx, file.name, file.tracking_id, file.completed_bytes, file.total_bytes
            );
            assert!(
                file.remaining_xorbs_parts.is_empty(),
                "File #{} ({}) still has uncompleted xorb parts: {:?}",
                idx,
                file.name,
                file.remaining_xorbs_parts
            );
        }

        for (hash, xorb_dep) in self.xorbs.iter() {
            assert!(xorb_dep.is_completed, "Xorb {hash:?} is not marked completed.");
            assert!(
                xorb_dep.file_indices.is_empty(),
                "Xorb {:?} still has file references: {:?}",
                hash,
                xorb_dep.file_indices
            );
        }
    }
}

impl CompletionTracker {
    pub fn new(group: Arc<GroupProgress>) -> Self {
        Self {
            inner: Mutex::new(CompletionTrackerImpl::default()),
            group,
        }
    }

    pub fn register_new_file(
        &self,
        updater: Arc<ItemProgressUpdater>,
        n_bytes: Option<u64>,
    ) -> CompletionTrackerFileId {
        let mut update_lock = self.inner.lock().unwrap();
        update_lock.register_new_file(updater, n_bytes)
    }

    pub fn increment_file_size(&self, file_id: CompletionTrackerFileId, size_increment: u64) {
        let mut update_lock = self.inner.lock().unwrap();
        update_lock.increment_file_size(file_id, size_increment);
    }

    pub fn register_new_xorb(&self, xorb_hash: MerkleHash, xorb_size: u64) -> bool {
        let mut update_lock = self.inner.lock().unwrap();
        update_lock.register_new_xorb(&self.group, xorb_hash, xorb_size)
    }

    pub fn register_dependencies(&self, dependencies: &[FileXorbDependency]) {
        let mut update_lock = self.inner.lock().unwrap();
        update_lock.register_dependencies(dependencies);
    }

    pub fn register_xorb_upload_completion(&self, xorb_hash: MerkleHash) {
        let mut update_lock = self.inner.lock().unwrap();
        update_lock.register_xorb_upload_completion(&self.group, xorb_hash);
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
        update_lock.register_xorb_upload_progress(&self.group, xorb_hash, new_byte_progress, check_ordering);
    }

    pub fn status(&self) -> (u64, u64) {
        self.inner.lock().unwrap().status()
    }

    pub fn is_complete(&self) -> bool {
        self.inner.lock().unwrap().is_complete()
    }

    pub fn assert_complete(&self) {
        self.inner.lock().unwrap().assert_complete();
    }
}

#[cfg(test)]
mod tests {
    use xet_core_structures::merklehash::MerkleHash;

    use super::*;

    #[test]
    fn test_status_and_is_complete() {
        let group = GroupProgress::new();
        let tracker = CompletionTracker::new(group.clone());

        let updater_a = group.new_item(UniqueID::new(), "fileA");
        let file_a = tracker.register_new_file(updater_a, Some(100));

        let updater_b = group.new_item(UniqueID::new(), "fileB");
        let file_b = tracker.register_new_file(updater_b, Some(50));

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
        group.assert_complete();
    }

    #[test]
    fn test_multiple_files_one_shared_xorb() {
        let group = GroupProgress::new();
        let tracker = CompletionTracker::new(group.clone());

        let updater_a = group.new_item(UniqueID::new(), "fileA");
        let file_a = tracker.register_new_file(updater_a, Some(200));

        let updater_b = group.new_item(UniqueID::new(), "fileB");
        let file_b = tracker.register_new_file(updater_b, Some(300));

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
        group.assert_complete();
    }

    #[test]
    fn test_single_file_multiple_xorbs() {
        let group = GroupProgress::new();
        let tracker = CompletionTracker::new(group.clone());

        let updater = group.new_item(UniqueID::new(), "bigFile");
        let f = tracker.register_new_file(updater, Some(300));

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
        group.assert_complete();
    }

    #[test]
    fn test_xorb_completed_before_dependencies() {
        let group = GroupProgress::new();
        let tracker = CompletionTracker::new(group.clone());

        let updater = group.new_item(UniqueID::new(), "lateFile");
        let file_id = tracker.register_new_file(updater, Some(50));

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
        group.assert_complete();
    }

    #[test]
    fn test_contradictory_logic_with_completed_xorb() {
        let group = GroupProgress::new();
        let tracker = CompletionTracker::new(group.clone());

        let updater = group.new_item(UniqueID::new(), "someFile");
        let file_id = tracker.register_new_file(updater, Some(100));
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
        group.assert_complete();
    }

    #[test]
    fn test_increment_file_size_basic() {
        let group = GroupProgress::new();
        let tracker = CompletionTracker::new(group.clone());

        let updater = group.new_item(UniqueID::new(), "growingFile");
        let file_id = tracker.register_new_file(updater, None);

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
        group.assert_complete();
    }

    #[test]
    fn test_increment_file_size_with_xorb_uploads() {
        let group = GroupProgress::new();
        let tracker = CompletionTracker::new(group.clone());

        let updater = group.new_item(UniqueID::new(), "streamFile");
        let file_id = tracker.register_new_file(updater, None);

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
        group.assert_complete();
    }

    #[test]
    fn test_increment_file_size_mixed_known_unknown() {
        let group = GroupProgress::new();
        let tracker = CompletionTracker::new(group.clone());

        let updater_a = group.new_item(UniqueID::new(), "fileA");
        let file_a = tracker.register_new_file(updater_a, Some(100));

        let updater_b = group.new_item(UniqueID::new(), "fileB");
        let file_b = tracker.register_new_file(updater_b, None);

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
        group.assert_complete();
    }

    #[test]
    fn test_increment_file_size_ignored_when_already_final() {
        let group = GroupProgress::new();
        let tracker = CompletionTracker::new(group.clone());

        let updater = group.new_item(UniqueID::new(), "fixedFile");
        let file_id = tracker.register_new_file(updater, Some(100));

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
        group.assert_complete();
    }

    #[test]
    fn test_increment_file_size_with_partial_xorb_progress() {
        let group = GroupProgress::new();
        let tracker = CompletionTracker::new(group.clone());

        let updater = group.new_item(UniqueID::new(), "partialFile");
        let file_id = tracker.register_new_file(updater, None);

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
        group.assert_complete();
    }
}
