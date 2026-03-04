use std::collections::BTreeSet;
use std::collections::hash_map::Entry as HashMapEntry;
use std::mem::take;
use std::sync::Arc;

use merklehash::MerkleHash;
use more_asserts::{debug_assert_ge, debug_assert_le};
use tokio::sync::Mutex;
use ulid::Ulid;
use utils::MerkleHashMap;

use crate::{ItemProgressUpdate, ProgressUpdate, TrackingProgressUpdater};

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
    /// A unique id when the below name is not enough to identify a single file.
    tracking_id: Ulid,

    /// Human-readable name of the file.
    name: Arc<str>,

    /// Total size of this file in bytes.
    total_bytes: u64,

    /// Whether the total size is known to be final. When false, the size can be
    /// updated via `update_file_size`.
    is_final_size_known: bool,

    /// Total bytes already uploaded for this file (across its xorbs).
    completed_bytes: u64,

    /// Mapping of xorb_hash -> (number of completed bytes / number of bytes of the file contained in that xorb).  Only
    /// xorbs that are not uploaded yet are tracked here.
    /// Once an xorb is uploaded, we remove it from here (and add to `completed_bytes`).
    remaining_xorbs_parts: MerkleHashMap<XorbPartCompletionStats>,
}

/// Tracks all files and all xorbs, allowing you to register file
/// dependencies on xorbs and then mark xorbs as completed when they
/// are fully uploaded.
#[derive(Default)]
struct CompletionTrackerImpl {
    /// List of all files being tracked.
    files: Vec<FileDependency>,
    /// Map of xorb hash -> its dependency info (which files rely on it).
    xorbs: MerkleHashMap<XorbDependency>,

    /// Keep track of the totals across all xorbs.
    total_upload_bytes: u64,
    total_upload_bytes_completed: u64,

    total_bytes: u64,
    total_bytes_completed: u64,
}

pub struct CompletionTracker {
    inner: Mutex<CompletionTrackerImpl>,
    progress_reporter: Arc<dyn TrackingProgressUpdater>,
}

impl CompletionTrackerImpl {
    /// Registers a new file for tracking and returns an ID (its index in `files`).
    ///
    /// If `n_bytes` is `Some(size)`, the file size is treated as final and cannot be
    /// updated later.  If `n_bytes` is `None`, the file is registered with a size of
    /// zero and `is_final_size_known` is set to `false`; callers should subsequently
    /// call `update_file_size` to provide the actual size.
    fn register_new_file(
        &mut self,
        tracking_id: Ulid,
        name: impl Into<Arc<str>>,
        n_bytes: Option<u64>,
    ) -> (ProgressUpdate, CompletionTrackerFileId) {
        let (total_bytes, is_final_size_known) = match n_bytes {
            Some(size) => (size, true),
            None => (0, false),
        };

        // The file's ID is simply its index in the internal `files` vector.
        let file_id = self.files.len() as CompletionTrackerFileId;

        // Create a new FileDependency record.
        let file_dependency = FileDependency {
            tracking_id,
            name: name.into(),
            total_bytes,
            is_final_size_known,
            completed_bytes: 0,
            remaining_xorbs_parts: MerkleHashMap::new(),
        };

        // Insert it into our files vector.
        self.files.push(file_dependency);

        // We have more to process now.
        self.total_bytes += total_bytes;

        // Register that the total bytes known has changed, and return the file ID so the caller can register
        // dependencies on this file.
        (
            ProgressUpdate {
                item_updates: vec![],
                total_bytes: self.total_bytes,
                total_bytes_increment: total_bytes,
                total_bytes_completed: self.total_bytes_completed,
                total_bytes_completion_increment: 0,
                total_transfer_bytes: self.total_upload_bytes,
                total_transfer_bytes_increment: 0,
                total_transfer_bytes_completed: self.total_upload_bytes_completed,
                total_transfer_bytes_completion_increment: 0,
                ..Default::default()
            },
            file_id,
        )
    }

    /// Increments the total size of a previously registered file by `size_increment` bytes.
    ///
    /// Returns `Some(ProgressUpdate)` if the size was updated, or `None` if the file's size
    /// has already been finalized (via `register_new_file` with `Some(size)`).  When `None`
    /// is returned, no internal state is modified.
    fn increment_file_size(&mut self, file_id: CompletionTrackerFileId, size_increment: u64) -> Option<ProgressUpdate> {
        let file_entry = &mut self.files[file_id as usize];

        // If already finalized, nothing to do.
        if file_entry.is_final_size_known {
            return None;
        }

        file_entry.total_bytes += size_increment;
        self.total_bytes += size_increment;

        debug_assert_ge!(file_entry.total_bytes, file_entry.completed_bytes);
        debug_assert_ge!(self.total_bytes, self.total_bytes_completed);

        // Emit an item update so progress reporters see the new total for this file.
        let item_update = ItemProgressUpdate {
            tracking_id: file_entry.tracking_id,
            item_name: file_entry.name.clone(),
            total_bytes: file_entry.total_bytes,
            bytes_completed: file_entry.completed_bytes,
            bytes_completion_increment: 0,
        };

        Some(ProgressUpdate {
            item_updates: vec![item_update],
            total_bytes: self.total_bytes,
            total_bytes_increment: size_increment,
            total_bytes_completed: self.total_bytes_completed,
            total_bytes_completion_increment: 0,
            total_transfer_bytes: self.total_upload_bytes,
            total_transfer_bytes_increment: 0,
            total_transfer_bytes_completed: self.total_upload_bytes_completed,
            total_transfer_bytes_completion_increment: 0,
            ..Default::default()
        })
    }

    /// Registers that all or part of a given file (by `file_id`) depends on one or more
    /// xorbs; Given a list of (xorb_hash, n_bytes, already_uploaded), registers the progress.
    fn register_dependencies(&mut self, dependencies: &[FileXorbDependency]) -> ProgressUpdate {
        let mut item_updates = Vec::new();

        let mut file_bytes_processed = 0;

        for dep in dependencies {
            let file_entry = &mut self.files[dep.file_id as usize];

            if dep.is_external {
                // This is the freebie case, where we can just increment the progress.
                file_entry.completed_bytes += dep.n_bytes;
                debug_assert_le!(file_entry.completed_bytes, file_entry.total_bytes);

                let progress_update = ItemProgressUpdate {
                    tracking_id: file_entry.tracking_id,
                    item_name: file_entry.name.clone(),
                    total_bytes: file_entry.total_bytes,
                    bytes_completed: file_entry.completed_bytes,
                    bytes_completion_increment: dep.n_bytes,
                };

                file_bytes_processed += dep.n_bytes;

                item_updates.push(progress_update);
            } else {
                // Make sure we aren't putting in an unfinished xorb, which
                // tracks with MerkleHash::marker().
                debug_assert_ne!(dep.xorb_hash, MerkleHash::marker());

                let entry = self.xorbs.entry(dep.xorb_hash).or_default();

                // If the entry has already been completed, then just mark this as completed.
                if entry.is_completed {
                    file_entry.completed_bytes += dep.n_bytes;
                    debug_assert_le!(file_entry.completed_bytes, file_entry.total_bytes);

                    let progress_update = ItemProgressUpdate {
                        tracking_id: file_entry.tracking_id,
                        item_name: file_entry.name.clone(),
                        total_bytes: file_entry.total_bytes,
                        bytes_completed: file_entry.completed_bytes,
                        bytes_completion_increment: dep.n_bytes,
                    };
                    item_updates.push(progress_update);
                    file_bytes_processed += dep.n_bytes;
                } else {
                    // Set the reference here to this file
                    entry.file_indices.insert(dep.file_id as usize);

                    // Set the reference here to the xorb
                    file_entry.remaining_xorbs_parts.entry(dep.xorb_hash).or_default().n_bytes += dep.n_bytes;
                }
            }
        }

        // Register that this much has been completed already
        self.total_bytes_completed += file_bytes_processed;

        debug_assert_le!(self.total_bytes_completed, self.total_bytes);

        // There may be a lot of per-file updates, but these don't actually count against the new byte total;
        // this is counted only using xorbs.
        ProgressUpdate {
            item_updates,
            total_bytes: self.total_bytes,
            total_bytes_increment: 0,
            total_bytes_completed: self.total_bytes_completed,
            total_bytes_completion_increment: file_bytes_processed,
            total_transfer_bytes: self.total_upload_bytes,
            total_transfer_bytes_increment: 0,
            total_transfer_bytes_completed: self.total_upload_bytes_completed,
            total_transfer_bytes_completion_increment: 0,
            ..Default::default()
        }
    }

    /// Register a new xorb.  Returns true if the xorb is new and now registered for upload, and false
    /// if it's already been uploaded and registered.
    fn register_new_xorb(&mut self, xorb_hash: MerkleHash, xorb_size: u64) -> (ProgressUpdate, bool) {
        match self.xorbs.entry(xorb_hash) {
            HashMapEntry::Occupied(occupied_entry) => {
                debug_assert_eq!(occupied_entry.get().xorb_size, xorb_size);
                (ProgressUpdate::default(), false)
            },
            HashMapEntry::Vacant(vacant_entry) => {
                vacant_entry.insert(XorbDependency {
                    file_indices: Default::default(),
                    xorb_size,
                    completed_bytes: 0,
                    is_completed: false,
                });

                self.total_upload_bytes += xorb_size;

                (
                    ProgressUpdate {
                        item_updates: vec![],
                        total_bytes: self.total_bytes,
                        total_bytes_increment: 0,
                        total_bytes_completed: self.total_bytes_completed,
                        total_bytes_completion_increment: 0,
                        total_transfer_bytes: self.total_upload_bytes,
                        total_transfer_bytes_increment: xorb_size,
                        total_transfer_bytes_completed: self.total_upload_bytes_completed,
                        total_transfer_bytes_completion_increment: 0,
                        ..Default::default()
                    },
                    true,
                )
            },
        }
    }

    /// Called when a xorb is finished uploading.  We look up which files depend on that
    /// xorb and update their `completed_bytes`, removing the xorb from their
    /// `remaining_xorbs_parts`.
    fn register_xorb_upload_completion(&mut self, xorb_hash: MerkleHash) -> ProgressUpdate {
        let (file_indices, byte_completion_increment) = {
            // Should have been registered above with register_xorb
            debug_assert!(self.xorbs.contains_key(&xorb_hash));

            // Mark as completed, return the list of files to mark as completed.
            let entry = self.xorbs.entry(xorb_hash).or_default();

            // How many new bytes uploaded do we have to write out to the total_completed_bytes?
            let new_byte_increment = entry.xorb_size - entry.completed_bytes;

            // This should be present but not completed.
            debug_assert!(!entry.is_completed);

            entry.is_completed = true;

            (take(&mut entry.file_indices), new_byte_increment)
        };

        // Mark all the relevant files as completed
        let mut item_updates = Vec::with_capacity(file_indices.len());

        let mut file_bytes_processed = 0;

        // For each file that depends on this xorb, remove the relevant
        // part from `remaining_xorbs_parts` and add to `completed_bytes`.
        for file_id in file_indices {
            let file_entry = &mut self.files[file_id];

            debug_assert!(file_entry.remaining_xorbs_parts.contains_key(&xorb_hash));

            // This xorb is completed, so remove the number of bytes in that file needed by that xorb.
            let xorb_part = file_entry.remaining_xorbs_parts.remove(&xorb_hash).unwrap_or_default();
            debug_assert_le!(xorb_part.completed_bytes, xorb_part.n_bytes);

            let n_bytes_remaining = xorb_part.n_bytes - xorb_part.completed_bytes;

            if n_bytes_remaining > 0 {
                file_entry.completed_bytes += n_bytes_remaining;

                let progress_update = ItemProgressUpdate {
                    tracking_id: file_entry.tracking_id,
                    item_name: file_entry.name.clone(),
                    total_bytes: file_entry.total_bytes,
                    bytes_completed: file_entry.completed_bytes,
                    bytes_completion_increment: n_bytes_remaining,
                };

                file_bytes_processed += n_bytes_remaining;

                item_updates.push(progress_update);
            }
        }

        debug_assert_le!(self.total_upload_bytes_completed + byte_completion_increment, self.total_upload_bytes);
        self.total_upload_bytes_completed += byte_completion_increment;

        self.total_bytes_completed += file_bytes_processed;
        debug_assert_le!(self.total_bytes_completed, self.total_bytes);

        ProgressUpdate {
            item_updates,
            total_bytes: self.total_bytes,
            total_bytes_increment: 0,
            total_bytes_completed: self.total_bytes_completed,
            total_bytes_completion_increment: file_bytes_processed,
            total_transfer_bytes: self.total_upload_bytes,
            total_transfer_bytes_completed: self.total_upload_bytes_completed,
            total_transfer_bytes_completion_increment: byte_completion_increment,
            total_transfer_bytes_increment: 0,
            ..Default::default()
        }
    }

    /// Register partial upload progress of a xorb; new_byte_progress is the number of new bytes uploaded.
    ///
    /// If force_proper_ordering is true, then all the updates should arrive before register_xorb_upload_completion;
    /// if debug_assertions are on, then all the details will be checked.  If this is false, then the updates can
    /// arrive out of order and will simply be ignored if the register_xorb_upload_completion has been called.
    fn register_xorb_upload_progress(
        &mut self,
        xorb_hash: MerkleHash,
        new_byte_progress: u64,
        check_ordering: bool,
    ) -> ProgressUpdate {
        // Should have already been registered.
        debug_assert!(self.xorbs.contains_key(&xorb_hash));

        // Mark as completed, return the list of files to mark as completed.
        let entry = self.xorbs.entry(xorb_hash).or_default();

        // If this update could arrive out of order, check to see if it's needed and ignore if not.
        if !check_ordering && entry.is_completed {
            // Return an empty update
            return ProgressUpdate {
                item_updates: vec![],
                total_bytes: self.total_bytes,
                total_bytes_increment: 0,
                total_bytes_completed: self.total_bytes_completed,
                total_bytes_completion_increment: 0,
                total_transfer_bytes: self.total_upload_bytes,
                total_transfer_bytes_increment: 0,
                total_transfer_bytes_completed: self.total_upload_bytes_completed,
                total_transfer_bytes_completion_increment: 0,
                ..Default::default()
            };
        }

        // Should not be completed when this is called.
        debug_assert!(!entry.is_completed);

        // Is the update reasonable?
        debug_assert_le!(entry.completed_bytes + new_byte_progress, entry.xorb_size);

        entry.completed_bytes += new_byte_progress;

        let new_completion_ratio = (entry.completed_bytes as f64) / (entry.xorb_size as f64);

        // Mark all the relevant files as completed
        let mut item_updates = Vec::with_capacity(entry.file_indices.len());

        let mut file_bytes_processed = 0;

        // For each file that depends on this xorb, update a proportion of that remove the relevant
        // part from `remaining_xorbs_parts` and add to `completed_bytes`.
        for &file_id in entry.file_indices.iter() {
            let file_entry = &mut self.files[file_id];

            // Should be registered there.
            debug_assert!(file_entry.remaining_xorbs_parts.contains_key(&xorb_hash));

            // Update
            let incremental_update = 'update: {
                let Some(xorb_part) = file_entry.remaining_xorbs_parts.get_mut(&xorb_hash) else {
                    break 'update 0;
                };
                debug_assert_le!(xorb_part.completed_bytes, xorb_part.n_bytes);

                // Use floor so as to not inproperly report completion when there is still some to go.
                let new_completion_bytes = ((xorb_part.n_bytes as f64) * new_completion_ratio).floor() as u64;

                // Make sure this is an update
                debug_assert_ge!(new_completion_bytes, xorb_part.completed_bytes);

                let incremental_update = new_completion_bytes.saturating_sub(xorb_part.completed_bytes);
                xorb_part.completed_bytes += incremental_update;

                debug_assert_le!(xorb_part.completed_bytes, xorb_part.n_bytes);

                incremental_update
            };

            if incremental_update != 0 {
                file_entry.completed_bytes += incremental_update;

                let progress_update = ItemProgressUpdate {
                    tracking_id: file_entry.tracking_id,
                    item_name: file_entry.name.clone(),
                    total_bytes: file_entry.total_bytes,
                    bytes_completed: file_entry.completed_bytes,
                    bytes_completion_increment: incremental_update,
                };
                file_bytes_processed += incremental_update;
                item_updates.push(progress_update);
            }
        }

        self.total_upload_bytes_completed += new_byte_progress;
        debug_assert_le!(self.total_upload_bytes_completed, self.total_upload_bytes);

        self.total_bytes_completed += file_bytes_processed;
        debug_assert_le!(self.total_bytes_completed, self.total_bytes);

        ProgressUpdate {
            item_updates,
            total_bytes: self.total_bytes,
            total_bytes_increment: 0,
            total_bytes_completed: self.total_bytes_completed,
            total_bytes_completion_increment: file_bytes_processed,
            total_transfer_bytes: self.total_upload_bytes,
            total_transfer_bytes_increment: 0,
            total_transfer_bytes_completed: self.total_upload_bytes_completed,
            total_transfer_bytes_completion_increment: new_byte_progress,
            ..Default::default()
        }
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

    /// Checks that all files are fully completed (no remaining xorbs or incomplete bytes),
    /// and that all xorbs are marked completed with no lingering file references.
    /// Panics if any incomplete data is found.
    fn assert_complete(&self) {
        // Check each file for completeness
        for (idx, file) in self.files.iter().enumerate() {
            assert_eq!(
                file.completed_bytes, file.total_bytes,
                "File #{} ({}) is not fully completed: {}/{} bytes",
                idx, file.name, file.completed_bytes, file.total_bytes
            );
            assert!(
                file.remaining_xorbs_parts.is_empty(),
                "File #{} ({}) still has uncompleted xorb parts: {:?}",
                idx,
                file.name,
                file.remaining_xorbs_parts
            );
        }

        // Check each xorb to ensure it's marked completed and no file references remain
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

/// A wrapper around the above class to work with the locking and the reporting.
impl CompletionTracker {
    pub fn new(progress_reporter: Arc<dyn TrackingProgressUpdater>) -> Self {
        CompletionTracker {
            inner: Mutex::new(CompletionTrackerImpl::default()),
            progress_reporter,
        }
    }

    pub async fn register_new_file(
        &self,
        tracking_id: Ulid,
        name: impl Into<Arc<str>>,
        n_bytes: Option<u64>,
    ) -> CompletionTrackerFileId {
        let mut update_lock = self.inner.lock().await;

        let (updates, ret) = update_lock.register_new_file(tracking_id, name, n_bytes);

        if !updates.is_empty() {
            self.progress_reporter.register_updates(updates).await;
        }

        ret
    }

    pub async fn increment_file_size(&self, file_id: CompletionTrackerFileId, size_increment: u64) {
        let mut update_lock = self.inner.lock().await;

        if let Some(updates) = update_lock.increment_file_size(file_id, size_increment)
            && !updates.is_empty()
        {
            self.progress_reporter.register_updates(updates).await;
        }
    }

    pub async fn register_new_xorb(&self, xorb_hash: MerkleHash, xorb_size: u64) -> bool {
        let mut update_lock = self.inner.lock().await;

        let (updates, ret) = update_lock.register_new_xorb(xorb_hash, xorb_size);

        if !updates.is_empty() {
            self.progress_reporter.register_updates(updates).await;
        }

        ret
    }

    /// Register a list of (file_id, xorb_hash, usize, bool)
    pub async fn register_dependencies(&self, dependencies: &[FileXorbDependency]) {
        let mut update_lock = self.inner.lock().await;

        let updates = update_lock.register_dependencies(dependencies);

        if !updates.is_empty() {
            self.progress_reporter.register_updates(updates).await;
        }
    }

    pub async fn register_xorb_upload_completion(&self, xorb_hash: MerkleHash) {
        let mut update_lock = self.inner.lock().await;

        let updates = update_lock.register_xorb_upload_completion(xorb_hash);

        if !updates.is_empty() {
            self.progress_reporter.register_updates(updates).await;
        }
    }

    pub async fn register_xorb_upload_progress(&self, xorb_hash: MerkleHash, new_byte_progress: u64) {
        self.register_xorb_upload_progress_impl(xorb_hash, new_byte_progress, true)
            .await;
    }

    pub fn register_xorb_upload_progress_background(self: Arc<Self>, xorb_hash: MerkleHash, new_byte_progress: u64) {
        // register partial progress in the background; if this happens out of order, no worries.
        tokio::spawn(async move {
            self.register_xorb_upload_progress_impl(xorb_hash, new_byte_progress, false)
                .await
        });
    }

    async fn register_xorb_upload_progress_impl(
        &self,
        xorb_hash: MerkleHash,
        new_byte_progress: u64,
        check_ordering: bool,
    ) {
        let mut update_lock = self.inner.lock().await;

        let updates = update_lock.register_xorb_upload_progress(xorb_hash, new_byte_progress, check_ordering);

        if !updates.is_empty() {
            self.progress_reporter.register_updates(updates).await;
        }
    }

    /// Async wrapper that locks the internal struct and calls the sync `verify_complete`.
    pub async fn status(&self) -> (u64, u64) {
        self.inner.lock().await.status()
    }

    /// Async wrapper that locks the internal struct and calls the sync `verify_complete`.
    pub async fn is_complete(&self) -> bool {
        self.inner.lock().await.is_complete()
    }

    /// Async wrapper that locks the internal struct and calls the sync `verify_complete`.
    pub async fn assert_complete(&self) {
        self.inner.lock().await.assert_complete();
    }

    /// Flush the progress reporter
    pub async fn flush(&self) {
        self.progress_reporter.flush().await;
    }
}

#[cfg(test)]
mod tests {
    use merklehash::MerkleHash;

    use super::*;
    use crate::no_op_tracker::NoOpProgressUpdater;
    use crate::verification_wrapper::ProgressUpdaterVerificationWrapper;

    /// A basic test showing partial updates and final completion checks
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_status_and_is_complete() {
        // 1) Create no-op + verification wrapper
        let no_op = NoOpProgressUpdater::new();
        let verifier = ProgressUpdaterVerificationWrapper::new(no_op);
        // 2) Create our CompletionTracker with the verifying reporter
        let tracker = CompletionTracker::new(verifier.clone());

        // Register two files
        let file_a = tracker.register_new_file(Ulid::new(), "fileA", Some(100)).await;
        let file_b = tracker.register_new_file(Ulid::new(), "fileB", Some(50)).await;

        // Initially, done=0, total=150
        let (done, total) = tracker.status().await;
        assert_eq!(done, 0);
        assert_eq!(total, 150);
        assert!(!tracker.is_complete().await);

        // fileA depends on x for 100 bytes, already uploaded
        let x = MerkleHash::random_from_seed(1);
        tracker
            .register_dependencies(&[FileXorbDependency {
                file_id: file_a,
                xorb_hash: x,
                n_bytes: 100,
                is_external: true,
            }])
            .await;

        // Now fileA is 100/100, fileB is 0/50 => done=100, total=150
        let (done, total) = tracker.status().await;
        assert_eq!(done, 100);
        assert_eq!(total, 150);
        assert!(!tracker.is_complete().await);

        // fileB depends on y for 50 bytes, not yet uploaded
        let y = MerkleHash::random_from_seed(2);
        tracker
            .register_dependencies(&[FileXorbDependency {
                file_id: file_b,
                xorb_hash: y,
                n_bytes: 50,
                is_external: false,
            }])
            .await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 100);
        assert_eq!(total, 150);

        // Now upload y
        tracker.register_xorb_upload_completion(y).await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 150);
        assert_eq!(total, 150);
        assert!(tracker.is_complete().await);

        // Confirm internal consistency in the tracker
        tracker.assert_complete().await;
        // Confirm the updates themselves were valid
        verifier.assert_complete().await;
    }

    /// Multiple files sharing one xorb, with partial "already uploaded" logic
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple_files_one_shared_xorb() {
        let no_op = NoOpProgressUpdater::new();
        let verifier = ProgressUpdaterVerificationWrapper::new(no_op);
        let tracker = CompletionTracker::new(verifier.clone());

        // Two files => 200 + 300 = 500 total
        let file_a = tracker.register_new_file(Ulid::new(), "fileA", Some(200)).await;
        let file_b = tracker.register_new_file(Ulid::new(), "fileB", Some(300)).await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 0);
        assert_eq!(total, 500);

        // Shared xorb
        let xhash = MerkleHash::random_from_seed(1);

        tracker.register_new_xorb(xhash, 1000).await;

        // fileA => xhash 100 bytes (not uploaded)
        // fileB => xhash 200 bytes (already uploaded)
        tracker
            .register_dependencies(&[
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
            ])
            .await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 200); // fileB got immediate 200
        assert_eq!(total, 500);
        assert!(!tracker.is_complete().await);

        // Mark xhash fully uploaded => fileA +100
        tracker.register_xorb_upload_completion(xhash).await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 300); // A:100 + B:200
        assert_eq!(total, 500);

        // Suppose fileA is 100/200. We'll "fix" it with x2 => 100 bytes (already uploaded)
        let x2 = MerkleHash::random_from_seed(2);

        tracker.register_new_xorb(x2, 1000).await;

        tracker
            .register_dependencies(&[FileXorbDependency {
                file_id: file_a,
                xorb_hash: x2,
                n_bytes: 100,
                is_external: true,
            }])
            .await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 400); // A:200, B:200
        assert_eq!(total, 500);

        // B's remaining 100 bytes also from x2, not uploaded
        tracker
            .register_dependencies(&[FileXorbDependency {
                file_id: file_b,
                xorb_hash: x2,
                n_bytes: 100,
                is_external: false,
            }])
            .await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 400);
        assert_eq!(total, 500);
        assert!(!tracker.is_complete().await);

        // Upload x2 => B now 300/300
        tracker.register_xorb_upload_completion(x2).await;
        let (done, total) = tracker.status().await;
        assert_eq!(done, 500);
        assert_eq!(total, 500);
        assert!(tracker.is_complete().await);

        tracker.assert_complete().await;
        verifier.assert_complete().await;
    }

    /// One file, multiple xorbs, partial "already_uploaded" scenario
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_single_file_multiple_xorbs() {
        let no_op = NoOpProgressUpdater::new();
        let verifier = ProgressUpdaterVerificationWrapper::new(no_op);
        let tracker = CompletionTracker::new(verifier.clone());

        let f = tracker.register_new_file(Ulid::new(), "bigFile", Some(300)).await;

        let x1 = MerkleHash::random_from_seed(1);
        let x2 = MerkleHash::random_from_seed(2);
        let x3 = MerkleHash::random_from_seed(3);

        tracker.register_new_xorb(x1, 100).await;
        tracker.register_new_xorb(x3, 100).await;

        // bigFile depends on:
        // x1 => 100 bytes, not uploaded
        // x2 => 100 bytes, already uploaded
        // x3 => 100 bytes, not uploaded
        tracker
            .register_dependencies(&[
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
            ])
            .await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 100); // from x2
        assert_eq!(total, 300);
        assert!(!tracker.is_complete().await);

        // Upload x1 => bigFile from 100 -> 200
        tracker.register_xorb_upload_completion(x1).await;
        let (done, total) = tracker.status().await;
        assert_eq!(done, 200);
        assert_eq!(total, 300);
        assert!(!tracker.is_complete().await);

        // Upload x3 => bigFile from 200 -> 300
        tracker.register_xorb_upload_completion(x3).await;
        let (done, total) = tracker.status().await;
        assert_eq!(done, 300);
        assert_eq!(total, 300);
        assert!(tracker.is_complete().await);

        tracker.assert_complete().await;
        verifier.assert_complete().await;
    }

    /// Xorb is completed before dependencies are registered,
    /// but the tracker credits the file immediately upon dependency registration
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_xorb_completed_before_dependencies() {
        let no_op = NoOpProgressUpdater::new();
        let verifier = ProgressUpdaterVerificationWrapper::new(no_op);
        let tracker = CompletionTracker::new(verifier.clone());

        // One file, 50 bytes
        let file_id = tracker.register_new_file(Ulid::new(), "lateFile", Some(50)).await;

        // xhash completed before we mention any dependencies
        let x = MerkleHash::random_from_seed(999);
        tracker.register_new_xorb(x, 1000).await;

        tracker.register_xorb_upload_completion(x).await;

        // Now we register that file depends on x for 50 bytes, "already_uploaded=false"
        // but the tracker sees x is completed => immediate credit.
        tracker
            .register_dependencies(&[FileXorbDependency {
                file_id,
                xorb_hash: x,
                n_bytes: 50,
                is_external: false,
            }])
            .await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 50);
        assert_eq!(total, 50);
        assert!(tracker.is_complete().await);

        tracker.assert_complete().await;
        verifier.assert_complete().await;
    }

    /// Demonstrates leftover references if we do contradictory logic,
    /// but with the updated logic, the tracker sees x is completed and
    /// grants immediate credit anyway.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_contradictory_logic_with_completed_xorb() {
        let no_op = NoOpProgressUpdater::new();
        let verifier = ProgressUpdaterVerificationWrapper::new(no_op);
        let tracker = CompletionTracker::new(verifier.clone());

        let file_id = tracker.register_new_file(Ulid::new(), "someFile", Some(100)).await;
        let x = MerkleHash::random_from_seed(123);

        tracker.register_new_xorb(x, 1000).await;

        // Mark x as completed, no dependencies known
        tracker.register_xorb_upload_completion(x).await;

        // Then register a dependency with "already_uploaded=false"
        // The code sees x.is_completed==true => immediate credit for 100 bytes
        tracker
            .register_dependencies(&[FileXorbDependency {
                file_id,
                xorb_hash: x,
                n_bytes: 100,
                is_external: false,
            }])
            .await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 100);
        assert_eq!(total, 100);
        assert!(tracker.is_complete().await);

        tracker.assert_complete().await;
        verifier.assert_complete().await;
    }

    /// Register a file with no initial size, then grow it incrementally.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_increment_file_size_basic() {
        let no_op = NoOpProgressUpdater::new();
        let verifier = ProgressUpdaterVerificationWrapper::new(no_op);
        let tracker = CompletionTracker::new(verifier.clone());

        // Register file with unknown size
        let file_id = tracker.register_new_file(Ulid::new(), "growingFile", None).await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 0);
        assert_eq!(total, 0);

        // Increment size in steps
        tracker.increment_file_size(file_id, 100).await;
        let (done, total) = tracker.status().await;
        assert_eq!(done, 0);
        assert_eq!(total, 100);

        tracker.increment_file_size(file_id, 150).await;
        let (done, total) = tracker.status().await;
        assert_eq!(done, 0);
        assert_eq!(total, 250);

        tracker.increment_file_size(file_id, 50).await;
        let (done, total) = tracker.status().await;
        assert_eq!(done, 0);
        assert_eq!(total, 300);

        // Complete the file via an external dependency
        let x = MerkleHash::random_from_seed(1);
        tracker
            .register_dependencies(&[FileXorbDependency {
                file_id,
                xorb_hash: x,
                n_bytes: 300,
                is_external: true,
            }])
            .await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 300);
        assert_eq!(total, 300);
        assert!(tracker.is_complete().await);

        tracker.assert_complete().await;
        verifier.assert_complete().await;
    }

    /// Register a file with unknown size, increment alongside dependency registration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_increment_file_size_with_xorb_uploads() {
        let no_op = NoOpProgressUpdater::new();
        let verifier = ProgressUpdaterVerificationWrapper::new(no_op);
        let tracker = CompletionTracker::new(verifier.clone());

        let file_id = tracker.register_new_file(Ulid::new(), "streamFile", None).await;

        let x1 = MerkleHash::random_from_seed(10);
        let x2 = MerkleHash::random_from_seed(20);

        tracker.register_new_xorb(x1, 500).await;
        tracker.register_new_xorb(x2, 500).await;

        // Discover first chunk: increment by 200, register dependency on x1 for 200 bytes
        tracker.increment_file_size(file_id, 200).await;
        tracker
            .register_dependencies(&[FileXorbDependency {
                file_id,
                xorb_hash: x1,
                n_bytes: 200,
                is_external: false,
            }])
            .await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 0);
        assert_eq!(total, 200);

        // Upload x1 => file goes to 200/200 so far
        tracker.register_xorb_upload_completion(x1).await;
        let (done, total) = tracker.status().await;
        assert_eq!(done, 200);
        assert_eq!(total, 200);

        // Discover second chunk: increment by 300, register dependency on x2 for 300 bytes
        tracker.increment_file_size(file_id, 300).await;
        tracker
            .register_dependencies(&[FileXorbDependency {
                file_id,
                xorb_hash: x2,
                n_bytes: 300,
                is_external: false,
            }])
            .await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 200);
        assert_eq!(total, 500);

        // Upload x2 => file goes to 500/500
        tracker.register_xorb_upload_completion(x2).await;
        let (done, total) = tracker.status().await;
        assert_eq!(done, 500);
        assert_eq!(total, 500);
        assert!(tracker.is_complete().await);

        tracker.assert_complete().await;
        verifier.assert_complete().await;
    }

    /// Multiple files, one with known size and one with unknown size that gets incremented.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_increment_file_size_mixed_known_unknown() {
        let no_op = NoOpProgressUpdater::new();
        let verifier = ProgressUpdaterVerificationWrapper::new(no_op);
        let tracker = CompletionTracker::new(verifier.clone());

        // fileA has known size, fileB does not
        let file_a = tracker.register_new_file(Ulid::new(), "fileA", Some(100)).await;
        let file_b = tracker.register_new_file(Ulid::new(), "fileB", None).await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 0);
        assert_eq!(total, 100);

        // Complete fileA immediately via external dep
        let xa = MerkleHash::random_from_seed(1);
        tracker
            .register_dependencies(&[FileXorbDependency {
                file_id: file_a,
                xorb_hash: xa,
                n_bytes: 100,
                is_external: true,
            }])
            .await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 100);
        assert_eq!(total, 100);

        // fileB discovers its size incrementally and gets deps
        tracker.increment_file_size(file_b, 200).await;
        let (done, total) = tracker.status().await;
        assert_eq!(done, 100);
        assert_eq!(total, 300);

        let xb = MerkleHash::random_from_seed(2);
        tracker.register_new_xorb(xb, 200).await;
        tracker
            .register_dependencies(&[FileXorbDependency {
                file_id: file_b,
                xorb_hash: xb,
                n_bytes: 200,
                is_external: false,
            }])
            .await;

        tracker.register_xorb_upload_completion(xb).await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 300);
        assert_eq!(total, 300);
        assert!(tracker.is_complete().await);

        tracker.assert_complete().await;
        verifier.assert_complete().await;
    }

    /// File registered with Some(size) ignores increment_file_size calls.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_increment_file_size_ignored_when_already_final() {
        let no_op = NoOpProgressUpdater::new();
        let verifier = ProgressUpdaterVerificationWrapper::new(no_op);
        let tracker = CompletionTracker::new(verifier.clone());

        // Register with a known size (is_final_size_known = true)
        let file_id = tracker.register_new_file(Ulid::new(), "fixedFile", Some(100)).await;

        // Attempt to increment -- should be ignored
        tracker.increment_file_size(file_id, 999).await;
        let (_, total) = tracker.status().await;
        assert_eq!(total, 100);

        // Complete the file
        let x = MerkleHash::random_from_seed(1);
        tracker
            .register_dependencies(&[FileXorbDependency {
                file_id,
                xorb_hash: x,
                n_bytes: 100,
                is_external: true,
            }])
            .await;

        assert!(tracker.is_complete().await);
        tracker.assert_complete().await;
        verifier.assert_complete().await;
    }

    /// File size increment with partial xorb upload progress.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_increment_file_size_with_partial_xorb_progress() {
        let no_op = NoOpProgressUpdater::new();
        let verifier = ProgressUpdaterVerificationWrapper::new(no_op);
        let tracker = CompletionTracker::new(verifier.clone());

        let file_id = tracker.register_new_file(Ulid::new(), "partialFile", None).await;

        let x = MerkleHash::random_from_seed(42);
        tracker.register_new_xorb(x, 1000).await;

        // Increment to initial size, register dep
        tracker.increment_file_size(file_id, 400).await;
        tracker
            .register_dependencies(&[FileXorbDependency {
                file_id,
                xorb_hash: x,
                n_bytes: 400,
                is_external: false,
            }])
            .await;

        // Partial upload progress on the xorb
        tracker.register_xorb_upload_progress(x, 500).await;
        let (done, total) = tracker.status().await;
        assert_eq!(total, 400);
        // Partial progress means some fraction of 400 is done
        assert!(done > 0);
        assert!(done < 400);

        // Grow the file by 200 more bytes
        tracker.increment_file_size(file_id, 200).await;
        let (_, total) = tracker.status().await;
        assert_eq!(total, 600);

        // Register the additional 200 bytes as external (already uploaded)
        tracker
            .register_dependencies(&[FileXorbDependency {
                file_id,
                xorb_hash: MerkleHash::random_from_seed(99),
                n_bytes: 200,
                is_external: true,
            }])
            .await;

        // Complete the xorb
        tracker.register_xorb_upload_completion(x).await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 600);
        assert_eq!(total, 600);
        assert!(tracker.is_complete().await);

        tracker.assert_complete().await;
        verifier.assert_complete().await;
    }
}
