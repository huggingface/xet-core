use std::collections::{BTreeSet, HashMap};
use std::mem::take;
use std::sync::Arc;

use deduplication::FileXorbDependency;
use merklehash::MerkleHash;
use more_asserts::debug_assert_le;
use tokio::sync::Mutex;
use utils::progress::{ProgressUpdate, TrackingProgressUpdater};

/// A type with with to track a File ID; reporting is done by Arc<str>, but
/// this ensures the bookkeeping is correct across duplicates and speeds up the
/// updates.
pub type CompletionTrackerFileId = u64;

/// Keeps track of which files depend on a given xorb.
#[derive(Default)]
struct XorbDependency {
    /// List of file indices that need this xorb.
    file_indices: BTreeSet<usize>,

    /// True if the xorb has already been updated successfully.
    is_completed: bool,
}

/// Represents a file that depends on one or more xorbs.
struct FileDependency {
    /// Human-readable name of the file.
    name: Arc<str>,
    /// Total size of this file in bytes.
    total_bytes: u64,
    /// Total bytes already uploaded for this file (across its xorbs).
    completed_bytes: u64,
    /// Mapping of xorb_hash -> number of bytes of the file contained in that xorb.  Only
    /// xorbs that are not uploaded yet are tracked here.
    /// Once an xorb is uploaded, we remove it from here (and add to `completed_bytes`).
    remaining_xorbs_parts: HashMap<MerkleHash, u64>,
}

/// Tracks all files and all xorbs, allowing you to register file
/// dependencies on xorbs and then mark xorbs as completed when they
/// are fully uploaded.
#[derive(Default)]
struct CompletionTrackerImpl {
    /// List of all files being tracked.
    files: Vec<FileDependency>,
    /// Map of xorb hash -> its dependency info (which files rely on it).
    xorbs: HashMap<MerkleHash, XorbDependency>,
}

pub struct CompletionTracker {
    inner: Mutex<CompletionTrackerImpl>,
    progress_reporter: Arc<dyn TrackingProgressUpdater>,
}

impl CompletionTrackerImpl {
    /// Registers a new file for tracking and returns an ID (its index in `files`).
    /// `n_bytes` is the total size of the file.
    fn register_new_file(&mut self, name: impl Into<Arc<str>>, n_bytes: u64) -> CompletionTrackerFileId {
        // The file's ID is simply its index in the internal `files` vector.
        let file_id = self.files.len() as CompletionTrackerFileId;

        // Create a new FileDependency record.
        let file_dependency = FileDependency {
            name: name.into(),
            total_bytes: n_bytes,
            completed_bytes: 0,
            remaining_xorbs_parts: HashMap::new(),
        };

        // Insert it into our files vector.
        self.files.push(file_dependency);

        // Return the file ID so the caller can register dependencies on this file.
        file_id
    }

    /// Registers that all or part of a given file (by `file_id`) depends on one or more
    /// xorbs; Given a list of (xorb_hash, n_bytes, already_uploaded), registers the progress.
    fn register_dependencies(&mut self, dependencies: &[FileXorbDependency]) -> Vec<ProgressUpdate> {
        let mut ret = Vec::new();

        for dep in dependencies {
            let file_entry = &mut self.files[dep.file_id as usize];

            if dep.is_external {
                // This is the freebie case, where we can just increment the progress.
                file_entry.completed_bytes += dep.n_bytes;
                debug_assert_le!(file_entry.completed_bytes, file_entry.total_bytes);

                let progress_update = ProgressUpdate {
                    item_name: file_entry.name.clone(),
                    total_count: file_entry.total_bytes,
                    completed_count: file_entry.completed_bytes,
                    update_increment: dep.n_bytes,
                };

                ret.push(progress_update);
            } else {
                // Make sure we aren't putting in an unfinished xorb, which
                // tracks with MerkleHash::marker().
                debug_assert_ne!(dep.xorb_hash, MerkleHash::marker());

                let entry = self.xorbs.entry(dep.xorb_hash).or_default();

                // If the entry has already been completed, then just mark this as completed.
                if entry.is_completed {
                    file_entry.completed_bytes += dep.n_bytes;
                    debug_assert_le!(file_entry.completed_bytes, file_entry.total_bytes);

                    let progress_update = ProgressUpdate {
                        item_name: file_entry.name.clone(),
                        total_count: file_entry.total_bytes,
                        completed_count: file_entry.completed_bytes,
                        update_increment: dep.n_bytes,
                    };
                    ret.push(progress_update);
                } else {
                    // Insert a new xorb entry if needed.
                    entry.file_indices.insert(dep.file_id as usize);
                    *file_entry.remaining_xorbs_parts.entry(dep.xorb_hash).or_default() += dep.n_bytes;
                }
            }
        }

        ret
    }

    /// Called when a xorb is finished uploading. We look up which files depend on that
    /// xorb and update their `completed_bytes`, removing the xorb from their
    /// `remaining_xorbs_parts`.
    fn register_xorb_upload_completion(&mut self, xorb_hash: MerkleHash) -> Vec<ProgressUpdate> {
        let file_indices = {
            // Mark as completed, return the list of files to mark as completed.
            let entry = self.xorbs.entry(xorb_hash).or_default();

            // It's possible that this was already uploaded previously due to mulitple identical files
            // in the mix.  In this case, don't register it a second time.
            if entry.is_completed {
                return vec![];
            }

            entry.is_completed = true;
            take(&mut entry.file_indices)
        };

        // Mark all the relevant files as completed
        let mut entry_update_list = Vec::with_capacity(file_indices.len());

        // For each file that depends on this xorb, remove the relevant
        // part from `remaining_xorbs_parts` and add to `completed_bytes`.
        for file_id in file_indices {
            let file_entry = &mut self.files[file_id];

            debug_assert!(file_entry.remaining_xorbs_parts.contains_key(&xorb_hash));

            // This xorb is completed, so remove the number of bytes in that file needed by that xorb.
            let n_bytes = file_entry.remaining_xorbs_parts.remove(&xorb_hash).unwrap_or(0);
            debug_assert_le!(n_bytes + file_entry.completed_bytes, file_entry.total_bytes);
            file_entry.completed_bytes += n_bytes;

            let progress_update = ProgressUpdate {
                item_name: file_entry.name.clone(),
                total_count: file_entry.total_bytes,
                completed_count: file_entry.completed_bytes,
                update_increment: n_bytes,
            };

            entry_update_list.push(progress_update);
        }

        entry_update_list
    }

    pub fn status(&self) -> (u64, u64) {
        let (mut sum_completed, mut sum_total) = (0, 0);
        for file in &self.files {
            sum_completed += file.completed_bytes;
            sum_total += file.total_bytes;
        }
        (sum_completed, sum_total)
    }

    pub fn is_complete(&self) -> bool {
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

    pub async fn register_new_file(&self, name: impl Into<Arc<str>>, n_bytes: u64) -> CompletionTrackerFileId {
        self.inner.lock().await.register_new_file(name, n_bytes)
    }

    /// Register a list of (file_id, xorb_hash, usize, bool)
    pub async fn register_dependencies(&self, dependencies: &[FileXorbDependency]) {
        let updates = self.inner.lock().await.register_dependencies(dependencies);

        if !updates.is_empty() {
            self.progress_reporter.register_updates(&updates).await;
        }
    }

    pub async fn register_xorb_upload_completion(&self, xorb_hash: MerkleHash) {
        let updates = self.inner.lock().await.register_xorb_upload_completion(xorb_hash);

        if !updates.is_empty() {
            self.progress_reporter.register_updates(&updates).await;
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
}

#[cfg(test)]
mod tests {
    use merklehash::MerkleHash;
    use utils::progress::{NoOpProgressUpdater, ProgressUpdaterVerificationWrapper};

    use super::*;

    /// A basic test showing partial updates and final completion checks
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_status_and_is_complete() {
        // 1) Create no-op + verification wrapper
        let no_op = NoOpProgressUpdater::new();
        let verifier = ProgressUpdaterVerificationWrapper::new(no_op);
        // 2) Create our CompletionTracker with the verifying reporter
        let tracker = CompletionTracker::new(verifier.clone());

        // Register two files
        let file_a = tracker.register_new_file("fileA", 100).await;
        let file_b = tracker.register_new_file("fileB", 50).await;

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
        let file_a = tracker.register_new_file("fileA", 200).await;
        let file_b = tracker.register_new_file("fileB", 300).await;

        let (done, total) = tracker.status().await;
        assert_eq!(done, 0);
        assert_eq!(total, 500);

        // Shared xorb
        let xhash = MerkleHash::random_from_seed(1);

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

        let f = tracker.register_new_file("bigFile", 300).await;

        let x1 = MerkleHash::random_from_seed(1);
        let x2 = MerkleHash::random_from_seed(2);
        let x3 = MerkleHash::random_from_seed(3);

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
        let file_id = tracker.register_new_file("lateFile", 50).await;

        // xhash completed before we mention any dependencies
        let x = MerkleHash::random_from_seed(999);
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

        let file_id = tracker.register_new_file("someFile", 100).await;
        let x = MerkleHash::random_from_seed(123);

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
}
