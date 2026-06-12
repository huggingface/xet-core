#![cfg(feature = "console")]

mod console_common;
use console_common::{install_scope, upload_random_file};
use serial_test::serial;
use xet_data::processing::FileUploadSession;
use xet_data::processing::test_utils::TestEnvironment;
use xet_runtime::console::model::{FileUploadState, ShardState};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn upload_commit_lifecycle_visible_in_console() {
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    let env = TestEnvironment::new().await;
    let scope = install_scope(&env.config);

    let upload_session = FileUploadSession::new(env.config.clone()).await.unwrap();

    // Live commit appears under the scope.
    let commits = scope.live_upload_commits();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].summary().state, xet_runtime::console::model::UploadCommitState::Active);

    // Upload a file and finalize. finalize() consumes the Arc so the
    // UploadCommitConsole Arc count falls to zero after this line.
    let _xfi = upload_random_file(&upload_session, &env.base_dir, 1 << 20).await;
    upload_session.finalize().await.unwrap();

    // Commit is finalized: verify it appears in the ended ring with Completed state
    // and dedup totals. (Live-list pruning is eventually-consistent; we check ended.)
    let ended = scope.ended_upload_commits();
    assert_eq!(ended.len(), 1);
    assert_eq!(ended[0].state, xet_runtime::console::model::UploadCommitState::Completed);
    assert!(ended[0].dedup.total_bytes > 0);
    assert!(ended[0].progress.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn aborted_commit_is_visible_in_console() {
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    let env = TestEnvironment::new().await;
    let scope = install_scope(&env.config);

    let upload_session = FileUploadSession::new(env.config.clone()).await.unwrap();
    upload_session.console_mark_aborted();

    {
        let commits = scope.live_upload_commits();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].summary().state, xet_runtime::console::model::UploadCommitState::Aborted);
        // commits drops here, releasing the strong refs to UploadCommitConsole.
    }

    drop(upload_session);
    // Ended entry lands when the cell drops; poll briefly (background tasks may hold the Arc).
    for _ in 0..20 {
        if !scope.ended_upload_commits().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let ended = scope.ended_upload_commits();
    assert_eq!(ended.len(), 1);
    assert_eq!(ended[0].state, xet_runtime::console::model::UploadCommitState::Aborted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn file_states_progress_through_pipeline() {
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    let env = TestEnvironment::new().await;
    let scope = install_scope(&env.config);

    let upload_session = FileUploadSession::new(env.config.clone()).await.unwrap();
    let commit = scope.live_upload_commits().pop().unwrap();

    // multi-block: ingestion_block_size is 8 MiB
    let _xfi = upload_random_file(&upload_session, &env.base_dir, 24 << 20).await;

    // After ingestion (pre-finalize): the file must be past Chunking with hash known.
    let detail = commit.snapshot(true);
    let all_files: Vec<_> = detail
        .files
        .iter()
        .cloned()
        .chain(detail.completed_files.iter().map(|(_, f)| f.clone()))
        .collect();
    assert_eq!(all_files.len(), 1);
    let f = &all_files[0];
    assert!(f.file_hash.is_some(), "hash must be known after processing");
    assert!(f.bytes_chunked > 0);
    assert!(f.n_chunks > 0);
    assert!(f.dedup.is_some());
    assert!(
        matches!(
            f.state,
            FileUploadState::Processed
                | FileUploadState::AwaitingXorbs
                | FileUploadState::AwaitingShard
                | FileUploadState::Complete
        ),
        "unexpected state: {:?}",
        f.state
    );

    upload_session.finalize().await.unwrap();
    let ended = scope.ended_upload_commits().pop().unwrap();
    // After finalize the commit is recorded; files may be in `files` (in-flight) or
    // `completed_files` (retired after all_shards_uploaded, Task 12).
    let all_ended: Vec<_> = ended
        .files
        .iter()
        .cloned()
        .chain(ended.completed_files.iter().map(|(_, f)| f.clone()))
        .collect();
    assert_eq!(all_ended.len(), 1);
    let fe = &all_ended[0];
    // Task 12 advances to Complete via all_shards_uploaded.
    assert_eq!(fe.state, FileUploadState::Complete, "unexpected post-finalize state: {:?}", fe.state);
    assert!(fe.shard_uploaded);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn xorbs_and_shards_tracked_through_upload() {
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    let env = TestEnvironment::new().await;
    let scope = install_scope(&env.config);
    let upload_session = FileUploadSession::new(env.config.clone()).await.unwrap();

    // Random bytes are novel to the fresh local CAS, so xorbs must be formed.
    let _xfi = upload_random_file(&upload_session, &env.base_dir, 24 << 20).await;
    upload_session.finalize().await.unwrap();

    let ended = scope.ended_upload_commits().pop().unwrap();
    assert!(ended.xorbs.counts.formed >= 1, "novel data must form at least one xorb");
    assert_eq!(ended.xorbs.counts.formed, ended.xorbs.counts.uploaded + ended.xorbs.counts.failed);
    assert!(ended.xorbs.in_flight.is_empty());
    assert!(!ended.xorbs.recent.is_empty());
    assert!(ended.shards.iter().all(|s| s.state == ShardState::Uploaded));
    let (_, f) = ended.completed_files.last().unwrap();
    assert!(f.shard_uploaded);
    assert!(!f.xorb_deps.is_empty());
}
