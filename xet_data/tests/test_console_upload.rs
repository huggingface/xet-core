#![cfg(feature = "console")]

mod console_common;
use console_common::{install_scope, upload_random_file};
use serial_test::serial;
use xet_data::processing::test_utils::TestEnvironment;
use xet_data::processing::FileUploadSession;

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
