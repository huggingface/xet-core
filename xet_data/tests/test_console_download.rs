#![cfg(feature = "console")]

mod console_common;
#[allow(dead_code)]
use console_common::{install_scope, upload_random_file};
use serial_test::serial;
use xet_data::processing::test_utils::TestEnvironment;
use xet_data::processing::{FileDownloadSession, FileUploadSession};
use xet_runtime::console::model::FileDownloadState;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn download_group_and_files_visible_in_console() {
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    let env = TestEnvironment::new().await;
    let scope = install_scope(&env.config);

    // Arrange: upload one file (its commit console is also created; ignore it).
    let upload_session = FileUploadSession::new(env.config.clone()).await.unwrap();
    let xfi = upload_random_file(&upload_session, &env.base_dir, 4 << 20).await;
    upload_session.finalize().await.unwrap();

    let download_session = FileDownloadSession::new(env.config.clone(), None).await.unwrap();
    let groups = scope.live_download_groups();
    assert_eq!(groups.len(), 1);

    let out = env.base_dir.join("out.bin");
    let (_id, _n) = download_session.download_file(&xfi, &out).await.unwrap();

    let detail = groups[0].snapshot(true);
    // File should have moved to completed_files after retire
    let all: Vec<_> = detail
        .files
        .iter()
        .cloned()
        .chain(detail.completed_files.iter().map(|(_, f)| f.clone()))
        .collect();
    assert_eq!(all.len(), 1);
    let f = &all[0];
    assert_eq!(f.state, FileDownloadState::Complete);
    assert_eq!(f.file_hash.as_deref(), Some(xfi.hash.as_str()));
    assert!(f.bytes_completed > 0, "items provider must surface bytes");

    // Drop groups first so the only remaining Arc<DownloadGroupConsole> is inside
    // download_session. Then dropping the session lets the console Arc reach refcount
    // zero, triggering Drop → finalize(Aborted) → ended ring write.
    drop(groups);
    drop(download_session);
    // Poll briefly — finalize is synchronous once the Arc drops.
    for _ in 0..20 {
        if !scope.ended_download_groups().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let ended = scope.ended_download_groups();
    assert_eq!(ended.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn background_download_path_retires_files_with_counts() {
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "0") };
    let env = TestEnvironment::new().await;
    let scope = install_scope(&env.config);

    let upload_session = FileUploadSession::new(env.config.clone()).await.unwrap();
    let xfi = upload_random_file(&upload_session, &env.base_dir, 4 << 20).await;
    upload_session.finalize().await.unwrap();

    let download_session = FileDownloadSession::new(env.config.clone(), None).await.unwrap();
    let out = env.base_dir.join("out_bg.bin");
    let (_id, handle) = download_session.download_file_background(xfi.clone(), out).await.unwrap();
    handle.await.unwrap().unwrap();

    let group = scope.live_download_groups().pop().unwrap();
    let detail = group.snapshot(true);
    assert_eq!(detail.file_counts.in_flight, 0, "retired after completion");
    assert_eq!(detail.file_counts.completed, 1);
    assert!(detail.files.is_empty());
    assert_eq!(detail.completed_files.len(), 1);
    assert_eq!(detail.completed_files[0].1.state, FileDownloadState::Complete);
}
