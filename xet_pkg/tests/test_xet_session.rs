//! Integration tests for the XetSession API.
//!
//! Exercises upload and download round trips across all supported executor
//! contexts and runtime modes:
//!
//! - **Tokio async** (External mode): standard `#[tokio::test]` tests.
//! - **Blocking** (Owned mode): sync `build()` + `_blocking` methods.
//! - **Non-tokio async bridge** (Owned mode): `futures::executor`, `smol`, `async-std` driving async methods via
//!   `bridge_to_owned`.
//! - **Deficient tokio runtime** (fallback to Owned mode): tokio runtimes missing IO/time drivers or using
//!   `current_thread` flavor.
//! - **Blocking from non-tokio executors**: `_blocking` methods called from within smol/async-std/futures executor
//!   contexts.

use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use tempfile::{TempDir, tempdir};
use xet::XetError;
use xet::xet_session::{Sha256Policy, XetFileInfo, XetSession, XetSessionBuilder};

// ── Helpers ──────────────────────────────────────────────────────────────

fn local_endpoint(temp: &TempDir) -> String {
    let cas_path = temp.path().join("cas");
    format!("local://{}", cas_path.display())
}

async fn async_session(temp: &TempDir) -> XetSession {
    XetSessionBuilder::new()
        .with_endpoint(local_endpoint(temp))
        .build_async()
        .await
        .unwrap()
}

fn sync_session(temp: &TempDir) -> XetSession {
    XetSessionBuilder::new().with_endpoint(local_endpoint(temp)).build().unwrap()
}

async fn upload_bytes_async(session: &XetSession, data: &[u8], name: &str) -> XetFileInfo {
    let commit = session.new_upload_commit().await.unwrap();
    let handle = commit
        .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some(name.into()))
        .await
        .unwrap();
    let meta = handle.finish().await.unwrap();
    commit.commit().await.unwrap();
    meta.xet_info
}

fn upload_bytes_sync(session: &XetSession, data: &[u8], name: &str) -> XetFileInfo {
    let commit = session.new_upload_commit_blocking().unwrap();
    let handle = commit
        .upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some(name.into()))
        .unwrap();
    let meta = handle.finish_blocking().unwrap();
    commit.commit_blocking().unwrap();
    meta.xet_info
}

async fn assert_roundtrip_async(session: &XetSession, temp: &TempDir, data: &[u8], name: &str) {
    let file_info = upload_bytes_async(session, data, name).await;
    assert_eq!(file_info.file_size, Some(data.len() as u64));

    let dest = temp.path().join(format!("{name}.out"));
    let group = session.new_download_group().await.unwrap();
    group.download_file_to_path(file_info, dest.clone()).await.unwrap();
    group.finish().await.unwrap();
    assert_eq!(fs::read(&dest).unwrap(), data);
}

fn assert_roundtrip_sync(session: &XetSession, temp: &TempDir, data: &[u8], name: &str) {
    let file_info = upload_bytes_sync(session, data, name);
    assert_eq!(file_info.file_size, Some(data.len() as u64));

    let dest = temp.path().join(format!("{name}.out"));
    let group = session.new_download_group_blocking().unwrap();
    group.download_file_to_path_blocking(file_info, dest.clone()).unwrap();
    group.finish_blocking().unwrap();
    assert_eq!(fs::read(&dest).unwrap(), data);
}

async fn assert_upload_from_path_roundtrip_async(
    session: &XetSession,
    temp: &TempDir,
    src_name: &str,
    dest_name: &str,
    data: &[u8],
) {
    let src = temp.path().join(src_name);
    fs::write(&src, data).unwrap();

    let commit = session.new_upload_commit().await.unwrap();
    let handle = commit.upload_from_path(src, Sha256Policy::Compute).await.unwrap();
    let meta = handle.finish().await.unwrap();
    drop(handle);
    commit.commit().await.unwrap();
    drop(commit);

    let dest = temp.path().join(dest_name);
    let group = session.new_download_group().await.unwrap();
    group.download_file_to_path(meta.xet_info, dest.clone()).await.unwrap();
    group.finish().await.unwrap();
    assert_eq!(fs::read(&dest).unwrap(), data);
}

fn assert_upload_from_path_roundtrip_sync(
    session: &XetSession,
    temp: &TempDir,
    src_name: &str,
    dest_name: &str,
    data: &[u8],
) {
    let src = temp.path().join(src_name);
    fs::write(&src, data).unwrap();

    let commit = session.new_upload_commit_blocking().unwrap();
    let handle = commit.upload_from_path_blocking(src, Sha256Policy::Compute).unwrap();
    let meta = handle.finish_blocking().unwrap();
    drop(handle);
    commit.commit_blocking().unwrap();
    drop(commit);

    let dest = temp.path().join(dest_name);
    let group = session.new_download_group_blocking().unwrap();
    group.download_file_to_path_blocking(meta.xet_info, dest.clone()).unwrap();
    group.finish_blocking().unwrap();
    assert_eq!(fs::read(&dest).unwrap(), data);
}

type BoxFuture = Pin<Box<dyn Future<Output = ()>>>;

#[derive(Clone, Copy, Debug)]
enum NonTokioExecutor {
    Futures,
    Smol,
    AsyncStd,
}

impl NonTokioExecutor {
    const ALL: [Self; 3] = [Self::Futures, Self::Smol, Self::AsyncStd];

    fn label(self) -> &'static str {
        match self {
            Self::Futures => "futures",
            Self::Smol => "smol",
            Self::AsyncStd => "async_std",
        }
    }

    fn run(self, future: BoxFuture) {
        match self {
            Self::Futures => futures::executor::block_on(future),
            Self::Smol => smol::block_on(future),
            Self::AsyncStd => async_std::task::block_on(future),
        }
    }
}

fn run_on_all_non_tokio_executors<F>(mut scenario: F)
where
    F: FnMut(NonTokioExecutor) -> BoxFuture,
{
    for executor in NonTokioExecutor::ALL {
        executor.run(scenario(executor));
    }
}

type RuntimeBuilder = fn() -> tokio::runtime::Runtime;

fn build_rt_no_drivers() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().build().unwrap()
}

fn build_rt_no_io() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().enable_time().build().unwrap()
}

fn build_rt_no_time() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().enable_io().build().unwrap()
}

#[cfg(not(target_family = "wasm"))]
fn build_rt_current_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

fn deficient_runtime_cases() -> Vec<(&'static str, RuntimeBuilder)> {
    let mut cases: Vec<(&'static str, RuntimeBuilder)> = vec![
        ("no_drivers", build_rt_no_drivers),
        ("no_io", build_rt_no_io),
        ("no_time", build_rt_no_time),
    ];
    #[cfg(not(target_family = "wasm"))]
    {
        cases.push(("current_thread", build_rt_current_thread));
    }
    cases
}

// ── 1. Async tokio tests (External mode) ─────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_upload_bytes_roundtrip() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp).await;
    assert_roundtrip_async(&session, &temp, b"async upload bytes test", "bytes").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_upload_from_path_roundtrip() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp).await;

    let src = temp.path().join("source.bin");
    let data = b"upload from path integration test content";
    fs::write(&src, data).unwrap();

    let commit = session.new_upload_commit().await.unwrap();
    let handle = commit.upload_from_path(src, Sha256Policy::Compute).await.unwrap();
    let meta = handle.finish().await.unwrap();
    drop(handle);
    commit.commit().await.unwrap();
    drop(commit);
    assert_eq!(meta.xet_info.file_size, Some(data.len() as u64));
    assert!(meta.xet_info.sha256.is_some());

    let dest = temp.path().join("dest.bin");
    let group = session.new_download_group().await.unwrap();
    group.download_file_to_path(meta.xet_info, dest.clone()).await.unwrap();
    group.finish().await.unwrap();
    assert_eq!(fs::read(&dest).unwrap(), data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_multiple_files_in_one_commit() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp).await;

    let files: Vec<(&str, &[u8])> = vec![
        ("alpha.bin", b"alpha content"),
        ("beta.bin", b"beta content is longer"),
        ("gamma.bin", &[0xAB; 4096]),
    ];

    let commit = session.new_upload_commit().await.unwrap();
    let mut handles = Vec::new();
    for (name, data) in &files {
        let h = commit
            .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some((*name).into()))
            .await
            .unwrap();
        handles.push(h);
    }
    commit.commit().await.unwrap();

    let mut infos = Vec::new();
    for handle in &handles {
        infos.push(handle.finish().await.unwrap().xet_info);
    }
    let task_ids: Vec<_> = handles.iter().map(|h| h.task_id()).collect();
    drop(handles);
    drop(commit);

    let group = session.new_download_group().await.unwrap();
    let mut dest_paths = Vec::new();
    for (i, info) in infos.into_iter().enumerate() {
        let dest = temp.path().join(format!("out_{}.bin", task_ids[i]));
        group.download_file_to_path(info, dest.clone()).await.unwrap();
        dest_paths.push(dest);
    }
    group.finish().await.unwrap();

    for (i, dest) in dest_paths.iter().enumerate() {
        assert_eq!(fs::read(dest).unwrap(), files[i].1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_sha256_policy_variants() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp).await;
    let provided_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();

    let commit = session.new_upload_commit().await.unwrap();

    let h_compute = commit
        .upload_bytes(b"compute sha".to_vec(), Sha256Policy::Compute, Some("compute.bin".into()))
        .await
        .unwrap();
    let h_provided = commit
        .upload_bytes(b"provided sha".to_vec(), Sha256Policy::from_hex(&provided_sha256), Some("provided.bin".into()))
        .await
        .unwrap();
    let h_skip = commit
        .upload_bytes(b"skip sha".to_vec(), Sha256Policy::Skip, Some("skip.bin".into()))
        .await
        .unwrap();

    commit.commit().await.unwrap();

    let m_compute = h_compute.finish().await.unwrap();
    assert!(m_compute.xet_info.sha256.is_some());
    assert_eq!(m_compute.xet_info.sha256.as_deref().unwrap().len(), 64);

    let m_provided = h_provided.finish().await.unwrap();
    assert_eq!(m_provided.xet_info.sha256.as_deref(), Some(provided_sha256.as_str()));

    let m_skip = h_skip.finish().await.unwrap();
    assert!(m_skip.xet_info.sha256.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_large_file_roundtrip() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp).await;
    let data: Vec<u8> = (0..65536u64).map(|i| (i % 251) as u8).collect();
    assert_roundtrip_async(&session, &temp, &data, "large").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_multiple_commits_and_groups() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp).await;

    let info_a = upload_bytes_async(&session, b"commit A data", "a.bin").await;
    let info_b = upload_bytes_async(&session, b"commit B data", "b.bin").await;

    let dest_a = temp.path().join("a.out");
    let dest_b = temp.path().join("b.out");

    let group1 = session.new_download_group().await.unwrap();
    group1.download_file_to_path(info_a, dest_a.clone()).await.unwrap();
    group1.finish().await.unwrap();

    let group2 = session.new_download_group().await.unwrap();
    group2.download_file_to_path(info_b, dest_b.clone()).await.unwrap();
    group2.finish().await.unwrap();

    assert_eq!(fs::read(&dest_a).unwrap(), b"commit A data");
    assert_eq!(fs::read(&dest_b).unwrap(), b"commit B data");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_finish_and_try_finish() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp).await;

    let commit = session.new_upload_commit().await.unwrap();
    let handle = commit
        .upload_bytes(b"status test".to_vec(), Sha256Policy::Compute, Some("status.bin".into()))
        .await
        .unwrap();

    let meta = handle.finish().await.unwrap();
    assert_eq!(meta.xet_info.file_size, Some(b"status test".len() as u64));

    assert!(handle.try_finish().is_some());
    assert!(handle.try_finish().unwrap().is_ok());

    commit.commit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_progress_tracking() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp).await;
    let data = b"progress tracking integration test data";

    let commit = session.new_upload_commit().await.unwrap();
    commit
        .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("prog.bin".into()))
        .await
        .unwrap();
    let progress_observer = commit.clone();
    commit.commit().await.unwrap();

    let report = progress_observer.get_progress();
    assert_eq!(report.total_bytes, data.len() as u64);
    assert_eq!(report.total_bytes_completed, data.len() as u64);
}

// ── Download with unknown file size ──────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_download_unknown_size_roundtrip() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp).await;
    let data = b"download with unknown size via xet_pkg";
    let file_info = upload_bytes_async(&session, data, "unknown_size.bin").await;

    let hash_only = XetFileInfo::new_hash_only(file_info.hash().to_string());

    let dest = temp.path().join("unknown_size.out");
    let group = session.new_download_group().await.unwrap();
    group.download_file_to_path(hash_only, dest.clone()).await.unwrap();
    let results = group.finish().await.unwrap();

    for result in results.values() {
        let dl = result.as_ref().as_ref().unwrap();
        assert_eq!(dl.file_info.file_size, Some(data.len() as u64));
    }
    assert_eq!(fs::read(&dest).unwrap(), data);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_download_invalid_hash_fails() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp).await;

    let group = session.new_download_group().await.unwrap();
    let handle = group
        .download_file_to_path(
            XetFileInfo {
                hash: "nonexistent_hash_abc123".to_string(),
                file_size: Some(100),
                sha256: None,
            },
            temp.path().join("missing.bin"),
        )
        .await
        .unwrap();
    let results = group.finish().await.unwrap();
    assert!(results.get(&handle.task_id).unwrap().is_err());
    assert!(matches!(handle.status().unwrap(), xet::xet_session::TaskStatus::Failed));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_upload_from_path_multiple_files() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp).await;

    let src_a = temp.path().join("src_a.bin");
    let src_b = temp.path().join("src_b.bin");
    fs::write(&src_a, b"file A from path").unwrap();
    fs::write(&src_b, [0xCD; 8192]).unwrap();

    let commit = session.new_upload_commit().await.unwrap();
    let ha = commit.upload_from_path(src_a, Sha256Policy::Compute).await.unwrap();
    let hb = commit.upload_from_path(src_b, Sha256Policy::Compute).await.unwrap();
    commit.commit().await.unwrap();

    let info_a = ha.finish().await.unwrap().xet_info;
    let info_b = hb.finish().await.unwrap().xet_info;
    drop(ha);
    drop(hb);
    drop(commit);

    let dest_a = temp.path().join("dest_a.bin");
    let dest_b = temp.path().join("dest_b.bin");
    let group = session.new_download_group().await.unwrap();
    group.download_file_to_path(info_a, dest_a.clone()).await.unwrap();
    group.download_file_to_path(info_b, dest_b.clone()).await.unwrap();
    group.finish().await.unwrap();

    assert_eq!(fs::read(&dest_a).unwrap(), b"file A from path");
    assert_eq!(fs::read(&dest_b).unwrap(), vec![0xCD; 8192]);
}

// ── 2. Blocking API tests (Owned mode) ──────────────────────────────────

#[test]
fn blocking_upload_bytes_roundtrip() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);
    assert_roundtrip_sync(&session, &temp, b"blocking upload bytes test", "bytes");
}

#[test]
fn blocking_upload_from_path_roundtrip() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);
    assert_upload_from_path_roundtrip_sync(
        &session,
        &temp,
        "source.bin",
        "dest.bin",
        b"blocking upload from path content",
    );
}

#[test]
fn blocking_multiple_files_roundtrip() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);

    let data_a = b"blocking file A";
    let data_b = b"blocking file B is longer";

    let commit = session.new_upload_commit_blocking().unwrap();
    let ha = commit
        .upload_bytes_blocking(data_a.to_vec(), Sha256Policy::Compute, Some("a.bin".into()))
        .unwrap();
    let hb = commit
        .upload_bytes_blocking(data_b.to_vec(), Sha256Policy::Compute, Some("b.bin".into()))
        .unwrap();
    commit.commit_blocking().unwrap();

    let info_a = ha.finish_blocking().unwrap().xet_info;
    let info_b = hb.finish_blocking().unwrap().xet_info;
    drop(ha);
    drop(hb);
    drop(commit);

    let dest_a = temp.path().join("a.out");
    let dest_b = temp.path().join("b.out");
    let group = session.new_download_group_blocking().unwrap();
    group.download_file_to_path_blocking(info_a, dest_a.clone()).unwrap();
    group.download_file_to_path_blocking(info_b, dest_b.clone()).unwrap();
    group.finish_blocking().unwrap();

    assert_eq!(fs::read(&dest_a).unwrap(), data_a);
    assert_eq!(fs::read(&dest_b).unwrap(), data_b);
}

#[test]
fn blocking_large_file_roundtrip() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);
    let data: Vec<u8> = (0..65536u64).map(|i| (i % 251) as u8).collect();
    assert_roundtrip_sync(&session, &temp, &data, "large");
}

#[test]
fn blocking_finish_and_try_finish() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);

    let commit = session.new_upload_commit_blocking().unwrap();
    let handle = commit
        .upload_bytes_blocking(b"status blocking".to_vec(), Sha256Policy::Compute, Some("status.bin".into()))
        .unwrap();

    let meta = handle.finish_blocking().unwrap();
    assert_eq!(meta.xet_info.file_size, Some(b"status blocking".len() as u64));

    commit.commit_blocking().unwrap();
}

#[test]
fn blocking_progress_tracking() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);
    let data = b"blocking progress tracking data";

    let commit = session.new_upload_commit_blocking().unwrap();
    commit
        .upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some("prog.bin".into()))
        .unwrap();
    let progress_observer = commit.clone();
    commit.commit_blocking().unwrap();

    let report = progress_observer.get_progress_blocking();
    assert_eq!(report.total_bytes, data.len() as u64);
    assert_eq!(report.total_bytes_completed, data.len() as u64);
}

#[test]
fn blocking_multiple_commits_and_groups() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);

    let info_a = upload_bytes_sync(&session, b"blocking commit A", "a.bin");
    let info_b = upload_bytes_sync(&session, b"blocking commit B", "b.bin");

    let dest_a = temp.path().join("a.out");
    let group1 = session.new_download_group_blocking().unwrap();
    group1.download_file_to_path_blocking(info_a, dest_a.clone()).unwrap();
    group1.finish_blocking().unwrap();
    assert_eq!(fs::read(&dest_a).unwrap(), b"blocking commit A");

    let dest_b = temp.path().join("b.out");
    let group2 = session.new_download_group_blocking().unwrap();
    group2.download_file_to_path_blocking(info_b, dest_b.clone()).unwrap();
    group2.finish_blocking().unwrap();
    assert_eq!(fs::read(&dest_b).unwrap(), b"blocking commit B");
}

// ── 3. Non-tokio async bridge tests (Owned mode) ────────────────────────
//
// build_async() from a non-tokio executor falls back to Owned mode.
// Async methods use bridge_to_owned: the future runs on the owned tokio
// pool while the caller's executor polls the oneshot receiver.

#[test]
fn bridge_upload_download_roundtrip() {
    run_on_all_non_tokio_executors(|executor| {
        let temp = tempdir().unwrap();
        let tag = executor.label().to_string();
        Box::pin(async move {
            let session = async_session(&temp).await;
            let payload = format!("{tag} executor roundtrip");
            assert_roundtrip_async(&session, &temp, payload.as_bytes(), &tag).await;
        })
    });
}

#[test]
fn bridge_multiple_files() {
    run_on_all_non_tokio_executors(|executor| {
        let temp = tempdir().unwrap();
        let tag = executor.label().to_string();
        Box::pin(async move {
            let session = async_session(&temp).await;

            let files: Vec<(String, Vec<u8>)> = vec![
                (format!("{tag}_a.bin"), format!("{tag} A").into_bytes()),
                (format!("{tag}_b.bin"), format!("{tag} B").into_bytes()),
            ];

            let commit = session.new_upload_commit().await.unwrap();
            let mut handles = Vec::new();
            for (name, data) in &files {
                handles.push(
                    commit
                        .upload_bytes(data.clone(), Sha256Policy::Compute, Some(name.clone()))
                        .await
                        .unwrap(),
                );
            }
            commit.commit().await.unwrap();

            let mut infos = Vec::new();
            for handle in &handles {
                infos.push(handle.finish().await.unwrap().xet_info);
            }
            drop(handles);
            drop(commit);

            let group = session.new_download_group().await.unwrap();
            let mut outputs = Vec::new();
            for (index, info) in infos.into_iter().enumerate() {
                let dest = temp.path().join(format!("{tag}_out_{index}.bin"));
                group.download_file_to_path(info, dest.clone()).await.unwrap();
                outputs.push(dest);
            }
            group.finish().await.unwrap();

            for (index, dest) in outputs.iter().enumerate() {
                assert_eq!(fs::read(dest).unwrap(), files[index].1);
            }
        })
    });
}

#[test]
fn bridge_upload_from_path_roundtrip() {
    run_on_all_non_tokio_executors(|executor| {
        let temp = tempdir().unwrap();
        let tag = executor.label().to_string();
        Box::pin(async move {
            let session = async_session(&temp).await;
            let payload = format!("{tag} upload from path");
            assert_upload_from_path_roundtrip_async(
                &session,
                &temp,
                &format!("src_{tag}.bin"),
                &format!("dest_{tag}.bin"),
                payload.as_bytes(),
            )
            .await;
        })
    });
}

#[test]
fn bridge_large_file_roundtrip() {
    run_on_all_non_tokio_executors(|executor| {
        let temp = tempdir().unwrap();
        let tag = executor.label().to_string();
        Box::pin(async move {
            let session = async_session(&temp).await;
            let data: Vec<u8> = (0..65536u64).map(|i| (i % 251) as u8).collect();
            assert_roundtrip_async(&session, &temp, &data, &format!("large_{tag}")).await;
        })
    });
}

// ── 4. Deficient tokio runtime tests ─────────────────────────────────────
//
// When build() is called from within a tokio runtime that lacks IO
// and/or time drivers (or uses current_thread), the handle fails
// handle_meets_requirements and the session falls back to Owned mode
// with its own full-featured runtime. The async bridge routes all work
// to that owned pool.

#[test]
fn deficient_tokio_async_roundtrip_matrix() {
    for (label, builder) in deficient_runtime_cases() {
        let rt = builder();
        let temp = tempdir().unwrap();
        rt.block_on(async {
            let session = async_session(&temp).await;
            let payload = format!("{label} async roundtrip");
            assert_roundtrip_async(&session, &temp, payload.as_bytes(), label).await;
        });
    }
}

#[test]
fn deficient_tokio_no_drivers_multiple_files() {
    let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    let temp = tempdir().unwrap();
    rt.block_on(async {
        let session = async_session(&temp).await;

        let commit = session.new_upload_commit().await.unwrap();
        let ha = commit
            .upload_bytes(b"deficient A".to_vec(), Sha256Policy::Compute, Some("a.bin".into()))
            .await
            .unwrap();
        let hb = commit
            .upload_bytes(b"deficient B".to_vec(), Sha256Policy::Compute, Some("b.bin".into()))
            .await
            .unwrap();
        commit.commit().await.unwrap();

        let info_a = ha.finish().await.unwrap().xet_info;
        let info_b = hb.finish().await.unwrap().xet_info;
        drop(ha);
        drop(hb);
        drop(commit);

        let dest_a = temp.path().join("a.out");
        let dest_b = temp.path().join("b.out");
        let group = session.new_download_group().await.unwrap();
        group.download_file_to_path(info_a, dest_a.clone()).await.unwrap();
        group.download_file_to_path(info_b, dest_b.clone()).await.unwrap();
        group.finish().await.unwrap();

        assert_eq!(fs::read(&dest_a).unwrap(), b"deficient A");
        assert_eq!(fs::read(&dest_b).unwrap(), b"deficient B");
    });
}

#[test]
fn deficient_tokio_no_drivers_upload_from_path() {
    let rt = build_rt_no_drivers();
    let temp = tempdir().unwrap();
    rt.block_on(async {
        let session = async_session(&temp).await;
        assert_upload_from_path_roundtrip_async(
            &session,
            &temp,
            "src.bin",
            "dest.bin",
            b"deficient tokio upload from path",
        )
        .await;
    });
}

#[test]
fn deficient_tokio_no_drivers_large_file() {
    let rt = build_rt_no_drivers();
    let temp = tempdir().unwrap();
    rt.block_on(async {
        let session = async_session(&temp).await;
        let data: Vec<u8> = (0..65536u64).map(|i| (i % 251) as u8).collect();
        assert_roundtrip_async(&session, &temp, &data, "large_deficient").await;
    });
}

// build() inside a deficient tokio runtime auto-falls-back to Owned mode;
// blocking API still works from a sync context afterward.
#[test]
fn deficient_tokio_handle_blocking_roundtrip() {
    for (label, builder) in [
        ("deficient", build_rt_no_drivers as RuntimeBuilder),
        ("no_io", build_rt_no_io as RuntimeBuilder),
        ("no_time", build_rt_no_time as RuntimeBuilder),
    ] {
        let rt = builder();
        let temp = tempdir().unwrap();
        let session = XetSessionBuilder::new()
            .with_endpoint(local_endpoint(&temp))
            .with_tokio_handle(rt.handle().clone())
            .build()
            .unwrap();

        let payload = format!("{label} handle blocking roundtrip");
        assert_roundtrip_sync(&session, &temp, payload.as_bytes(), &format!("{label}_blocking"));
    }
}

// ── 5. Blocking from non-tokio executor contexts ─────────────────────────
//
// _blocking methods use bridge_sync (handle.block_on) on the
// owned pool. Non-tokio executors (smol, async-std, futures) do not set a
// tokio thread-local context, so block_on does not panic.

#[test]
fn blocking_in_non_tokio_executor_roundtrip() {
    run_on_all_non_tokio_executors(|executor| {
        let temp = tempdir().unwrap();
        let session = sync_session(&temp);
        let tag = executor.label().to_string();
        Box::pin(async move {
            let payload = format!("blocking in {tag}");
            assert_roundtrip_sync(&session, &temp, payload.as_bytes(), &format!("blocking_{tag}"));
        })
    });
}

#[test]
fn blocking_in_non_tokio_executor_upload_from_path() {
    run_on_all_non_tokio_executors(|executor| {
        let temp = tempdir().unwrap();
        let session = sync_session(&temp);
        let tag = executor.label().to_string();
        Box::pin(async move {
            let payload = format!("blocking {tag} upload from path");
            assert_upload_from_path_roundtrip_sync(
                &session,
                &temp,
                &format!("src_{tag}.bin"),
                &format!("dest_{tag}.bin"),
                payload.as_bytes(),
            );
        })
    });
}

// ── 6. External-mode guard tests ─────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_mode_blocking_upload_returns_wrong_mode() {
    let session = XetSessionBuilder::new().build_async().await.unwrap();
    let err = session.new_upload_commit_blocking().err().unwrap();
    assert!(matches!(err, XetError::WrongRuntimeMode(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_mode_blocking_download_returns_wrong_mode() {
    let session = XetSessionBuilder::new().build_async().await.unwrap();
    let err = session.new_download_group_blocking().err().unwrap();
    assert!(matches!(err, XetError::WrongRuntimeMode(_)));
}

// ── 7. Abort behavior ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_abort_prevents_new_commits() {
    let session = XetSessionBuilder::new().build_async().await.unwrap();
    session.abort().unwrap();
    let err = session.new_upload_commit().await.err().unwrap();
    assert!(matches!(err, XetError::Aborted));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_abort_prevents_new_groups() {
    let session = XetSessionBuilder::new().build_async().await.unwrap();
    session.abort().unwrap();
    let err = session.new_download_group().await.err().unwrap();
    assert!(matches!(err, XetError::Aborted));
}

#[test]
fn blocking_abort_prevents_new_commits() {
    let session = XetSessionBuilder::new().build().unwrap();
    session.abort().unwrap();
    let err = session.new_upload_commit_blocking().err().unwrap();
    assert!(matches!(err, XetError::Aborted));
}

#[test]
fn blocking_abort_prevents_new_groups() {
    let session = XetSessionBuilder::new().build().unwrap();
    session.abort().unwrap();
    let err = session.new_download_group_blocking().err().unwrap();
    assert!(matches!(err, XetError::Aborted));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_abort_rejects_upload_on_existing_commit() {
    let session = XetSessionBuilder::new().build_async().await.unwrap();
    let commit = session.new_upload_commit().await.unwrap();
    session.abort().unwrap();
    let err = commit
        .upload_bytes(b"after abort".to_vec(), Sha256Policy::Compute, None)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, XetError::Aborted));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_abort_rejects_download_on_existing_group() {
    let session = XetSessionBuilder::new().build_async().await.unwrap();
    let group = session.new_download_group().await.unwrap();
    session.abort().unwrap();
    let err = group
        .download_file_to_path(
            XetFileInfo {
                hash: "abc".to_string(),
                file_size: Some(1),
                sha256: None,
            },
            PathBuf::from("dest.bin"),
        )
        .await
        .err()
        .unwrap();
    assert!(matches!(err, XetError::Aborted));
}

// ── 8. Deduplication (same content uploaded twice) ───────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_duplicate_content_produces_same_hash() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp).await;
    let data = b"deduplication test content";

    let info1 = upload_bytes_async(&session, data, "first.bin").await;
    let info2 = upload_bytes_async(&session, data, "second.bin").await;

    assert_eq!(info1.hash, info2.hash);
    assert_eq!(info1.file_size, info2.file_size);
}

// ── 9. Cross-session isolation ───────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_separate_sessions_are_isolated() {
    let temp1 = tempdir().unwrap();
    let temp2 = tempdir().unwrap();
    let session1 = async_session(&temp1).await;
    let session2 = async_session(&temp2).await;

    let info1 = upload_bytes_async(&session1, b"session 1 data", "s1.bin").await;

    let group = session2.new_download_group().await.unwrap();
    group
        .download_file_to_path(info1, temp2.path().join("cross.bin"))
        .await
        .unwrap();
    let finish_result = group.finish().await;
    match finish_result {
        Err(_) => {},
        Ok(results) => {
            assert!(results.values().any(|r| r.is_err()));
        },
    }
}
