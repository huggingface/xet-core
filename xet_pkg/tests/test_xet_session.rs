//! Integration tests for the XetSession API.
//!
//! Exercises upload and download round trips across all supported executor
//! contexts and runtime modes:
//!
//! - **Tokio async** (External mode): standard `#[tokio::test]` tests.
//! - **Blocking** (Owned mode): sync `build()` + `_blocking` methods.
//! - **Non-tokio async bridge** (Owned mode): `futures::executor`, `smol`, `async-std` driving async methods via
//!   `XetRuntime::bridge_async`.
//! - **Deficient tokio runtime** (fallback to Owned mode): tokio runtimes missing IO/time drivers or using
//!   `current_thread` flavor.
//! - **Blocking from non-tokio executors**: `_blocking` methods called from within smol/async-std/futures executor
//!   contexts.

use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use bytes::Bytes;
use tempfile::{TempDir, tempdir};
use xet::xet_session::{
    SessionError, Sha256Policy, XetDownloadStream, XetFileInfo, XetFileMetadata, XetSession, XetSessionBuilder,
    XetTaskState, XetUnorderedDownloadStream,
};

// ── Helpers ──────────────────────────────────────────────────────────────

fn local_endpoint(temp: &TempDir) -> String {
    let cas_path = temp.path().join("cas");
    format!("local://{}", cas_path.display())
}

fn async_session(temp: &TempDir) -> XetSession {
    XetSessionBuilder::new().with_endpoint(local_endpoint(temp)).build().unwrap()
}

fn sync_session(temp: &TempDir) -> XetSession {
    XetSessionBuilder::new().with_endpoint(local_endpoint(temp)).build().unwrap()
}

fn to_file_info(meta: &XetFileMetadata) -> XetFileInfo {
    meta.xet_info.clone()
}

async fn upload_bytes_async(session: &XetSession, data: &[u8], name: &str) -> XetFileInfo {
    let commit = session.new_upload_commit().await.unwrap();
    let handle = commit
        .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some(name.into()))
        .await
        .unwrap();
    let file_meta = handle.finalize_ingestion().await.unwrap();
    commit.commit().await.unwrap();
    file_meta.xet_info
}

fn upload_bytes_sync(session: &XetSession, data: &[u8], name: &str) -> XetFileInfo {
    let commit = session.new_upload_commit_blocking().unwrap();
    let handle = commit
        .upload_bytes_blocking(data.to_vec(), Sha256Policy::Compute, Some(name.into()))
        .unwrap();
    let file_meta = handle.finalize_ingestion_blocking().unwrap();
    commit.commit_blocking().unwrap();
    file_meta.xet_info
}

async fn assert_roundtrip_async(session: &XetSession, temp: &TempDir, data: &[u8], name: &str) {
    let file_info = upload_bytes_async(session, data, name).await;
    assert_eq!(file_info.file_size(), Some(data.len() as u64));

    let dest = temp.path().join(format!("{name}.out"));
    let group = session.new_file_download_group().await.unwrap();
    group.download_file_to_path(file_info, dest.clone()).await.unwrap();
    group.finish().await.unwrap();
    assert_eq!(fs::read(&dest).unwrap(), data);
}

fn assert_roundtrip_sync(session: &XetSession, temp: &TempDir, data: &[u8], name: &str) {
    let file_info = upload_bytes_sync(session, data, name);
    assert_eq!(file_info.file_size(), Some(data.len() as u64));

    let dest = temp.path().join(format!("{name}.out"));
    let group = session.new_file_download_group_blocking().unwrap();
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

    let file_meta = {
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit.upload_from_path(src, Sha256Policy::Compute).await.unwrap();
        let file_meta = handle.finalize_ingestion().await.unwrap();
        commit.commit().await.unwrap();
        file_meta
    };

    let dest = temp.path().join(dest_name);
    let group = session.new_file_download_group().await.unwrap();
    group
        .download_file_to_path(to_file_info(&file_meta), dest.clone())
        .await
        .unwrap();
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

    let file_meta = {
        let commit = session.new_upload_commit_blocking().unwrap();
        let handle = commit.upload_from_path_blocking(src, Sha256Policy::Compute).unwrap();
        let file_meta = handle.finalize_ingestion_blocking().unwrap();
        commit.commit_blocking().unwrap();
        file_meta
    };

    let dest = temp.path().join(dest_name);
    let group = session.new_file_download_group_blocking().unwrap();
    group
        .download_file_to_path_blocking(to_file_info(&file_meta), dest.clone())
        .unwrap();
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
    let session = async_session(&temp);
    assert_roundtrip_async(&session, &temp, b"async upload bytes test", "bytes").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_upload_from_path_roundtrip() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);

    let src = temp.path().join("source.bin");
    let data = b"upload from path integration test content";
    fs::write(&src, data).unwrap();

    let file_meta = {
        let commit = session.new_upload_commit().await.unwrap();
        let handle = commit.upload_from_path(src, Sha256Policy::Compute).await.unwrap();
        let file_meta = handle.finalize_ingestion().await.unwrap();
        assert_eq!(file_meta.xet_info.file_size(), Some(data.len() as u64));
        assert!(file_meta.xet_info.sha256().is_some());
        commit.commit().await.unwrap();
        file_meta
    };

    let dest = temp.path().join("dest.bin");
    let group = session.new_file_download_group().await.unwrap();
    group
        .download_file_to_path(file_meta.xet_info.clone(), dest.clone())
        .await
        .unwrap();
    group.finish().await.unwrap();
    assert_eq!(fs::read(&dest).unwrap(), data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_multiple_files_in_one_commit() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);

    let files: Vec<(&str, &[u8])> = vec![
        ("alpha.bin", b"alpha content"),
        ("beta.bin", b"beta content is longer"),
        ("gamma.bin", &[0xAB; 4096]),
    ];

    let metas = {
        let commit = session.new_upload_commit().await.unwrap();
        let mut metas = Vec::new();
        for (name, data) in &files {
            let h = commit
                .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some((*name).into()))
                .await
                .unwrap();
            metas.push(h.finalize_ingestion().await.unwrap());
        }
        commit.commit().await.unwrap();
        metas
    };

    let group = session.new_file_download_group().await.unwrap();
    let mut dest_paths = Vec::new();
    for (i, file_meta) in metas.iter().enumerate() {
        let dest = temp.path().join(format!("out_{i}.bin"));
        group
            .download_file_to_path(file_meta.xet_info.clone(), dest.clone())
            .await
            .unwrap();
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
    let session = async_session(&temp);
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

    let m_compute = h_compute.finalize_ingestion().await.unwrap();
    let m_provided = h_provided.finalize_ingestion().await.unwrap();
    let m_skip = h_skip.finalize_ingestion().await.unwrap();
    commit.commit().await.unwrap();

    assert!(m_compute.xet_info.sha256().is_some());
    assert_eq!(m_compute.xet_info.sha256().unwrap().len(), 64);

    assert_eq!(m_provided.xet_info.sha256(), Some(provided_sha256.as_str()));

    assert!(m_skip.xet_info.sha256().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_large_file_roundtrip() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let data: Vec<u8> = (0..65536u64).map(|i| (i % 251) as u8).collect();
    assert_roundtrip_async(&session, &temp, &data, "large").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_multiple_commits_and_groups() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);

    let info_a = upload_bytes_async(&session, b"commit A data", "a.bin").await;
    let info_b = upload_bytes_async(&session, b"commit B data", "b.bin").await;

    let dest_a = temp.path().join("a.out");
    let dest_b = temp.path().join("b.out");

    let group1 = session.new_file_download_group().await.unwrap();
    group1.download_file_to_path(info_a, dest_a.clone()).await.unwrap();
    group1.finish().await.unwrap();

    let group2 = session.new_file_download_group().await.unwrap();
    group2.download_file_to_path(info_b, dest_b.clone()).await.unwrap();
    group2.finish().await.unwrap();

    assert_eq!(fs::read(&dest_a).unwrap(), b"commit A data");
    assert_eq!(fs::read(&dest_b).unwrap(), b"commit B data");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_task_status_transitions() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);

    let commit = session.new_upload_commit().await.unwrap();
    let handle = commit
        .upload_bytes(b"status test".to_vec(), Sha256Policy::Compute, Some("status.bin".into()))
        .await
        .unwrap();

    assert!(handle.progress().is_some() || handle.try_finish().is_none());

    let file_meta = handle.finalize_ingestion().await.unwrap();
    assert!(file_meta.xet_info.file_size().is_some());

    commit.commit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_progress_tracking() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let data = b"progress tracking integration test data";

    let commit = session.new_upload_commit().await.unwrap();
    commit
        .upload_bytes(data.to_vec(), Sha256Policy::Compute, Some("prog.bin".into()))
        .await
        .unwrap();
    let progress_observer = commit.clone();
    commit.commit().await.unwrap();

    let report = progress_observer.progress();
    assert_eq!(report.total_bytes, data.len() as u64);
    assert_eq!(report.total_bytes_completed, data.len() as u64);
}

// ── Download with unknown file size ──────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_download_unknown_size_roundtrip() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let data = b"download with unknown size via xet_pkg";
    let file_info = upload_bytes_async(&session, data, "unknown_size.bin").await;

    let hash_only = XetFileInfo::new_hash_only(file_info.hash().to_string());

    let dest = temp.path().join("unknown_size.out");
    let group = session.new_file_download_group().await.unwrap();
    group.download_file_to_path(hash_only, dest.clone()).await.unwrap();
    let report = group.finish().await.unwrap();

    for result in report.downloads.values() {
        let dl = result.as_ref().unwrap();
        assert_eq!(dl.file_info.file_size, Some(data.len() as u64));
    }
    assert_eq!(fs::read(&dest).unwrap(), data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_download_invalid_hash_fails() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);

    let group = session.new_file_download_group().await.unwrap();
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
    let report = group.finish().await.unwrap();
    assert!(report.downloads.get(&handle.task_id()).unwrap().is_err());
    assert!(matches!(handle.status().unwrap(), XetTaskState::Error(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_upload_from_path_multiple_files() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);

    let src_a = temp.path().join("src_a.bin");
    let src_b = temp.path().join("src_b.bin");
    fs::write(&src_a, b"file A from path").unwrap();
    fs::write(&src_b, [0xCD; 8192]).unwrap();

    let (info_a, info_b) = {
        let commit = session.new_upload_commit().await.unwrap();
        let ha = commit.upload_from_path(src_a, Sha256Policy::Compute).await.unwrap();
        let hb = commit.upload_from_path(src_b, Sha256Policy::Compute).await.unwrap();
        let info_a = ha.finalize_ingestion().await.unwrap().xet_info;
        let info_b = hb.finalize_ingestion().await.unwrap().xet_info;
        commit.commit().await.unwrap();
        (info_a, info_b)
    };

    let dest_a = temp.path().join("dest_a.bin");
    let dest_b = temp.path().join("dest_b.bin");
    let group = session.new_file_download_group().await.unwrap();
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

    let (info_a, info_b) = {
        let commit = session.new_upload_commit_blocking().unwrap();
        let ha = commit
            .upload_bytes_blocking(data_a.to_vec(), Sha256Policy::Compute, Some("a.bin".into()))
            .unwrap();
        let hb = commit
            .upload_bytes_blocking(data_b.to_vec(), Sha256Policy::Compute, Some("b.bin".into()))
            .unwrap();
        let info_a = ha.finalize_ingestion_blocking().unwrap().xet_info;
        let info_b = hb.finalize_ingestion_blocking().unwrap().xet_info;
        commit.commit_blocking().unwrap();
        (info_a, info_b)
    };

    let dest_a = temp.path().join("a.out");
    let dest_b = temp.path().join("b.out");
    let group = session.new_file_download_group_blocking().unwrap();
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
fn blocking_task_status_transitions() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);

    let commit = session.new_upload_commit_blocking().unwrap();
    let handle = commit
        .upload_bytes_blocking(b"status blocking".to_vec(), Sha256Policy::Compute, Some("status.bin".into()))
        .unwrap();
    let file_meta = handle.finalize_ingestion_blocking().unwrap();
    assert!(file_meta.xet_info.file_size().is_some());
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

    let report = progress_observer.progress();
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
    let group1 = session.new_file_download_group_blocking().unwrap();
    group1.download_file_to_path_blocking(info_a, dest_a.clone()).unwrap();
    group1.finish_blocking().unwrap();
    assert_eq!(fs::read(&dest_a).unwrap(), b"blocking commit A");

    let dest_b = temp.path().join("b.out");
    let group2 = session.new_file_download_group_blocking().unwrap();
    group2.download_file_to_path_blocking(info_b, dest_b.clone()).unwrap();
    group2.finish_blocking().unwrap();
    assert_eq!(fs::read(&dest_b).unwrap(), b"blocking commit B");
}

// ── 3. Non-tokio async bridge tests (Owned mode) ────────────────────────
//
// build() from a non-tokio executor creates an Owned-mode runtime.
// Async methods use XetRuntime::bridge_async: the future runs on the owned tokio
// pool while the caller's executor polls the oneshot receiver.

#[test]
fn bridge_upload_download_roundtrip() {
    run_on_all_non_tokio_executors(|executor| {
        let temp = tempdir().unwrap();
        let tag = executor.label().to_string();
        Box::pin(async move {
            let session = async_session(&temp);
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
            let session = async_session(&temp);

            let files: Vec<(String, Vec<u8>)> = vec![
                (format!("{tag}_a.bin"), format!("{tag} A").into_bytes()),
                (format!("{tag}_b.bin"), format!("{tag} B").into_bytes()),
            ];

            let metas = {
                let commit = session.new_upload_commit().await.unwrap();
                let mut metas = Vec::new();
                for (name, data) in &files {
                    let h = commit
                        .upload_bytes(data.clone(), Sha256Policy::Compute, Some(name.clone()))
                        .await
                        .unwrap();
                    metas.push(h.finalize_ingestion().await.unwrap());
                }
                commit.commit().await.unwrap();
                metas
            };

            let group = session.new_file_download_group().await.unwrap();
            let mut outputs = Vec::new();
            for (index, file_meta) in metas.iter().enumerate() {
                let info = file_meta.xet_info.clone();
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
            let session = async_session(&temp);
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
            let session = async_session(&temp);
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
            let session = async_session(&temp);
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
        let session = async_session(&temp);

        let (info_a, info_b) = {
            let commit = session.new_upload_commit().await.unwrap();
            let ha = commit
                .upload_bytes(b"deficient A".to_vec(), Sha256Policy::Compute, Some("a.bin".into()))
                .await
                .unwrap();
            let hb = commit
                .upload_bytes(b"deficient B".to_vec(), Sha256Policy::Compute, Some("b.bin".into()))
                .await
                .unwrap();
            let info_a = ha.finalize_ingestion().await.unwrap().xet_info;
            let info_b = hb.finalize_ingestion().await.unwrap().xet_info;
            commit.commit().await.unwrap();
            (info_a, info_b)
        };

        let dest_a = temp.path().join("a.out");
        let dest_b = temp.path().join("b.out");
        let group = session.new_file_download_group().await.unwrap();
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
        let session = async_session(&temp);
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
        let session = async_session(&temp);
        let data: Vec<u8> = (0..65536u64).map(|i| (i % 251) as u8).collect();
        assert_roundtrip_async(&session, &temp, &data, "large_deficient").await;
    });
}

// build() inside a deficient tokio runtime auto-falls-back to Owned mode;
// blocking API still works from a sync context afterward.
#[test]
fn deficient_tokio_handle_auto_fallback_blocking_roundtrip() {
    for (label, builder) in [
        ("deficient", build_rt_no_drivers as RuntimeBuilder),
        ("no_io", build_rt_no_io as RuntimeBuilder),
        ("no_time", build_rt_no_time as RuntimeBuilder),
    ] {
        let rt = builder();
        let temp = tempdir().unwrap();
        let session =
            rt.block_on(async { XetSessionBuilder::new().with_endpoint(local_endpoint(&temp)).build().unwrap() });

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
    let session = XetSessionBuilder::new().build().unwrap();
    let err = session.new_upload_commit_blocking().err().unwrap();
    assert!(matches!(err, SessionError::WrongRuntimeMode(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_mode_blocking_download_returns_wrong_mode() {
    let session = XetSessionBuilder::new().build().unwrap();
    let err = session.new_file_download_group_blocking().err().unwrap();
    assert!(matches!(err, SessionError::WrongRuntimeMode(_)));
}

// ── 7. Abort behavior ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_abort_prevents_new_commits() {
    let session = XetSessionBuilder::new().build().unwrap();
    session.abort().unwrap();
    let err = session.new_upload_commit().await.err().unwrap();
    assert!(matches!(err, SessionError::UserCancelled(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_abort_prevents_new_groups() {
    let session = XetSessionBuilder::new().build().unwrap();
    session.abort().unwrap();
    let err = session.new_file_download_group().await.err().unwrap();
    assert!(matches!(err, SessionError::UserCancelled(_)));
}

#[test]
fn blocking_abort_prevents_new_commits() {
    let session = XetSessionBuilder::new().build().unwrap();
    session.abort().unwrap();
    let err = session.new_upload_commit_blocking().err().unwrap();
    assert!(matches!(err, SessionError::UserCancelled(_)));
}

#[test]
fn blocking_abort_prevents_new_groups() {
    let session = XetSessionBuilder::new().build().unwrap();
    session.abort().unwrap();
    let err = session.new_file_download_group_blocking().err().unwrap();
    assert!(matches!(err, SessionError::UserCancelled(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_abort_rejects_upload_on_existing_commit() {
    let session = XetSessionBuilder::new().build().unwrap();
    let commit = session.new_upload_commit().await.unwrap();
    session.abort().unwrap();
    let err = commit
        .upload_bytes(b"after abort".to_vec(), Sha256Policy::Compute, None)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, SessionError::UserCancelled(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_abort_rejects_download_on_existing_group() {
    let session = XetSessionBuilder::new().build().unwrap();
    let group = session.new_file_download_group().await.unwrap();
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
    assert!(matches!(err, SessionError::UserCancelled(_)));
}

// ── 8. Deduplication (same content uploaded twice) ───────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_duplicate_content_produces_same_hash() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
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
    let session1 = async_session(&temp1);
    let session2 = async_session(&temp2);

    let info1 = upload_bytes_async(&session1, b"session 1 data", "s1.bin").await;

    // Data from session1 should not be downloadable from session2 (different CAS store).
    let group = session2.new_file_download_group().await.unwrap();
    group
        .download_file_to_path(info1, temp2.path().join("cross.bin"))
        .await
        .unwrap();
    let finish_result = group.finish().await;
    match finish_result {
        Err(_) => {},
        Ok(report) => {
            assert!(report.downloads.values().any(|r| r.is_err()));
        },
    }
}

// ── 10. Streaming download (XetDownloadStream) ──────────────────────────

async fn collect_stream(stream: &mut XetDownloadStream) -> Vec<u8> {
    let mut collected = Vec::new();
    while let Some(chunk) = stream.next().await.unwrap() {
        collected.extend_from_slice(&chunk);
    }
    collected
}

fn collect_stream_blocking(stream: &mut XetDownloadStream) -> Vec<u8> {
    let mut collected = Vec::new();
    while let Some(chunk) = stream.blocking_next().unwrap() {
        collected.extend_from_slice(&chunk);
    }
    collected
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_stream_roundtrip() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let data = b"async streaming download roundtrip";
    let file_info = upload_bytes_async(&session, data, "stream.bin").await;

    let mut stream = session.download_stream(file_info, None).await.unwrap();
    assert_eq!(collect_stream(&mut stream).await, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_stream_large_file() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let data: Vec<u8> = (0..65536u64).map(|i| (i % 251) as u8).collect();
    let file_info = upload_bytes_async(&session, &data, "large_stream.bin").await;

    let mut stream = session.download_stream(file_info, None).await.unwrap();
    assert_eq!(collect_stream(&mut stream).await, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_stream_progress_tracking() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let data = b"stream progress tracking integration test";
    let file_info = upload_bytes_async(&session, data, "progress_stream.bin").await;

    let mut stream = session.download_stream(file_info, None).await.unwrap();

    let initial = stream.progress();
    assert_eq!(initial.total_bytes, data.len() as u64);
    assert_eq!(initial.bytes_completed, 0);

    let _ = collect_stream(&mut stream).await;

    let final_progress = stream.progress();
    assert_eq!(final_progress.total_bytes, data.len() as u64);
    assert_eq!(final_progress.bytes_completed, data.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_stream_multiple_sequential() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);

    let data_a = b"stream sequential A";
    let data_b = b"stream sequential B is different";
    let info_a = upload_bytes_async(&session, data_a, "seq_a.bin").await;
    let info_b = upload_bytes_async(&session, data_b, "seq_b.bin").await;

    let mut stream_a = session.download_stream(info_a, None).await.unwrap();
    assert_eq!(collect_stream(&mut stream_a).await, data_a);

    let mut stream_b = session.download_stream(info_b, None).await.unwrap();
    assert_eq!(collect_stream(&mut stream_b).await, data_b);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_stream_cancel_before_consuming() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let data = b"stream cancel test data";
    let file_info = upload_bytes_async(&session, data, "cancel_stream.bin").await;

    let mut stream = session.download_stream(file_info, None).await.unwrap();
    stream.cancel();
    assert!(stream.next().await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_stream_aborted_session() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    session.abort().unwrap();

    let result = session
        .download_stream(
            XetFileInfo {
                hash: "abc".to_string(),
                file_size: Some(1),
                sha256: None,
            },
            None,
        )
        .await;
    assert!(matches!(result, Err(SessionError::UserCancelled(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_stream_abort_cancels_active_stream() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let data: Vec<u8> = (0..65536u64).map(|i| (i % 251) as u8).collect();
    let file_info = upload_bytes_async(&session, &data, "abort_stream.bin").await;

    let mut stream = session.download_stream(file_info, None).await.unwrap();
    session.abort().unwrap();

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert!(first.is_none());
    assert!(stream.next().await.unwrap().is_none());
}

#[test]
fn blocking_stream_roundtrip() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);
    let data = b"blocking streaming download roundtrip";
    let file_info = upload_bytes_sync(&session, data, "stream.bin");

    let mut stream = session.download_stream_blocking(file_info, None).unwrap();
    assert_eq!(collect_stream_blocking(&mut stream), data);
}

#[test]
fn blocking_stream_large_file() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);
    let data: Vec<u8> = (0..65536u64).map(|i| (i % 251) as u8).collect();
    let file_info = upload_bytes_sync(&session, &data, "large_stream.bin");

    let mut stream = session.download_stream_blocking(file_info, None).unwrap();
    assert_eq!(collect_stream_blocking(&mut stream), data);
}

#[test]
fn blocking_stream_progress_tracking() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);
    let data = b"blocking stream progress integration test";
    let file_info = upload_bytes_sync(&session, data, "progress_stream.bin");

    let mut stream = session.download_stream_blocking(file_info, None).unwrap();
    let _ = collect_stream_blocking(&mut stream);

    let final_progress = stream.progress();
    assert_eq!(final_progress.total_bytes, data.len() as u64);
    assert_eq!(final_progress.bytes_completed, data.len() as u64);
}

#[test]
fn blocking_stream_multiple_sequential() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);

    let data_a = b"blocking stream seq A";
    let data_b = b"blocking stream seq B is longer";
    let info_a = upload_bytes_sync(&session, data_a, "seq_a.bin");
    let info_b = upload_bytes_sync(&session, data_b, "seq_b.bin");

    let mut stream_a = session.download_stream_blocking(info_a, None).unwrap();
    assert_eq!(collect_stream_blocking(&mut stream_a), data_a);

    let mut stream_b = session.download_stream_blocking(info_b, None).unwrap();
    assert_eq!(collect_stream_blocking(&mut stream_b), data_b);
}

#[test]
fn blocking_stream_aborted_session() {
    let session = XetSessionBuilder::new().build().unwrap();
    session.abort().unwrap();

    let result = session.download_stream_blocking(
        XetFileInfo {
            hash: "abc".to_string(),
            file_size: Some(1),
            sha256: None,
        },
        None,
    );
    assert!(matches!(result, Err(SessionError::UserCancelled(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_mode_blocking_stream_returns_wrong_mode() {
    let session = XetSessionBuilder::new().build().unwrap();
    let result = session.download_stream_blocking(
        XetFileInfo {
            hash: "abc".to_string(),
            file_size: Some(1),
            sha256: None,
        },
        None,
    );
    assert!(matches!(result, Err(SessionError::WrongRuntimeMode(_))));
}

#[test]
fn bridge_stream_roundtrip() {
    run_on_all_non_tokio_executors(|executor| {
        let temp = tempdir().unwrap();
        let tag = executor.label().to_string();
        Box::pin(async move {
            let session = async_session(&temp);
            let payload = format!("{tag} stream roundtrip");
            let file_info = upload_bytes_async(&session, payload.as_bytes(), &format!("{tag}_stream.bin")).await;

            let mut stream = session.download_stream(file_info, None).await.unwrap();
            assert_eq!(collect_stream(&mut stream).await, payload.as_bytes());
        })
    });
}

#[test]
fn deficient_tokio_stream_roundtrip() {
    for (label, builder) in deficient_runtime_cases() {
        let rt = builder();
        let temp = tempdir().unwrap();
        rt.block_on(async {
            let session = async_session(&temp);
            let payload = format!("{label} deficient stream");
            let file_info = upload_bytes_async(&session, payload.as_bytes(), &format!("{label}_stream.bin")).await;

            let mut stream = session.download_stream(file_info, None).await.unwrap();
            assert_eq!(collect_stream(&mut stream).await, payload.as_bytes());
        });
    }
}

#[test]
fn blocking_stream_in_non_tokio_executor() {
    run_on_all_non_tokio_executors(|executor| {
        let temp = tempdir().unwrap();
        let session = sync_session(&temp);
        let tag = executor.label().to_string();
        Box::pin(async move {
            let payload = format!("blocking stream in {tag}");
            let file_info = upload_bytes_sync(&session, payload.as_bytes(), &format!("{tag}_stream.bin"));

            let mut stream = session.download_stream_blocking(file_info, None).unwrap();
            assert_eq!(collect_stream_blocking(&mut stream), payload.as_bytes());
        })
    });
}

// ── 11. Unordered streaming download (XetUnorderedDownloadStream) ────────

fn reassemble_unordered(chunks: Vec<(u64, Bytes)>, expected_len: usize) -> Vec<u8> {
    let mut buf = vec![0u8; expected_len];
    for (offset, data) in chunks {
        buf[offset as usize..offset as usize + data.len()].copy_from_slice(&data);
    }
    buf
}

async fn collect_unordered_stream(stream: &mut XetUnorderedDownloadStream, expected_len: usize) -> Vec<u8> {
    let mut chunks = Vec::new();
    while let Some((offset, data)) = stream.next().await.unwrap() {
        chunks.push((offset, data));
    }
    reassemble_unordered(chunks, expected_len)
}

fn collect_unordered_stream_blocking(stream: &mut XetUnorderedDownloadStream, expected_len: usize) -> Vec<u8> {
    let mut chunks = Vec::new();
    while let Some((offset, data)) = stream.blocking_next().unwrap() {
        chunks.push((offset, data));
    }
    reassemble_unordered(chunks, expected_len)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_unordered_stream_roundtrip() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let data = b"async unordered streaming download roundtrip";
    let file_info = upload_bytes_async(&session, data, "unordered.bin").await;

    let mut stream = session.download_unordered_stream(file_info, None).await.unwrap();
    assert_eq!(collect_unordered_stream(&mut stream, data.len()).await, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_unordered_stream_large_file() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let data: Vec<u8> = (0..65536u64).map(|i| (i % 251) as u8).collect();
    let file_info = upload_bytes_async(&session, &data, "large_unordered.bin").await;

    let mut stream = session.download_unordered_stream(file_info, None).await.unwrap();
    assert_eq!(collect_unordered_stream(&mut stream, data.len()).await, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_unordered_stream_progress_tracking() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let data = b"unordered stream progress tracking integration test";
    let file_info = upload_bytes_async(&session, data, "progress_unordered.bin").await;

    let mut stream = session.download_unordered_stream(file_info, None).await.unwrap();

    let initial = stream.progress();
    assert_eq!(initial.total_bytes, data.len() as u64);
    assert_eq!(initial.bytes_completed, 0);

    let _ = collect_unordered_stream(&mut stream, data.len()).await;

    let final_progress = stream.progress();
    assert_eq!(final_progress.total_bytes, data.len() as u64);
    assert_eq!(final_progress.bytes_completed, data.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_unordered_stream_cancel_before_consuming() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let data = b"unordered stream cancel test data";
    let file_info = upload_bytes_async(&session, data, "cancel_unordered.bin").await;

    let mut stream = session.download_unordered_stream(file_info, None).await.unwrap();
    stream.cancel();
    assert!(stream.next().await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_unordered_stream_aborted_session() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    session.abort().unwrap();

    let result = session
        .download_unordered_stream(
            XetFileInfo {
                hash: "abc".to_string(),
                file_size: Some(1),
                sha256: None,
            },
            None,
        )
        .await;
    assert!(matches!(result, Err(SessionError::UserCancelled(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_unordered_stream_abort_cancels_active_stream() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let data: Vec<u8> = (0..65536u64).map(|i| (i % 251) as u8).collect();
    let file_info = upload_bytes_async(&session, &data, "abort_unordered_stream.bin").await;

    let mut stream = session.download_unordered_stream(file_info, None).await.unwrap();
    session.abort().unwrap();

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert!(first.is_none());
    assert!(stream.next().await.unwrap().is_none());
}

#[test]
fn blocking_unordered_stream_roundtrip() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);
    let data = b"blocking unordered streaming download roundtrip";
    let file_info = upload_bytes_sync(&session, data, "unordered.bin");

    let mut stream = session.download_unordered_stream_blocking(file_info, None).unwrap();
    assert_eq!(collect_unordered_stream_blocking(&mut stream, data.len()), data);
}

#[test]
fn blocking_unordered_stream_large_file() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);
    let data: Vec<u8> = (0..65536u64).map(|i| (i % 251) as u8).collect();
    let file_info = upload_bytes_sync(&session, &data, "large_unordered.bin");

    let mut stream = session.download_unordered_stream_blocking(file_info, None).unwrap();
    assert_eq!(collect_unordered_stream_blocking(&mut stream, data.len()), data);
}

#[test]
fn blocking_unordered_stream_progress_tracking() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);
    let data = b"blocking unordered stream progress integration test";
    let file_info = upload_bytes_sync(&session, data, "progress_unordered.bin");

    let mut stream = session.download_unordered_stream_blocking(file_info, None).unwrap();
    let _ = collect_unordered_stream_blocking(&mut stream, data.len());

    let final_progress = stream.progress();
    assert_eq!(final_progress.total_bytes, data.len() as u64);
    assert_eq!(final_progress.bytes_completed, data.len() as u64);
}

#[test]
fn blocking_unordered_stream_aborted_session() {
    let session = XetSessionBuilder::new().build().unwrap();
    session.abort().unwrap();

    let result = session.download_unordered_stream_blocking(
        XetFileInfo {
            hash: "abc".to_string(),
            file_size: Some(1),
            sha256: None,
        },
        None,
    );
    assert!(matches!(result, Err(SessionError::UserCancelled(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_mode_blocking_unordered_stream_returns_wrong_mode() {
    let session = XetSessionBuilder::new().build().unwrap();
    let result = session.download_unordered_stream_blocking(
        XetFileInfo {
            hash: "abc".to_string(),
            file_size: Some(1),
            sha256: None,
        },
        None,
    );
    assert!(matches!(result, Err(SessionError::WrongRuntimeMode(_))));
}

#[test]
fn bridge_unordered_stream_roundtrip() {
    run_on_all_non_tokio_executors(|executor| {
        let temp = tempdir().unwrap();
        let tag = executor.label().to_string();
        Box::pin(async move {
            let session = async_session(&temp);
            let payload = format!("{tag} unordered stream roundtrip");
            let file_info = upload_bytes_async(&session, payload.as_bytes(), &format!("{tag}_unordered.bin")).await;

            let mut stream = session.download_unordered_stream(file_info, None).await.unwrap();
            assert_eq!(collect_unordered_stream(&mut stream, payload.len()).await, payload.as_bytes());
        })
    });
}

#[test]
fn deficient_tokio_unordered_stream_roundtrip() {
    for (label, builder) in deficient_runtime_cases() {
        let rt = builder();
        let temp = tempdir().unwrap();
        rt.block_on(async {
            let session = async_session(&temp);
            let payload = format!("{label} deficient unordered stream");
            let file_info = upload_bytes_async(&session, payload.as_bytes(), &format!("{label}_unordered.bin")).await;

            let mut stream = session.download_unordered_stream(file_info, None).await.unwrap();
            assert_eq!(collect_unordered_stream(&mut stream, payload.len()).await, payload.as_bytes());
        });
    }
}

#[test]
fn blocking_unordered_stream_in_non_tokio_executor() {
    run_on_all_non_tokio_executors(|executor| {
        let temp = tempdir().unwrap();
        let session = sync_session(&temp);
        let tag = executor.label().to_string();
        Box::pin(async move {
            let payload = format!("blocking unordered stream in {tag}");
            let file_info = upload_bytes_sync(&session, payload.as_bytes(), &format!("{tag}_unordered.bin"));

            let mut stream = session.download_unordered_stream_blocking(file_info, None).unwrap();
            assert_eq!(collect_unordered_stream_blocking(&mut stream, payload.len()), payload.as_bytes());
        })
    });
}

// ── 12. Range downloads (DownloadStream + UnorderedDownloadStream) ───────

const RANGE_TEST_DATA: &[u8; 256] = &{
    let mut arr = [0u8; 256];
    let mut i = 0;
    while i < 256 {
        arr[i] = i as u8;
        i += 1;
    }
    arr
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_stream_range_middle() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let file_info = upload_bytes_async(&session, RANGE_TEST_DATA, "range.bin").await;

    let mut stream = session.download_stream(file_info, Some(64..192)).await.unwrap();
    assert_eq!(collect_stream(&mut stream).await, &RANGE_TEST_DATA[64..192]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_stream_range_from_start() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let file_info = upload_bytes_async(&session, RANGE_TEST_DATA, "range_start.bin").await;

    let mut stream = session.download_stream(file_info, Some(0..100)).await.unwrap();
    assert_eq!(collect_stream(&mut stream).await, &RANGE_TEST_DATA[..100]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_stream_range_to_end() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let file_info = upload_bytes_async(&session, RANGE_TEST_DATA, "range_end.bin").await;

    let mut stream = session.download_stream(file_info, Some(200..256)).await.unwrap();
    assert_eq!(collect_stream(&mut stream).await, &RANGE_TEST_DATA[200..]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_stream_range_full() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let file_info = upload_bytes_async(&session, RANGE_TEST_DATA, "range_full.bin").await;

    let mut stream = session.download_stream(file_info, Some(0..256)).await.unwrap();
    assert_eq!(collect_stream(&mut stream).await, RANGE_TEST_DATA.as_slice());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_stream_range_progress() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let file_info = upload_bytes_async(&session, RANGE_TEST_DATA, "range_progress.bin").await;

    let mut stream = session.download_stream(file_info, Some(50..150)).await.unwrap();

    let initial = stream.progress();
    assert_eq!(initial.total_bytes, 100);
    assert_eq!(initial.bytes_completed, 0);

    let _ = collect_stream(&mut stream).await;

    let final_progress = stream.progress();
    assert_eq!(final_progress.total_bytes, 100);
    assert_eq!(final_progress.bytes_completed, 100);
}

#[test]
fn blocking_stream_range_middle() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);
    let file_info = upload_bytes_sync(&session, RANGE_TEST_DATA, "range.bin");

    let mut stream = session.download_stream_blocking(file_info, Some(64..192)).unwrap();
    assert_eq!(collect_stream_blocking(&mut stream), &RANGE_TEST_DATA[64..192]);
}

#[test]
fn blocking_stream_range_progress() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);
    let file_info = upload_bytes_sync(&session, RANGE_TEST_DATA, "range_progress.bin");

    let mut stream = session.download_stream_blocking(file_info, Some(10..110)).unwrap();
    let _ = collect_stream_blocking(&mut stream);

    let final_progress = stream.progress();
    assert_eq!(final_progress.total_bytes, 100);
    assert_eq!(final_progress.bytes_completed, 100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_unordered_stream_range_middle() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let file_info = upload_bytes_async(&session, RANGE_TEST_DATA, "unord_range.bin").await;

    let mut stream = session.download_unordered_stream(file_info, Some(64..192)).await.unwrap();
    assert_eq!(collect_unordered_stream(&mut stream, 128).await, &RANGE_TEST_DATA[64..192]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_unordered_stream_range_from_start() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let file_info = upload_bytes_async(&session, RANGE_TEST_DATA, "unord_range_start.bin").await;

    let mut stream = session.download_unordered_stream(file_info, Some(0..100)).await.unwrap();
    assert_eq!(collect_unordered_stream(&mut stream, 100).await, &RANGE_TEST_DATA[..100]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_unordered_stream_range_to_end() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let file_info = upload_bytes_async(&session, RANGE_TEST_DATA, "unord_range_end.bin").await;

    let mut stream = session.download_unordered_stream(file_info, Some(200..256)).await.unwrap();
    assert_eq!(collect_unordered_stream(&mut stream, 56).await, &RANGE_TEST_DATA[200..]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_unordered_stream_range_progress() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let file_info = upload_bytes_async(&session, RANGE_TEST_DATA, "unord_range_progress.bin").await;

    let mut stream = session.download_unordered_stream(file_info, Some(50..150)).await.unwrap();

    let initial = stream.progress();
    assert_eq!(initial.total_bytes, 100);
    assert_eq!(initial.bytes_completed, 0);

    let _ = collect_unordered_stream(&mut stream, 100).await;

    let final_progress = stream.progress();
    assert_eq!(final_progress.total_bytes, 100);
    assert_eq!(final_progress.bytes_completed, 100);
}

#[test]
fn blocking_unordered_stream_range_middle() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);
    let file_info = upload_bytes_sync(&session, RANGE_TEST_DATA, "unord_range.bin");

    let mut stream = session.download_unordered_stream_blocking(file_info, Some(64..192)).unwrap();
    assert_eq!(collect_unordered_stream_blocking(&mut stream, 128), &RANGE_TEST_DATA[64..192]);
}

#[test]
fn blocking_unordered_stream_range_progress() {
    let temp = tempdir().unwrap();
    let session = sync_session(&temp);
    let file_info = upload_bytes_sync(&session, RANGE_TEST_DATA, "unord_range_progress.bin");

    let mut stream = session.download_unordered_stream_blocking(file_info, Some(10..110)).unwrap();
    let _ = collect_unordered_stream_blocking(&mut stream, 100);

    let final_progress = stream.progress();
    assert_eq!(final_progress.total_bytes, 100);
    assert_eq!(final_progress.bytes_completed, 100);
}

#[test]
fn bridge_stream_range_roundtrip() {
    run_on_all_non_tokio_executors(|executor| {
        let temp = tempdir().unwrap();
        let tag = executor.label().to_string();
        Box::pin(async move {
            let session = async_session(&temp);
            let file_info = upload_bytes_async(&session, RANGE_TEST_DATA, &format!("{tag}_range_stream.bin")).await;

            let mut stream = session.download_stream(file_info, Some(30..200)).await.unwrap();
            assert_eq!(collect_stream(&mut stream).await, &RANGE_TEST_DATA[30..200]);
        })
    });
}

#[test]
fn bridge_unordered_stream_range_roundtrip() {
    run_on_all_non_tokio_executors(|executor| {
        let temp = tempdir().unwrap();
        let tag = executor.label().to_string();
        Box::pin(async move {
            let session = async_session(&temp);
            let file_info = upload_bytes_async(&session, RANGE_TEST_DATA, &format!("{tag}_range_unord.bin")).await;

            let mut stream = session.download_unordered_stream(file_info, Some(30..200)).await.unwrap();
            assert_eq!(collect_unordered_stream(&mut stream, 170).await, &RANGE_TEST_DATA[30..200]);
        })
    });
}

#[test]
fn deficient_tokio_stream_range_roundtrip() {
    for (label, builder) in deficient_runtime_cases() {
        let rt = builder();
        let temp = tempdir().unwrap();
        rt.block_on(async {
            let session = async_session(&temp);
            let file_info = upload_bytes_async(&session, RANGE_TEST_DATA, &format!("{label}_range_stream.bin")).await;

            let mut stream = session.download_stream(file_info, Some(40..180)).await.unwrap();
            assert_eq!(collect_stream(&mut stream).await, &RANGE_TEST_DATA[40..180]);
        });
    }
}

#[test]
fn deficient_tokio_unordered_stream_range_roundtrip() {
    for (label, builder) in deficient_runtime_cases() {
        let rt = builder();
        let temp = tempdir().unwrap();
        rt.block_on(async {
            let session = async_session(&temp);
            let file_info = upload_bytes_async(&session, RANGE_TEST_DATA, &format!("{label}_range_unord.bin")).await;

            let mut stream = session.download_unordered_stream(file_info, Some(40..180)).await.unwrap();
            assert_eq!(collect_unordered_stream(&mut stream, 140).await, &RANGE_TEST_DATA[40..180]);
        });
    }
}

#[test]
fn blocking_stream_range_in_non_tokio_executor() {
    run_on_all_non_tokio_executors(|executor| {
        let temp = tempdir().unwrap();
        let session = sync_session(&temp);
        let tag = executor.label().to_string();
        Box::pin(async move {
            let file_info = upload_bytes_sync(&session, RANGE_TEST_DATA, &format!("{tag}_range_stream.bin"));

            let mut stream = session.download_stream_blocking(file_info, Some(20..220)).unwrap();
            assert_eq!(collect_stream_blocking(&mut stream), &RANGE_TEST_DATA[20..220]);
        })
    });
}

#[test]
fn blocking_unordered_stream_range_in_non_tokio_executor() {
    run_on_all_non_tokio_executors(|executor| {
        let temp = tempdir().unwrap();
        let session = sync_session(&temp);
        let tag = executor.label().to_string();
        Box::pin(async move {
            let file_info = upload_bytes_sync(&session, RANGE_TEST_DATA, &format!("{tag}_range_unord.bin"));

            let mut stream = session.download_unordered_stream_blocking(file_info, Some(20..220)).unwrap();
            assert_eq!(collect_unordered_stream_blocking(&mut stream, 200), &RANGE_TEST_DATA[20..220]);
        })
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_stream_range_large_file() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let data: Vec<u8> = (0..65536u64).map(|i| (i % 251) as u8).collect();
    let file_info = upload_bytes_async(&session, &data, "range_large.bin").await;

    let mut stream = session.download_stream(file_info, Some(10000..50000)).await.unwrap();
    assert_eq!(collect_stream(&mut stream).await, &data[10000..50000]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_unordered_stream_range_large_file() {
    let temp = tempdir().unwrap();
    let session = async_session(&temp);
    let data: Vec<u8> = (0..65536u64).map(|i| (i % 251) as u8).collect();
    let file_info = upload_bytes_async(&session, &data, "range_large_unord.bin").await;

    let mut stream = session.download_unordered_stream(file_info, Some(10000..50000)).await.unwrap();
    assert_eq!(collect_unordered_stream(&mut stream, 40000).await, &data[10000..50000]);
}
