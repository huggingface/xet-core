#![cfg(feature = "simulation")]
use std::fs;
use std::sync::Arc;

use async_trait::async_trait;
use more_asserts::assert_le;
use tempfile::TempDir;
use tokio::sync::Mutex;
use xet::legacy::progress_tracking::{ItemProgressUpdate, ProgressUpdate, TrackingProgressUpdater};
use xet::legacy::{Sha256Policy, XetFileInfo, data_client};
use xet_client::cas_client::LocalTestServerBuilder;

/// A test `TrackingProgressUpdater` that records all updates.
#[derive(Debug, Default)]
struct RecordingUpdater {
    updates: Mutex<Vec<ProgressUpdate>>,
}

#[async_trait]
impl TrackingProgressUpdater for RecordingUpdater {
    async fn register_updates(&self, update: ProgressUpdate) {
        self.updates.lock().await.push(update);
    }
}

impl RecordingUpdater {
    async fn total_item_updates(&self) -> Vec<ItemProgressUpdate> {
        let updates = self.updates.lock().await;
        updates.iter().flat_map(|u| u.item_updates.clone()).collect()
    }
}

fn make_endpoint(server: &xet_client::cas_client::LocalTestServer) -> Option<String> {
    Some(server.http_endpoint().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_runtime() -> XetRuntime {
        XetRuntime::default().expect("default runtime")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_bytes_and_download_roundtrip() {
        let runtime = test_runtime();
        let server = LocalTestServerBuilder::new().start().await;
        let endpoint = make_endpoint(&server);

        let contents: Vec<Vec<u8>> = vec![b"hello world".to_vec(), b"foo bar baz".to_vec(), vec![0xAB; 4096]];
        let policies = vec![Sha256Policy::Compute; contents.len()];

        let file_infos = data_client::upload_bytes_async(
            &runtime,
            contents.clone(),
            policies,
            endpoint.clone(),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(file_infos.len(), 3);
        for info in &file_infos {
            assert!(!info.hash.is_empty());
            assert!(info.file_size.unwrap_or(0) > 0);
        }

        let download_dir = TempDir::new().unwrap();
        let download_pairs: Vec<(XetFileInfo, String)> = file_infos
            .iter()
            .enumerate()
            .map(|(i, info)| {
                let path = download_dir.path().join(format!("file_{i}"));
                (info.clone(), path.to_string_lossy().to_string())
            })
            .collect();

        let paths = data_client::download_async(&runtime, download_pairs, endpoint, None, None, None, None)
            .await
            .unwrap();

        assert_eq!(paths.len(), 3);
        for (i, path) in paths.iter().enumerate() {
            let downloaded = fs::read(path).unwrap();
            assert_eq!(downloaded, contents[i], "content mismatch for file {i}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_files_and_download_roundtrip() {
        let runtime = test_runtime();
        let server = LocalTestServerBuilder::new().start().await;
        let endpoint = make_endpoint(&server);

        let src_dir = TempDir::new().unwrap();
        let file_data: Vec<(&str, Vec<u8>)> = vec![
            ("small.txt", b"small file content".to_vec()),
            ("medium.bin", vec![0xCD; 8192]),
            ("empty.txt", vec![]),
        ];

        let mut file_paths = Vec::new();
        let mut policies = Vec::new();
        for (name, data) in &file_data {
            let path = src_dir.path().join(name);
            fs::write(&path, data).unwrap();
            file_paths.push(path.to_string_lossy().to_string());
            policies.push(Sha256Policy::Compute);
        }

        let file_infos =
            data_client::upload_async(&runtime, file_paths, policies, endpoint.clone(), None, None, None, None)
                .await
                .unwrap();

        assert_eq!(file_infos.len(), 3);

        let download_dir = TempDir::new().unwrap();
        let download_pairs: Vec<(XetFileInfo, String)> = file_infos
            .iter()
            .enumerate()
            .map(|(i, info)| {
                let path = download_dir.path().join(format!("out_{i}"));
                (info.clone(), path.to_string_lossy().to_string())
            })
            .collect();

        let paths = data_client::download_async(&runtime, download_pairs, endpoint, None, None, None, None)
            .await
            .unwrap();

        for (i, path) in paths.iter().enumerate() {
            let downloaded = fs::read(path).unwrap();
            assert_eq!(downloaded, file_data[i].1, "content mismatch for {}", file_data[i].0);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_bytes_with_progress_updater() {
        let runtime = test_runtime();
        let server = LocalTestServerBuilder::new().start().await;
        let endpoint = make_endpoint(&server);

        let contents: Vec<Vec<u8>> = vec![vec![0x42; 4096], vec![0x99; 8192]];
        let policies = vec![Sha256Policy::Compute; contents.len()];
        let updater = Arc::new(RecordingUpdater::default());

        let file_infos = data_client::upload_bytes_async(
            &runtime,
            contents,
            policies,
            endpoint,
            None,
            None,
            Some(updater.clone()),
            None,
        )
        .await
        .unwrap();

        assert_eq!(file_infos.len(), 2);

        let updates = updater.updates.lock().await;
        assert!(!updates.is_empty(), "should have received progress updates");

        let last = updates.last().unwrap();
        assert_le!(last.total_bytes_completed, last.total_bytes);

        drop(updates);
        let items = updater.total_item_updates().await;
        assert!(!items.is_empty(), "should have received item-level updates");

        let total_reported: u64 = items.iter().map(|u| u.total_bytes).max().unwrap_or(0);
        assert!(total_reported > 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_download_with_per_file_progress_updaters() {
        let runtime = test_runtime();
        let server = LocalTestServerBuilder::new().start().await;
        let endpoint = make_endpoint(&server);

        let contents: Vec<Vec<u8>> = vec![vec![0xAA; 2048], vec![0xBB; 4096]];
        let policies = vec![Sha256Policy::Compute; contents.len()];

        let file_infos = data_client::upload_bytes_async(
            &runtime,
            contents.clone(),
            policies,
            endpoint.clone(),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let download_dir = TempDir::new().unwrap();
        let updater_a = Arc::new(RecordingUpdater::default());
        let updater_b = Arc::new(RecordingUpdater::default());

        let download_pairs: Vec<(XetFileInfo, String)> = file_infos
            .iter()
            .enumerate()
            .map(|(i, info)| {
                let path = download_dir.path().join(format!("dl_{i}"));
                (info.clone(), path.to_string_lossy().to_string())
            })
            .collect();

        let updaters: Vec<Arc<dyn TrackingProgressUpdater>> =
            vec![updater_a.clone() as Arc<dyn TrackingProgressUpdater>, updater_b.clone()];

        let paths = data_client::download_async(&runtime, download_pairs, endpoint, None, None, Some(updaters), None)
            .await
            .unwrap();

        for (i, path) in paths.iter().enumerate() {
            let downloaded = fs::read(path).unwrap();
            assert_eq!(downloaded, contents[i]);
        }

        let updates_a = updater_a.updates.lock().await;
        let updates_b = updater_b.updates.lock().await;

        assert!(!updates_a.is_empty(), "updater A should have received updates");
        assert!(!updates_b.is_empty(), "updater B should have received updates");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_files_with_progress_updater() {
        let runtime = test_runtime();
        let server = LocalTestServerBuilder::new().start().await;
        let endpoint = make_endpoint(&server);

        let src_dir = TempDir::new().unwrap();
        let file_data: Vec<(&str, Vec<u8>)> = vec![("big_a.bin", vec![0x11; 16384]), ("big_b.bin", vec![0x22; 16384])];

        let mut file_paths = Vec::new();
        let mut policies = Vec::new();
        for (name, data) in &file_data {
            let path = src_dir.path().join(name);
            fs::write(&path, data).unwrap();
            file_paths.push(path.to_string_lossy().to_string());
            policies.push(Sha256Policy::Compute);
        }

        let updater = Arc::new(RecordingUpdater::default());

        let file_infos = data_client::upload_async(
            &runtime,
            file_paths,
            policies,
            endpoint.clone(),
            None,
            None,
            Some(updater.clone()),
            None,
        )
        .await
        .unwrap();

        assert_eq!(file_infos.len(), 2);

        let updates = updater.updates.lock().await;
        assert!(!updates.is_empty(), "should have received progress updates");

        let last = updates.last().unwrap();
        assert_le!(last.total_bytes_completed, last.total_bytes);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_upload_download_large_files() {
        let runtime = test_runtime();
        let server = LocalTestServerBuilder::new().start().await;
        let endpoint = make_endpoint(&server);

        let src_dir = TempDir::new().unwrap();

        let large_data: Vec<u8> = (0..65536u64).map(|i| (i % 251) as u8).collect();
        let path = src_dir.path().join("large.bin");
        fs::write(&path, &large_data).unwrap();

        let file_infos = data_client::upload_async(
            &runtime,
            vec![path.to_string_lossy().to_string()],
            vec![Sha256Policy::Compute],
            endpoint.clone(),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(file_infos.len(), 1);
        assert_eq!(file_infos[0].file_size, Some(large_data.len() as u64));

        let download_dir = TempDir::new().unwrap();
        let out_path = download_dir.path().join("large_out.bin");

        let paths = data_client::download_async(
            &runtime,
            vec![(file_infos[0].clone(), out_path.to_string_lossy().to_string())],
            endpoint,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let downloaded = fs::read(&paths[0]).unwrap();
        assert_eq!(downloaded, large_data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_progress_updates_are_monotonic() {
        let runtime = test_runtime();
        let server = LocalTestServerBuilder::new().start().await;
        let endpoint = make_endpoint(&server);

        let src_dir = TempDir::new().unwrap();
        let data = vec![0xFFu8; 32768];
        let path = src_dir.path().join("monotonic_test.bin");
        fs::write(&path, &data).unwrap();

        let updater = Arc::new(RecordingUpdater::default());

        data_client::upload_async(
            &runtime,
            vec![path.to_string_lossy().to_string()],
            vec![Sha256Policy::Compute],
            endpoint,
            None,
            None,
            Some(updater.clone()),
            None,
        )
        .await
        .unwrap();

        let updates = updater.updates.lock().await;

        let mut prev_completed = 0u64;
        let mut prev_total = 0u64;
        for update in updates.iter() {
            assert!(
                update.total_bytes >= prev_total,
                "total_bytes decreased: {} -> {}",
                prev_total,
                update.total_bytes
            );
            assert!(
                update.total_bytes_completed >= prev_completed,
                "total_bytes_completed decreased: {} -> {}",
                prev_completed,
                update.total_bytes_completed
            );
            assert_le!(update.total_bytes_completed, update.total_bytes);
            prev_total = update.total_bytes;
            prev_completed = update.total_bytes_completed;
        }
    }
}
