#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;
    use std::time::Duration;

    use tempfile::TempDir;
    use xet_client::cas_client::LocalTestServerBuilder;
    use xet_data::processing::configurations::TranslatorConfig;
    use xet_data::processing::{FileDownloadSession, FileUploadSession, Sha256Policy, XetFileInfo};

    async fn upload_bytes(upload_session: &Arc<FileUploadSession>, name: &str, data: &[u8]) -> XetFileInfo {
        let (_id, mut cleaner) = upload_session
            .start_clean(Some(name.into()), data.len() as u64, Sha256Policy::Compute)
            .unwrap();
        cleaner.add_data(data).await.unwrap();
        let (xfi, _metrics) = cleaner.finish().await.unwrap();
        xfi
    }

    fn reassemble(chunks: Vec<(u64, bytes::Bytes)>, expected_len: usize) -> Vec<u8> {
        let mut buf = vec![0u8; expected_len];
        for (offset, data) in chunks {
            buf[offset as usize..offset as usize + data.len()].copy_from_slice(&data);
        }
        buf
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unordered_stream_async_various_sizes() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let test_cases: Vec<(&str, Vec<u8>)> = vec![
            ("one_byte", vec![0x42]),
            ("small", b"hello world".to_vec()),
            ("medium", vec![0xAB; 4096]),
            ("larger", vec![0xCD; 64 * 1024]),
        ];

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();

        for (name, data) in &test_cases {
            let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
            let xfi = upload_bytes(&upload_session, name, data).await;
            upload_session.finalize().await.unwrap();

            let (_id, mut stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();

            let mut chunks = Vec::new();
            while let Some((offset, chunk)) = stream.next().await.unwrap() {
                chunks.push((offset, chunk));
            }

            let assembled = reassemble(chunks, data.len());
            assert_eq!(assembled, *data, "content mismatch for {name}");
            assert!(stream.is_complete());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unordered_stream_blocking_various_sizes() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let test_cases: Vec<(&str, Vec<u8>)> = vec![
            ("one_byte", vec![0x42]),
            ("small", b"hello world".to_vec()),
            ("medium", vec![0xAB; 4096]),
            ("larger", vec![0xCD; 64 * 1024]),
        ];

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();

        for (name, data) in &test_cases {
            let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
            let xfi = upload_bytes(&upload_session, name, data).await;
            upload_session.finalize().await.unwrap();

            let (_id, stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();

            let expected_data = data.clone();
            let collected = tokio::task::spawn_blocking(move || {
                let mut stream = stream;
                let mut chunks = Vec::new();
                while let Some((offset, chunk)) = stream.blocking_next().unwrap() {
                    chunks.push((offset, chunk));
                }
                reassemble(chunks, expected_data.len())
            })
            .await
            .unwrap();

            assert_eq!(collected, *data, "content mismatch for {name}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unordered_stream_reassemble_to_file() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let original_data = vec![0xEF; 64 * 1024];

        let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
        let xfi = upload_bytes(&upload_session, "file_to_disk", &original_data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();
        let (_id, mut stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();

        let mut chunks = Vec::new();
        while let Some((offset, chunk)) = stream.next().await.unwrap() {
            chunks.push((offset, chunk));
        }

        let assembled = reassemble(chunks, original_data.len());
        let out_path = base_dir.path().join("reassembled.bin");
        fs::write(&out_path, &assembled).unwrap();
        assert_eq!(fs::read(&out_path).unwrap(), original_data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unordered_stream_total_bytes_expected() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let original_data = b"total bytes tracking test";

        let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
        let xfi = upload_bytes(&upload_session, "tracking", original_data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();
        let (_id, mut stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();

        assert_eq!(stream.total_bytes_expected(), original_data.len() as u64);

        while stream.next().await.unwrap().is_some() {}

        assert!(stream.is_complete());
        assert_eq!(stream.bytes_completed(), original_data.len() as u64);
        assert_eq!(stream.bytes_in_progress(), 0);
        assert_eq!(stream.terms_in_progress(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unordered_stream_is_complete_loop_drains_all_data() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let original_data: Vec<u8> = (0..131072u32).map(|i| (i % 251) as u8).collect();

        let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
        let xfi = upload_bytes(&upload_session, "is_complete_loop", &original_data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();
        let (_id, mut stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();

        let mut chunks = Vec::new();
        while !stream.is_complete() {
            if let Some((offset, chunk)) = stream.next().await.unwrap() {
                chunks.push((offset, chunk));
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }

        let assembled = reassemble(chunks, original_data.len());
        assert_eq!(assembled, original_data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unordered_stream_next_returns_none_after_complete() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let original_data = b"extra none calls";

        let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
        let xfi = upload_bytes(&upload_session, "none_test", original_data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();
        let (_id, mut stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();

        while stream.next().await.unwrap().is_some() {}

        assert!(stream.next().await.unwrap().is_none());
        assert!(stream.next().await.unwrap().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unordered_stream_cancel_before_start() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let original_data = b"cancel before start";

        let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
        let xfi = upload_bytes(&upload_session, "cancel_pre", original_data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();
        let (_id, mut stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();

        stream.cancel();
        assert!(stream.next().await.unwrap().is_none());
        assert!(stream.next().await.unwrap().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unordered_stream_cancel_after_partial_read() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let original_data = b"cancel after partial read test data";

        let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
        let xfi = upload_bytes(&upload_session, "cancel_mid", original_data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();
        let (_id, mut stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();

        let _ = stream.next().await.unwrap();
        stream.cancel();
        assert!(stream.next().await.unwrap().is_none());

        let (_id2, mut stream2) = download_session.download_unordered_stream(&xfi, None).await.unwrap();
        let mut chunks = Vec::new();
        while let Some((offset, chunk)) = stream2.next().await.unwrap() {
            chunks.push((offset, chunk));
        }
        let assembled = reassemble(chunks, original_data.len());
        assert_eq!(assembled, original_data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unordered_stream_multiple_concurrent() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let data_a = b"Unordered stream A for concurrent download";
        let data_b = b"Unordered stream B for concurrent download - different content";

        let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
        let xfi_a = upload_bytes(&upload_session, "concurrent_a", data_a).await;
        let xfi_b = upload_bytes(&upload_session, "concurrent_b", data_b).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();

        let (_id_a, mut stream_a) = download_session.download_unordered_stream(&xfi_a, None).await.unwrap();
        let (_id_b, mut stream_b) = download_session.download_unordered_stream(&xfi_b, None).await.unwrap();

        let len_a = data_a.len();
        let task_a = tokio::spawn(async move {
            let mut chunks = Vec::new();
            while let Some((offset, chunk)) = stream_a.next().await.unwrap() {
                chunks.push((offset, chunk));
            }
            reassemble(chunks, len_a)
        });

        let len_b = data_b.len();
        let task_b = tokio::spawn(async move {
            let mut chunks = Vec::new();
            while let Some((offset, chunk)) = stream_b.next().await.unwrap() {
                chunks.push((offset, chunk));
            }
            reassemble(chunks, len_b)
        });

        let result_a = task_a.await.unwrap();
        let result_b = task_b.await.unwrap();

        assert_eq!(result_a, data_a);
        assert_eq!(result_b, data_b);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unordered_stream_drop_without_reading() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let original_data = b"drop without reading cleanup test";

        let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
        let xfi = upload_bytes(&upload_session, "drop_test", original_data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();

        let (_id, stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();
        drop(stream);
        tokio::task::yield_now().await;

        let out_path = base_dir.path().join("after_drop.txt");
        let (_id2, n_bytes) = download_session.download_file(&xfi, &out_path).await.unwrap();
        assert_eq!(n_bytes, original_data.len() as u64);
        assert_eq!(fs::read(&out_path).unwrap(), original_data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unordered_stream_matches_sequential_download() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let original_data: Vec<u8> = (0..=255u8).cycle().take(32 * 1024).collect();

        let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
        let xfi = upload_bytes(&upload_session, "compare_test", &original_data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();

        let out_path = base_dir.path().join("sequential.bin");
        let (_id, _) = download_session.download_file(&xfi, &out_path).await.unwrap();
        let sequential_result = fs::read(&out_path).unwrap();

        let (_id2, mut stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();
        let mut chunks = Vec::new();
        while let Some((offset, chunk)) = stream.next().await.unwrap() {
            chunks.push((offset, chunk));
        }
        let unordered_result = reassemble(chunks, original_data.len());

        assert_eq!(sequential_result, unordered_result);
        assert_eq!(sequential_result, original_data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stress_test_repeated_blocking_downloads() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let original_data: Vec<u8> = (0..65536u32).map(|i| (i % 251) as u8).collect();

        let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
        let xfi = upload_bytes(&upload_session, "stress_blocking", &original_data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();

        for _ in 0..30 {
            let (_id, stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();
            let expected_len = original_data.len();
            let result = tokio::task::spawn_blocking(move || {
                let mut stream = stream;
                let mut chunks = Vec::new();
                while let Some((offset, chunk)) = stream.blocking_next().unwrap() {
                    chunks.push((offset, chunk));
                }
                reassemble(chunks, expected_len)
            })
            .await
            .unwrap();
            assert_eq!(result, original_data);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stress_test_repeated_async_downloads() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let original_data: Vec<u8> = (0..65536u32).map(|i| (i % 251) as u8).collect();

        let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
        let xfi = upload_bytes(&upload_session, "stress_async", &original_data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();

        for _ in 0..30 {
            let (_id, mut stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();
            let mut chunks = Vec::new();
            while let Some((offset, chunk)) = stream.next().await.unwrap() {
                chunks.push((offset, chunk));
            }
            let result = reassemble(chunks, original_data.len());
            assert_eq!(result, original_data);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stress_test_concurrent_blocking_downloads() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let original_data: Vec<u8> = (0..65536u32).map(|i| (i % 251) as u8).collect();

        let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
        let xfi = upload_bytes(&upload_session, "stress_concurrent", &original_data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();

        let mut handles = Vec::new();
        for _ in 0..8 {
            let (_id, stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();
            let expected_len = original_data.len();
            handles.push(tokio::task::spawn_blocking(move || {
                let mut stream = stream;
                let mut chunks = Vec::new();
                while let Some((offset, chunk)) = stream.blocking_next().unwrap() {
                    chunks.push((offset, chunk));
                }
                reassemble(chunks, expected_len)
            }));
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert_eq!(result, original_data);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stress_test_large_file_download() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let original_data: Vec<u8> = (0..262144u32)
            .map(|i| ((i.wrapping_mul(7919) ^ (i >> 3)) % 256) as u8)
            .collect();

        let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
        let xfi = upload_bytes(&upload_session, "stress_large", &original_data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();

        {
            let (_id, mut stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();
            let mut chunks = Vec::new();
            while let Some((offset, chunk)) = stream.next().await.unwrap() {
                chunks.push((offset, chunk));
            }
            assert_eq!(reassemble(chunks, original_data.len()), original_data);
        }

        {
            let (_id, stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();
            let expected_len = original_data.len();
            let expected_data = original_data.clone();
            let result = tokio::task::spawn_blocking(move || {
                let mut stream = stream;
                let mut chunks = Vec::new();
                while let Some((offset, chunk)) = stream.blocking_next().unwrap() {
                    chunks.push((offset, chunk));
                }
                reassemble(chunks, expected_len)
            })
            .await
            .unwrap();
            assert_eq!(result, expected_data);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stress_test_mixed_concurrent_async_and_blocking() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let original_data: Vec<u8> = (0..65536u32).map(|i| (i % 251) as u8).collect();

        let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
        let xfi = upload_bytes(&upload_session, "stress_mixed", &original_data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();

        let mut handles: Vec<tokio::task::JoinHandle<Vec<u8>>> = Vec::new();

        for i in 0..8 {
            if i % 2 == 0 {
                let (_id, mut stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();
                let expected_len = original_data.len();
                handles.push(tokio::spawn(async move {
                    let mut chunks = Vec::new();
                    while let Some((offset, chunk)) = stream.next().await.unwrap() {
                        chunks.push((offset, chunk));
                    }
                    reassemble(chunks, expected_len)
                }));
            } else {
                let (_id, stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();
                let expected_len = original_data.len();
                handles.push(tokio::task::spawn_blocking(move || {
                    let mut stream = stream;
                    let mut chunks = Vec::new();
                    while let Some((offset, chunk)) = stream.blocking_next().unwrap() {
                        chunks.push((offset, chunk));
                    }
                    reassemble(chunks, expected_len)
                }));
            }
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert_eq!(result, original_data);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stress_test_rapid_create_and_drop() {
        let server = LocalTestServerBuilder::new().start().await;
        let base_dir = TempDir::new().unwrap();
        let config = Arc::new(TranslatorConfig::test_server_config(server.http_endpoint(), base_dir.path()).unwrap());

        let original_data: Vec<u8> = (0..32768u32).map(|i| (i % 199) as u8).collect();

        let upload_session = FileUploadSession::new(config.clone()).await.unwrap();
        let xfi = upload_bytes(&upload_session, "stress_drop", &original_data).await;
        upload_session.finalize().await.unwrap();

        let download_session = FileDownloadSession::new(config.clone()).await.unwrap();

        for _ in 0..20 {
            let (_id, mut stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();
            let _ = stream.next().await;
            drop(stream);
        }

        let (_id, mut stream) = download_session.download_unordered_stream(&xfi, None).await.unwrap();
        let mut chunks = Vec::new();
        while let Some((offset, chunk)) = stream.next().await.unwrap() {
            chunks.push((offset, chunk));
        }
        assert_eq!(reassemble(chunks, original_data.len()), original_data);
    }
}
