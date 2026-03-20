use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use http::header::HeaderMap;
use tracing::{Instrument, Span, info_span, instrument};
use ulid::Ulid;
use xet_client::cas_client::auth::{AuthConfig, TokenRefresher};
use xet_core_structures::merklehash::MerkleHash;
use xet_runtime::core::par_utils::run_constrained_with_semaphore;
use xet_runtime::core::{XetRuntime, check_sigint_shutdown, xet_config};

use super::configurations::{SessionContext, TranslatorConfig};
use super::file_cleaner::Sha256Policy;
use super::{FileUploadSession, XetFileInfo};
use crate::deduplication::{Chunker, DeduplicationMetrics};
use crate::error::Result;

pub fn default_config(
    endpoint: String,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
    custom_headers: Option<Arc<HeaderMap>>,
) -> Result<TranslatorConfig> {
    let (token, token_expiration) = token_info.unzip();
    let auth_cfg = AuthConfig::maybe_new(token, token_expiration, token_refresher);

    let session = SessionContext {
        endpoint,
        auth: auth_cfg,
        custom_headers,
        repo_paths: vec!["".into()],
        session_id: Some(Ulid::new().to_string()),
    };

    TranslatorConfig::new(session)
}

#[instrument(skip_all, name = "clean_bytes", fields(bytes.len = bytes.len()))]
pub async fn clean_bytes(
    processor: Arc<FileUploadSession>,
    bytes: Vec<u8>,
    sha256_policy: Sha256Policy,
) -> Result<(XetFileInfo, DeduplicationMetrics)> {
    let (_id, mut handle) = processor.start_clean(None, bytes.len() as u64, sha256_policy)?;
    handle.add_data(&bytes).await?;
    handle.finish().await
}

#[instrument(skip_all, name = "clean_file", fields(file.name = tracing::field::Empty, file.len = tracing::field::Empty))]
pub async fn clean_file(
    processor: Arc<FileUploadSession>,
    filename: impl AsRef<Path>,
    sha256_policy: Sha256Policy,
) -> Result<(XetFileInfo, DeduplicationMetrics)> {
    let mut reader = File::open(&filename)?;

    let filesize = reader.metadata()?.len();
    let span = Span::current();
    span.record("file.name", filename.as_ref().to_str());
    span.record("file.len", filesize);
    let mut buffer = vec![0u8; u64::min(filesize, *xet_config().data.ingestion_block_size) as usize];

    let (_id, mut handle) =
        processor.start_clean(Some(filename.as_ref().to_string_lossy().into()), filesize, sha256_policy)?;

    loop {
        let bytes = reader.read(&mut buffer)?;
        if bytes == 0 {
            break;
        }

        handle.add_data(&buffer[0..bytes]).await?;
    }

    handle.finish().await
}

/// Computes the xet hash for a single file without uploading.
///
/// This function performs local-only hash computation by reading the file,
/// chunking it using content-defined chunking, and computing the aggregated
/// hash from the chunk hashes. The resulting hash is identical to what would
/// be returned by upload operations, enabling verification of downloaded files.
///
/// # Arguments
/// * `filename` - Path to the file to hash
/// * `buffer_size` - Size of the read buffer in bytes
///
/// # Returns
/// * `XetFileInfo` containing the hex-encoded hash and file size
///
/// # Errors
/// * `IoError` if the file cannot be opened or read
///
/// # Use Cases
/// - Verify that downloaded files are correctly reassembled
/// - Check if a file needs to be uploaded (by comparing hashes)
/// - Generate cache keys for local file operations
fn hash_single_file(filename: String, buffer_size: usize) -> Result<XetFileInfo> {
    let mut reader = File::open(&filename)?;
    let filesize = reader.metadata()?.len();

    let mut buffer = vec![0u8; buffer_size];
    let mut chunker = Chunker::default();
    let mut chunk_hashes: Vec<(MerkleHash, u64)> = Vec::new();

    loop {
        check_sigint_shutdown()?;

        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        let data = Bytes::copy_from_slice(&buffer[0..bytes_read]);
        let chunks = chunker.next_block_bytes(&data, false);

        for chunk in chunks {
            chunk_hashes.push((chunk.hash, chunk.data.len() as u64));
        }
    }

    // Get the final chunk if any data remains in the chunker
    if let Some(final_chunk) = chunker.finish() {
        chunk_hashes.push((final_chunk.hash, final_chunk.data.len() as u64));
    }

    let file_hash = xet_core_structures::merklehash::file_hash(&chunk_hashes);
    Ok(XetFileInfo::new(file_hash.hex(), filesize))
}

/// Computes xet hashes for multiple files in parallel without uploading.
///
/// This function processes multiple files concurrently using a semaphore to limit
/// parallelism. Each file is hashed independently using `hash_single_file()`.
/// The resulting hashes are identical to those from upload operations,
/// enabling validation and verification of file transfers.
///
/// # Arguments
/// * `file_paths` - Vector of file paths to hash
///
/// # Returns
/// * Vector of `XetFileInfo` in the same order as input file paths
///
/// # Errors
/// * Returns error if any file cannot be read or hashed
///
/// # Use Cases
/// - Verify integrity of downloaded files by comparing computed hashes
/// - Batch validation of multiple files after transfer
/// - Determine which files need to be uploaded by comparing with server hashes
///
/// # Performance
/// - Uses `file_ingestion_semaphore` to control parallelism
/// - No authentication or server connection required
/// - Pure local computation
#[instrument(skip_all, name = "data_client::hash_files", fields(num_files=file_paths.len()))]
pub async fn hash_files_async(file_paths: Vec<String>) -> Result<Vec<XetFileInfo>> {
    let rt = XetRuntime::current();
    let semaphore = rt.common().file_ingestion_semaphore.clone();
    let buffer_size = *xet_config().data.ingestion_block_size as usize;

    let hash_futures = file_paths.into_iter().map(|file_path| {
        let rt = rt.clone();
        async move {
            rt.spawn_blocking(move || hash_single_file(file_path, buffer_size))
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?
        }
        .instrument(info_span!("hash_file"))
    });

    let files = run_constrained_with_semaphore(hash_futures, semaphore).await?;

    Ok(files)
}

#[cfg(test)]
mod tests {
    use dirs::home_dir;
    use serial_test::serial;
    use tempfile::tempdir;
    use xet_runtime::utils::EnvVarGuard;

    use super::*;

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_with_hf_home() {
        let temp_dir = tempdir().unwrap();
        let _hf_home_guard = EnvVarGuard::set("HF_HOME", temp_dir.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None);

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.shard_cache_directory.starts_with(temp_dir.path()));
    }

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_with_hf_xet_cache_and_hf_home() {
        let temp_dir_xet_cache = tempdir().unwrap();
        let temp_dir_hf_home = tempdir().unwrap();

        let hf_xet_cache_guard = EnvVarGuard::set("HF_XET_CACHE", temp_dir_xet_cache.path().to_str().unwrap());
        let hf_home_guard = EnvVarGuard::set("HF_HOME", temp_dir_hf_home.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None);

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.shard_cache_directory.starts_with(temp_dir_xet_cache.path()));

        drop(hf_xet_cache_guard);
        drop(hf_home_guard);

        let temp_dir = tempdir().unwrap();
        let _hf_home_guard = EnvVarGuard::set("HF_HOME", temp_dir.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None);

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.shard_cache_directory.starts_with(temp_dir.path()));
    }

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_with_hf_xet_cache() {
        let temp_dir = tempdir().unwrap();
        let _hf_xet_cache_guard = EnvVarGuard::set("HF_XET_CACHE", temp_dir.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None);

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.shard_cache_directory.starts_with(temp_dir.path()));
    }

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_without_env_vars() {
        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None);

        let expected = home_dir().unwrap().join(".cache").join("huggingface").join("xet");

        assert!(result.is_ok());
        let config = result.unwrap();
        let test_cache_dir = &config.shard_cache_directory;
        assert!(
            test_cache_dir.starts_with(&expected),
            "cache dir = {test_cache_dir:?}; does not start with {expected:?}",
        );
    }

    #[tokio::test]
    async fn test_hash_empty_file() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("empty.txt");
        std::fs::write(&file_path, b"").unwrap();

        let buffer_size = 8 * 1024 * 1024; // 8MB
        let result = hash_single_file(file_path.to_str().unwrap().to_string(), buffer_size);
        assert!(result.is_ok());

        let file_info = result.unwrap();
        assert_eq!(file_info.file_size(), Some(0));
        assert!(!file_info.hash().is_empty());
    }

    #[tokio::test]
    async fn test_hash_small_file() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("small.txt");
        let content = b"Hello, World!";
        std::fs::write(&file_path, content).unwrap();

        let buffer_size = 8 * 1024 * 1024; // 8MB
        let result = hash_single_file(file_path.to_str().unwrap().to_string(), buffer_size);
        assert!(result.is_ok());

        let file_info = result.unwrap();
        assert_eq!(file_info.file_size(), Some(content.len() as u64));
        assert!(!file_info.hash().is_empty());
    }

    #[tokio::test]
    #[cfg_attr(feature = "smoke-test", ignore)]
    async fn test_hash_determinism() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.txt");

        // Create a file that is large enough to span multiple buffer reads
        // Using 20MB to ensure it's larger than typical buffer sizes
        let file_size = 20 * 1024 * 1024;
        let content: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
        std::fs::write(&file_path, &content).unwrap();

        let file_path_str = file_path.to_str().unwrap().to_string();

        // Hash with 8MB buffer size
        let result1 = hash_single_file(file_path_str.clone(), 8 * 1024 * 1024);
        assert!(result1.is_ok());
        let file_info1 = result1.unwrap();

        // Hash with 4MB buffer size
        let result2 = hash_single_file(file_path_str, 4 * 1024 * 1024);
        assert!(result2.is_ok());
        let file_info2 = result2.unwrap();

        // Hashes should be identical regardless of buffer size
        // This verifies that chunker.finish() is called correctly
        assert_eq!(file_info1.hash(), file_info2.hash());
        assert_eq!(file_info1.file_size(), file_info2.file_size());
    }

    #[tokio::test]
    async fn test_hash_file_not_found() {
        let buffer_size = 8 * 1024 * 1024; // 8MB
        let result = hash_single_file("/nonexistent/file.txt".to_string(), buffer_size);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_hash_files_async() {
        let temp_dir = tempdir().unwrap();

        let file1_path = temp_dir.path().join("file1.txt");
        let file2_path = temp_dir.path().join("file2.txt");

        std::fs::write(&file1_path, b"First file content").unwrap();
        std::fs::write(&file2_path, b"Second file content").unwrap();

        let file_paths = vec![
            file1_path.to_str().unwrap().to_string(),
            file2_path.to_str().unwrap().to_string(),
        ];

        let result = hash_files_async(file_paths).await;
        assert!(result.is_ok());

        let file_infos = result.unwrap();
        assert_eq!(file_infos.len(), 2);
        assert_eq!(file_infos[0].file_size(), Some(18));
        assert_eq!(file_infos[1].file_size(), Some(19));
        assert_ne!(file_infos[0].hash(), file_infos[1].hash());
    }

    #[tokio::test]
    #[cfg_attr(feature = "smoke-test", ignore)]
    async fn test_hash_file_size_multiple_of_buffer() {
        // Regression test for bug where final chunk wasn't produced when file size
        // is exactly a multiple of buffer_size. This test verifies that
        // chunker.finish() is called to flush any remaining data.
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("multiple_of_buffer.bin");

        // Create a file that is exactly 16MB
        let file_size = 16 * 1024 * 1024;
        let content: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
        std::fs::write(&file_path, &content).unwrap();

        let file_path_str = file_path.to_str().unwrap().to_string();

        // Hash with 8MB buffer size - file is exactly 2x buffer size
        let result1 = hash_single_file(file_path_str.clone(), 8 * 1024 * 1024);
        assert!(result1.is_ok());
        let file_info1 = result1.unwrap();
        assert_eq!(file_info1.file_size(), Some(file_size as u64));
        assert!(!file_info1.hash().is_empty());

        // Hash with 4MB buffer size - file is exactly 4x buffer size
        let result2 = hash_single_file(file_path_str.clone(), 4 * 1024 * 1024);
        assert!(result2.is_ok());
        let file_info2 = result2.unwrap();

        // Hash with 2MB buffer size - file is exactly 8x buffer size
        let result3 = hash_single_file(file_path_str, 2 * 1024 * 1024);
        assert!(result3.is_ok());
        let file_info3 = result3.unwrap();

        // All hashes should be identical regardless of buffer size
        // This verifies that chunker.finish() is properly called to flush remaining chunks
        // Without finish(), different buffer sizes would produce different (incomplete) hashes
        assert_eq!(file_info1.hash(), file_info2.hash(), "Hash mismatch between 8MB and 4MB buffer sizes");
        assert_eq!(file_info1.hash(), file_info3.hash(), "Hash mismatch between 8MB and 2MB buffer sizes");
        assert_eq!(file_info1.file_size(), file_info2.file_size());
        assert_eq!(file_info1.file_size(), file_info3.file_size());
    }
}
