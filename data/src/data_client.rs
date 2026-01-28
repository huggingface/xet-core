use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use cas_client::remote_client::PREFIX_DEFAULT;
use cas_object::CompressionScheme;
use deduplication::{Chunker, DeduplicationMetrics};
use file_reconstruction::DataOutput;
use lazy_static::lazy_static;
use mdb_shard::Sha256;
use merklehash::MerkleHash;
use progress_tracking::TrackingProgressUpdater;
use progress_tracking::item_tracking::ItemProgressUpdater;
use tracing::{Instrument, Span, info, info_span, instrument};
use ulid::Ulid;
use utils::auth::{AuthConfig, TokenRefresher};
use xet_runtime::utils::run_constrained_with_semaphore;
use xet_runtime::{GlobalSemaphoreHandle, XetRuntime, global_semaphore_handle, xet_cache_root, xet_config};

use crate::configurations::*;
use crate::errors::DataProcessingError;
use crate::file_upload_session::CONCURRENT_FILE_INGESTION_LIMITER;
use crate::{FileDownloader, FileUploadSession, XetFileInfo, errors};

lazy_static! {
    static ref CONCURRENT_FILE_DOWNLOAD_LIMITER: GlobalSemaphoreHandle =
        global_semaphore_handle!(xet_config().data.max_concurrent_file_downloads as usize);
}

pub fn default_config(
    endpoint: String,
    xorb_compression: Option<CompressionScheme>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
    user_agent: String,
) -> errors::Result<TranslatorConfig> {
    // Intercept local:// to run a simulated CAS server in a specified directory.
    // This is useful for testing and development.
    if endpoint.starts_with("local://") {
        let local_path = endpoint.strip_prefix("local://").unwrap();
        let local_path = PathBuf::from(local_path);
        std::fs::create_dir_all(&local_path)?;
        return TranslatorConfig::local_config(local_path);
    }

    let cache_root_path = xet_cache_root();
    info!("Using cache path {cache_root_path:?}.");

    let (token, token_expiration) = token_info.unzip();
    let auth_cfg = AuthConfig::maybe_new(token, token_expiration, token_refresher);

    // Calculate a fingerprint of the current endpoint to make sure caches stay separated.
    let endpoint_tag = {
        let endpoint_prefix = endpoint
            .chars()
            .take(16)
            .map(|c| if c.is_alphanumeric() { c } else { '_' })
            .collect::<String>();

        // If more gets added
        let endpoint_hash = merklehash::compute_data_hash(endpoint.as_bytes()).base64();

        format!("{endpoint_prefix}-{}", &endpoint_hash[..16])
    };

    let cache_path = cache_root_path.join(endpoint_tag);
    std::fs::create_dir_all(&cache_path)?;

    let staging_root = cache_path.join("staging");
    std::fs::create_dir_all(&staging_root)?;

    let translator_config = TranslatorConfig {
        data_config: DataConfig {
            endpoint: Endpoint::Server(endpoint.clone()),
            compression: xorb_compression,
            auth: auth_cfg.clone(),
            prefix: PREFIX_DEFAULT.into(),
            staging_directory: None,
            user_agent: user_agent.clone(),
        },
        shard_config: ShardConfig {
            prefix: PREFIX_DEFAULT.into(),
            cache_directory: cache_path.join("shard-cache"),
            session_directory: staging_root.join("shard-session"),
            global_dedup_policy: Default::default(),
        },
        repo_info: Some(RepoInfo {
            repo_paths: vec!["".into()],
        }),
        session_id: Some(Ulid::new().to_string()),
        progress_config: ProgressConfig { aggregate: true },
    };

    // Return the temp dir so that it's not dropped and thus the directory deleted.
    Ok(translator_config)
}

#[instrument(skip_all, name = "data_client::upload_bytes", fields(session_id = tracing::field::Empty, num_files=file_contents.len()))]
pub async fn upload_bytes_async(
    file_contents: Vec<Vec<u8>>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
    progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
    user_agent: String,
) -> errors::Result<Vec<XetFileInfo>> {
    let config = default_config(
        endpoint.unwrap_or_else(|| xet_config().data.default_cas_endpoint.clone()),
        None,
        token_info,
        token_refresher,
        user_agent,
    )?;

    Span::current().record("session_id", &config.session_id);

    let semaphore = XetRuntime::current().global_semaphore(*CONCURRENT_FILE_INGESTION_LIMITER);
    let upload_session = FileUploadSession::new(config.into(), progress_updater).await?;
    let clean_futures = file_contents.into_iter().map(|blob| {
        let upload_session = upload_session.clone();
        async move { clean_bytes(upload_session, blob).await.map(|(xf, _metrics)| xf) }
            .instrument(info_span!("clean_task"))
    });
    let files = run_constrained_with_semaphore(clean_futures, semaphore).await?;

    // Push the CAS blocks and flush the mdb to disk
    let _metrics = upload_session.finalize().await?;

    Ok(files)
}

// The sha256, if provided and valid, will be directly used in shard upload to avoid redundant computation.
#[instrument(skip_all, name = "data_client::upload_files",
    fields(session_id = tracing::field::Empty,
    num_files=file_paths.len(),
    new_bytes = tracing::field::Empty,
    deduped_bytes = tracing::field::Empty,
    defrag_prevented_dedup_bytes = tracing::field::Empty,
    new_chunks = tracing::field::Empty,
    deduped_chunks = tracing::field::Empty,
    defrag_prevented_dedup_chunks = tracing::field::Empty
    ))]
pub async fn upload_async(
    file_paths: Vec<String>,
    sha256s: Option<Vec<String>>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
    progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
    user_agent: String,
) -> errors::Result<Vec<XetFileInfo>> {
    // chunk files
    // produce Xorbs + Shards
    // upload shards and xorbs
    // for each file, return the filehash
    let config = default_config(
        endpoint.unwrap_or_else(|| xet_config().data.default_cas_endpoint.clone()),
        None,
        token_info,
        token_refresher,
        user_agent,
    )?;

    let span = Span::current();

    span.record("session_id", &config.session_id);

    let upload_session = FileUploadSession::new(config.into(), progress_updater).await?;

    // Parse sha256 hex string and ignore invalid ones, or if no sha256 is provided,
    // create an iterator of infinite number of "None"s.
    let sha256s: Box<dyn Iterator<Item = Option<Sha256>> + Send> = match &sha256s {
        Some(v) => {
            if v.len() != file_paths.len() {
                return Err(DataProcessingError::ParameterError(
                    "mistached length of the file list and the sha256 list".into(),
                ));
            }
            Box::new(v.iter().map(|s| Sha256::from_hex(s).ok()))
        },
        None => Box::new(std::iter::repeat(None)),
    };

    let files_and_sha256s = file_paths.into_iter().zip(sha256s);

    let ret = upload_session.upload_files(files_and_sha256s).await?;

    // Push the CAS blocks and flush the mdb to disk
    let metrics = upload_session.finalize().await?;

    // Record dedup metrics.
    span.record("new_bytes", metrics.new_bytes);
    span.record("deduped_bytes ", metrics.deduped_bytes);
    span.record("defrag_prevented_dedup_bytes", metrics.defrag_prevented_dedup_bytes);
    span.record("new_chunks", metrics.new_chunks);
    span.record("deduped_chunks", metrics.deduped_chunks);
    span.record("defrag_prevented_dedup_chunks", metrics.defrag_prevented_dedup_chunks);

    Ok(ret)
}

#[instrument(skip_all, name = "data_client::download", fields(session_id = tracing::field::Empty, num_files=file_infos.len()))]
pub async fn download_async(
    file_infos: Vec<(XetFileInfo, String)>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
    progress_updaters: Option<Vec<Arc<dyn TrackingProgressUpdater>>>,
    user_agent: String,
) -> errors::Result<Vec<String>> {
    if let Some(updaters) = &progress_updaters
        && updaters.len() != file_infos.len()
    {
        return Err(DataProcessingError::ParameterError("updaters are not same length as pointer_files".to_string()));
    }
    let config = default_config(
        endpoint.unwrap_or_else(|| xet_config().data.default_cas_endpoint.clone()),
        None,
        token_info,
        token_refresher,
        user_agent,
    )?;
    Span::current().record("session_id", &config.session_id);

    let processor = Arc::new(FileDownloader::new(config.into()).await?);
    let updaters = match progress_updaters {
        None => vec![None; file_infos.len()],
        Some(updaters) => updaters.into_iter().map(Some).collect(),
    };
    let smudge_file_futures = file_infos.into_iter().zip(updaters).map(|((file_info, file_path), updater)| {
        let proc = processor.clone();
        async move { smudge_file(&proc, &file_info, &file_path, updater).await }.instrument(info_span!("download_file"))
    });

    let semaphore = XetRuntime::current().global_semaphore(*CONCURRENT_FILE_DOWNLOAD_LIMITER);

    let paths = run_constrained_with_semaphore(smudge_file_futures, semaphore).await?;

    Ok(paths)
}

#[instrument(skip_all, name = "clean_bytes", fields(bytes.len = bytes.len()))]
pub async fn clean_bytes(
    processor: Arc<FileUploadSession>,
    bytes: Vec<u8>,
) -> errors::Result<(XetFileInfo, DeduplicationMetrics)> {
    let mut handle = processor.start_clean(None, bytes.len() as u64, None).await;
    handle.add_data(&bytes).await?;
    handle.finish().await
}

// The provided sha256, if valid, will be directly used in shard upload to avoid redundant computation.
#[instrument(skip_all, name = "clean_file", fields(file.name = tracing::field::Empty, file.len = tracing::field::Empty))]
pub async fn clean_file(
    processor: Arc<FileUploadSession>,
    filename: impl AsRef<Path>,
    sha256: impl AsRef<str>,
) -> errors::Result<(XetFileInfo, DeduplicationMetrics)> {
    let mut reader = File::open(&filename)?;

    let filesize = reader.metadata()?.len();
    let span = Span::current();
    span.record("file.name", filename.as_ref().to_str());
    span.record("file.len", filesize);
    let mut buffer = vec![0u8; u64::min(filesize, *xet_config().data.ingestion_block_size) as usize];

    let mut handle = processor
        .start_clean(Some(filename.as_ref().to_string_lossy().into()), filesize, Sha256::from_hex(sha256.as_ref()).ok())
        .await;

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
fn hash_single_file(filename: String, buffer_size: usize) -> errors::Result<XetFileInfo> {
    let mut reader = File::open(&filename)?;
    let filesize = reader.metadata()?.len();

    let mut buffer = vec![0u8; buffer_size];
    let mut chunker = Chunker::default();
    let mut chunk_hashes: Vec<(MerkleHash, u64)> = Vec::new();

    loop {
        if XetRuntime::current().in_sigint_shutdown() {
            return Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "Operation interrupted by SIGINT").into());
        }

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

    let file_hash = merklehash::file_hash(&chunk_hashes);
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
/// - Uses `CONCURRENT_FILE_INGESTION_LIMITER` to control parallelism
/// - No authentication or server connection required
/// - Pure local computation
#[instrument(skip_all, name = "data_client::hash_files", fields(num_files=file_paths.len()))]
pub async fn hash_files_async(file_paths: Vec<String>) -> errors::Result<Vec<XetFileInfo>> {
    let semaphore = XetRuntime::current().global_semaphore(*CONCURRENT_FILE_INGESTION_LIMITER);
    let buffer_size = *xet_config().data.ingestion_block_size as usize;

    let hash_futures = file_paths.into_iter().map(|file_path| {
        async move {
            tokio::task::spawn_blocking(move || hash_single_file(file_path, buffer_size))
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?
        }
        .instrument(info_span!("hash_file"))
    });

    let files = run_constrained_with_semaphore(hash_futures, semaphore).await?;

    Ok(files)
}

async fn smudge_file(
    downloader: &FileDownloader,
    file_info: &XetFileInfo,
    file_path: &str,
    progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
) -> errors::Result<String> {
    let path = PathBuf::from(file_path);
    if let Some(parent_dir) = path.parent() {
        std::fs::create_dir_all(parent_dir)?;
    }

    // Wrap the progress updater in the proper tracking struct.
    let progress_updater = progress_updater.map(ItemProgressUpdater::new);

    let output = DataOutput::write_in_file(&path);

    downloader
        .smudge_file_from_hash(&file_info.merkle_hash()?, file_path.into(), output, None, progress_updater)
        .await?;

    Ok(file_path.to_string())
}

#[cfg(test)]
mod tests {
    use dirs::home_dir;
    use serial_test::serial;
    use tempfile::tempdir;
    use utils::EnvVarGuard;

    use super::*;

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_with_hf_home() {
        let temp_dir = tempdir().unwrap();
        let _hf_home_guard = EnvVarGuard::set("HF_HOME", temp_dir.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None, String::new());

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.shard_config.cache_directory.starts_with(&temp_dir.path()));
    }

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_with_hf_xet_cache_and_hf_home() {
        let temp_dir_xet_cache = tempdir().unwrap();
        let temp_dir_hf_home = tempdir().unwrap();

        let hf_xet_cache_guard = EnvVarGuard::set("HF_XET_CACHE", temp_dir_xet_cache.path().to_str().unwrap());
        let hf_home_guard = EnvVarGuard::set("HF_HOME", temp_dir_hf_home.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None, String::new());

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.shard_config.cache_directory.starts_with(&temp_dir_xet_cache.path()));

        drop(hf_xet_cache_guard);
        drop(hf_home_guard);

        let temp_dir = tempdir().unwrap();
        let _hf_home_guard = EnvVarGuard::set("HF_HOME", temp_dir.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None, String::new());

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.shard_config.cache_directory.starts_with(&temp_dir.path()));
    }

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_with_hf_xet_cache() {
        let temp_dir = tempdir().unwrap();
        let _hf_xet_cache_guard = EnvVarGuard::set("HF_XET_CACHE", temp_dir.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None, String::new());

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.shard_config.cache_directory.starts_with(&temp_dir.path()));
    }

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_without_env_vars() {
        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None, String::new());

        let expected = home_dir().unwrap().join(".cache").join("huggingface").join("xet");

        assert!(result.is_ok());
        let config = result.unwrap();
        let test_cache_dir = &config.shard_config.cache_directory;
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
        assert_eq!(file_info.file_size(), 0);
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
        assert_eq!(file_info.file_size(), content.len() as u64);
        assert!(!file_info.hash().is_empty());
    }

    #[tokio::test]
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
        assert_eq!(file_infos[0].file_size(), 18);
        assert_eq!(file_infos[1].file_size(), 19);
        assert_ne!(file_infos[0].hash(), file_infos[1].hash());
    }

    #[tokio::test]
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
        assert_eq!(file_info1.file_size(), file_size as u64);
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
