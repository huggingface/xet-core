use std::env;
use std::env::current_dir;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cas_client::remote_client::PREFIX_DEFAULT;
use cas_client::{CHUNK_CACHE_SIZE_BYTES, CacheConfig, FileProvider, OutputProvider};
use cas_object::CompressionScheme;
use deduplication::DeduplicationMetrics;
use dirs::home_dir;
use progress_tracking::TrackingProgressUpdater;
use progress_tracking::item_tracking::ItemProgressUpdater;
use tracing::{Instrument, Span, info, info_span, instrument};
use ulid::Ulid;
use utils::auth::{AuthConfig, TokenRefresher};
use utils::normalized_path_from_user_string;
use xet_runtime::utils::run_constrained_with_semaphore;
use xet_runtime::{GlobalSemaphoreHandle, XetRuntime, global_semaphore_handle};

use crate::configurations::*;
use crate::constants::{INGESTION_BLOCK_SIZE, MAX_CONCURRENT_DOWNLOADS};
use crate::errors::DataProcessingError;
use crate::file_upload_session::CONCURRENT_FILE_INGESTION_LIMITER;
use crate::{FileDownloader, FileUploadSession, XetFileInfo, errors};

utils::configurable_constants! {
    ref DEFAULT_CAS_ENDPOINT: String = "http://localhost:8080".to_string();
}

pub fn default_config(
    endpoint: String,
    xorb_compression: Option<CompressionScheme>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
) -> errors::Result<TranslatorConfig> {
    // if HF_HOME is set use that instead of ~/.cache/huggingface
    // if HF_XET_CACHE is set use that instead of ~/.cache/huggingface/xet
    // HF_XET_CACHE takes precedence over HF_HOME
    let cache_root_path = {
        // If HF_XET_CACHE is set, use that directly.
        if let Ok(cache) = env::var("HF_XET_CACHE") {
            normalized_path_from_user_string(cache)

        // If HF_HOME is set, use the $HF_HOME/xet
        } else if let Ok(hf_home) = env::var("HF_HOME") {
            normalized_path_from_user_string(hf_home).join("xet")

        // If XDG_CACHE_HOME is set, use the $XDG_CACHE_HOME/huggingface/xet, otherwise
        // use $HOME/.cache/huggingface/xet
        } else if let Ok(xdg_cache_home) = env::var("XDG_CACHE_HOME") {
            normalized_path_from_user_string(xdg_cache_home).join("huggingface").join("xet")

        // Use the same default as huggingface_hub, ~/.cache/huggingface/xet (slightly nonstandard, but won't
        // mess with it).
        } else {
            home_dir()
                .unwrap_or(current_dir()?)
                .join(".cache")
                .join("huggingface")
                .join("xet")
        }
    };

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
            cache_config: CacheConfig {
                cache_directory: cache_path.join("chunk-cache"),
                cache_size: *CHUNK_CACHE_SIZE_BYTES,
            },
            staging_directory: None,
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
) -> errors::Result<Vec<XetFileInfo>> {
    let config = default_config(endpoint.unwrap_or(DEFAULT_CAS_ENDPOINT.clone()), None, token_info, token_refresher)?;
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
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
    progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
) -> errors::Result<Vec<XetFileInfo>> {
    // chunk files
    // produce Xorbs + Shards
    // upload shards and xorbs
    // for each file, return the filehash
    let config = default_config(endpoint.unwrap_or(DEFAULT_CAS_ENDPOINT.clone()), None, token_info, token_refresher)?;

    let span = Span::current();

    span.record("session_id", &config.session_id);

    let upload_session = FileUploadSession::new(config.into(), progress_updater).await?;

    let ret = upload_session.upload_files(&file_paths).await?;

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
) -> errors::Result<Vec<String>> {
    lazy_static! {
        static ref CONCURRENT_FILE_DOWNLOAD_LIMITER: GlobalSemaphoreHandle =
            global_semaphore_handle!(*MAX_CONCURRENT_DOWNLOADS);
    }

    if let Some(updaters) = &progress_updaters
        && updaters.len() != file_infos.len()
    {
        return Err(DataProcessingError::ParameterError("updaters are not same length as pointer_files".to_string()));
    }
    let config =
        default_config(endpoint.unwrap_or(DEFAULT_CAS_ENDPOINT.to_string()), None, token_info, token_refresher)?;
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
    let mut handle = processor.start_clean(None, bytes.len() as u64).await;
    handle.add_data(&bytes).await?;
    handle.finish().await
}

#[instrument(skip_all, name = "clean_file", fields(file.name = tracing::field::Empty, file.len = tracing::field::Empty))]
pub async fn clean_file(
    processor: Arc<FileUploadSession>,
    filename: impl AsRef<Path>,
) -> errors::Result<(XetFileInfo, DeduplicationMetrics)> {
    let mut reader = File::open(&filename)?;

    let n = reader.metadata()?.len();
    let span = Span::current();
    span.record("file.name", filename.as_ref().to_str());
    span.record("file.len", n);
    let mut buffer = vec![0u8; u64::min(n, *INGESTION_BLOCK_SIZE as u64) as usize];

    let mut handle = processor.start_clean(Some(filename.as_ref().to_string_lossy().into()), n).await;

    loop {
        let bytes = reader.read(&mut buffer)?;
        if bytes == 0 {
            break;
        }

        handle.add_data(&buffer[0..bytes]).await?;
    }

    handle.finish().await
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
    let output = OutputProvider::File(FileProvider::new(path));

    // Wrap the progress updater in the proper tracking struct.
    let progress_updater = progress_updater.map(ItemProgressUpdater::new);

    downloader
        .smudge_file_from_hash(&file_info.merkle_hash()?, file_path.into(), &output, None, progress_updater)
        .await?;
    Ok(file_path.to_string())
}

#[cfg(test)]
mod tests {
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
        let result = default_config(endpoint, None, None, None);

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.data_config.cache_config.cache_directory.starts_with(&temp_dir.path()));
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
        assert!(
            config
                .data_config
                .cache_config
                .cache_directory
                .starts_with(&temp_dir_xet_cache.path())
        );

        drop(hf_xet_cache_guard);
        drop(hf_home_guard);

        let temp_dir = tempdir().unwrap();
        let _hf_home_guard = EnvVarGuard::set("HF_HOME", temp_dir.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None);

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.data_config.cache_config.cache_directory.starts_with(&temp_dir.path()));
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
        assert!(config.data_config.cache_config.cache_directory.starts_with(&temp_dir.path()));
    }

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_without_env_vars() {
        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None);

        let expected = home_dir().unwrap().join(".cache").join("huggingface").join("xet");

        assert!(result.is_ok());
        let config = result.unwrap();
        let test_cache_dir = &config.data_config.cache_config.cache_directory;
        assert!(
            test_cache_dir.starts_with(&expected),
            "cache dir = {test_cache_dir:?}; does not start with {expected:?}",
        );
    }
}
