use std::env;
use std::env::current_dir;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cas_client::remote_client::PREFIX_DEFAULT;
use cas_client::{CacheConfig, FileProvider, OutputProvider, CHUNK_CACHE_SIZE_BYTES};
use cas_object::CompressionScheme;
use deduplication::DeduplicationMetrics;
use dirs::home_dir;
use parutils::{tokio_par_for_each, ParallelError};
use utils::auth::{AuthConfig, TokenRefresher};
use utils::progress::ProgressUpdater;
use xet_threadpool::ThreadPool;

use crate::configurations::*;
use crate::constants::{INGESTION_BLOCK_SIZE, MAX_CONCURRENT_DOWNLOADS, MAX_CONCURRENT_FILE_INGESTION};
use crate::errors::DataProcessingError;
use crate::repo_salt::RepoSalt;
use crate::{errors, FileDownloader, FileUploadSession, PointerFile};

utils::configurable_constants! {
    ref DEFAULT_CAS_ENDPOINT: String = "http://localhost:8080".to_string();
}

pub fn default_config(
    endpoint: String,
    xorb_compression: Option<CompressionScheme>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
) -> errors::Result<Arc<TranslatorConfig>> {
    // if HF_HOME is set use that instead of ~/.cache/huggingface
    // if HF_XET_CACHE is set use that instead of ~/.cache/huggingface/xet
    // HF_XET_CACHE takes precedence over HF_HOME
    let cache_root_path = if env::var("HF_XET_CACHE").is_ok() {
        PathBuf::from(env::var("HF_XET_CACHE").unwrap())
    } else if env::var("HF_HOME").is_ok() {
        let home = env::var("HF_HOME").unwrap();
        PathBuf::from(home).join("xet")
    } else {
        let home = home_dir().unwrap_or(current_dir()?);
        home.join(".cache").join("huggingface").join("xet")
    };

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
            repo_salt: RepoSalt::default(),
        },
        repo_info: Some(RepoInfo {
            repo_paths: vec!["".into()],
        }),
    };

    // Return the temp dir so that it's not dropped and thus the directory deleted.
    Ok(Arc::new(translator_config))
}

pub async fn upload_async(
    threadpool: Arc<ThreadPool>,
    file_paths: Vec<String>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
    progress_updater: Option<Arc<dyn ProgressUpdater>>,
) -> errors::Result<Vec<PointerFile>> {
    // chunk files
    // produce Xorbs + Shards
    // upload shards and xorbs
    // for each file, return the filehash
    let config = default_config(endpoint.unwrap_or(DEFAULT_CAS_ENDPOINT.clone()), None, token_info, token_refresher)?;

    let upload_session = FileUploadSession::new(config, threadpool, progress_updater).await?;

    // for all files, clean them, producing pointer files.
    let pointers = tokio_par_for_each(file_paths, *MAX_CONCURRENT_FILE_INGESTION, |f, _| async {
        let (pf, _metrics) = clean_file(upload_session.clone(), f).await?;
        Ok(pf)
    })
    .await
    .map_err(|e| match e {
        ParallelError::JoinError => DataProcessingError::InternalError("Join error".to_string()),
        ParallelError::TaskError(e) => e,
    })?;

    // Push the CAS blocks and flush the mdb to disk
    let _metrics = upload_session.finalize().await?;

    // TODO: Report on metrics

    Ok(pointers)
}

pub async fn download_async(
    threadpool: Arc<ThreadPool>,
    pointer_files: Vec<PointerFile>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
    progress_updaters: Option<Vec<Arc<dyn ProgressUpdater>>>,
) -> errors::Result<Vec<String>> {
    if let Some(updaters) = &progress_updaters {
        if updaters.len() != pointer_files.len() {
            return Err(DataProcessingError::ParameterError(
                "updaters are not same length as pointer_files".to_string(),
            ));
        }
    }
    let config =
        default_config(endpoint.unwrap_or(DEFAULT_CAS_ENDPOINT.to_string()), None, token_info, token_refresher)?;

    let updaters = match progress_updaters {
        None => vec![None; pointer_files.len()],
        Some(updaters) => updaters.into_iter().map(Some).collect(),
    };
    let pointer_files_plus = pointer_files.into_iter().zip(updaters).collect::<Vec<_>>();

    let processor = &Arc::new(FileDownloader::new(config, threadpool).await?);
    let paths =
        tokio_par_for_each(pointer_files_plus, *MAX_CONCURRENT_DOWNLOADS, |(pointer_file, updater), _| async move {
            let proc = processor.clone();
            smudge_file(&proc, &pointer_file, updater).await
        })
        .await
        .map_err(|e| match e {
            ParallelError::JoinError => DataProcessingError::InternalError("Join error".to_string()),
            ParallelError::TaskError(e) => e,
        })?;

    Ok(paths)
}

pub async fn clean_file(
    processor: Arc<FileUploadSession>,
    filename: impl AsRef<Path>,
) -> errors::Result<(PointerFile, DeduplicationMetrics)> {
    let mut reader = File::open(&filename)?;

    let n = reader.metadata()?.len() as usize;
    let mut buffer = vec![0u8; usize::min(n, *INGESTION_BLOCK_SIZE)];

    let mut handle = processor.start_clean(filename.as_ref().to_string_lossy().into());

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
    pointer_file: &PointerFile,
    progress_updater: Option<Arc<dyn ProgressUpdater>>,
) -> errors::Result<String> {
    let path = PathBuf::from(pointer_file.path());
    if let Some(parent_dir) = path.parent() {
        std::fs::create_dir_all(parent_dir)?;
    }
    let output = OutputProvider::File(FileProvider::new(path));
    downloader
        .smudge_file_from_pointer(pointer_file, &output, None, progress_updater)
        .await?;
    Ok(pointer_file.path().to_string())
}

#[cfg(test)]
mod tests {
    use std::env;

    use serial_test::serial;
    use tempfile::tempdir;

    use super::*;

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_with_hf_home() {
        let temp_dir = tempdir().unwrap();
        env::set_var("HF_HOME", temp_dir.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None);

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.data_config.cache_config.cache_directory.starts_with(&temp_dir.path()));

        env::remove_var("HF_HOME");
    }

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_with_hf_xet_cache_and_hf_home() {
        let temp_dir_xet_cache = tempdir().unwrap();
        let temp_dir_hf_home = tempdir().unwrap();
        env::set_var("HF_XET_CACHE", temp_dir_xet_cache.path().to_str().unwrap());
        env::set_var("HF_HOME", temp_dir_hf_home.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None);

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config
            .data_config
            .cache_config
            .cache_directory
            .starts_with(&temp_dir_xet_cache.path()));

        env::remove_var("HF_XET_CACHE");
        env::remove_var("HF_HOME");

        let temp_dir = tempdir().unwrap();
        env::set_var("HF_HOME", temp_dir.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None);

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.data_config.cache_config.cache_directory.starts_with(&temp_dir.path()));

        env::remove_var("HF_HOME");
    }

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_with_hf_xet_cache() {
        let temp_dir = tempdir().unwrap();
        env::set_var("HF_XET_CACHE", temp_dir.path().to_str().unwrap());

        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None);

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.data_config.cache_config.cache_directory.starts_with(&temp_dir.path()));

        env::remove_var("HF_XET_CACHE");
    }

    #[test]
    #[serial(default_config_env)]
    fn test_default_config_without_env_vars() {
        let endpoint = "http://localhost:8080".to_string();
        let result = default_config(endpoint, None, None, None);

        let expected = home_dir()
            .unwrap_or(std::env::current_dir().unwrap())
            .join(".cache")
            .join("huggingface")
            .join("xet");

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.data_config.cache_config.cache_directory.starts_with(&expected));
    }
}
