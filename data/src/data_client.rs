use std::env::current_dir;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::num::NonZero;
use std::path::PathBuf;
use std::sync::Arc;

use cas_client::CacheConfig;
use cas_object::CompressionScheme;
use dirs::home_dir;
use lazy_static::lazy_static;
use merkledb::constants::IDEAL_CAS_BLOCK_SIZE;
use parutils::{tokio_par_for_each, ParallelError};
use tempfile::{tempdir_in, TempDir};
use utils::auth::{AuthConfig, TokenRefresher};
use utils::progress::ProgressUpdater;
use xet_threadpool::ThreadPool;

use crate::configurations::*;
use crate::errors::DataProcessingError;
use crate::{errors, PointerFile, PointerFileTranslator};

// Concurrency in number of files
lazy_static! {
    // Upload may be CPU-bound, this depends on network bandwidth and CPU speed
    static ref MAX_CONCURRENT_UPLOADS: usize =
        std::thread::available_parallelism().unwrap_or(NonZero::new(8).unwrap()).get();
}
const MAX_CONCURRENT_DOWNLOADS: usize = 8; // Download is not CPU-bound

const DEFAULT_CAS_ENDPOINT: &str = "http://localhost:8080";
const READ_BLOCK_SIZE: usize = 1024 * 1024;
const DEFAULT_XORB_COMPRESSION: CompressionScheme = CompressionScheme::LZ4;

pub fn xorb_compression_for_repo_type(repo_type: &str) -> CompressionScheme {
    match repo_type {
        "model" | "models" => CompressionScheme::ByteGrouping4LZ4,
        _ => DEFAULT_XORB_COMPRESSION,
    }
}

pub fn default_config(
    endpoint: String,
    xorb_compression: Option<CompressionScheme>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
) -> errors::Result<(TranslatorConfig, TempDir)> {
    let home = home_dir().unwrap_or(current_dir()?);

    let cache_root_path = home.join(".cache").join("huggingface").join("xet");

    let staging_root = cache_root_path.join("staging");
    std::fs::create_dir_all(&staging_root)?;
    let shard_staging_directory = tempdir_in(staging_root)?;

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

    let translator_config = TranslatorConfig {
        file_query_policy: FileQueryPolicy::ServerOnly,
        cas_storage_config: StorageConfig {
            endpoint: Endpoint::Server(endpoint.clone()),
            compression: xorb_compression.unwrap_or(DEFAULT_XORB_COMPRESSION),
            auth: auth_cfg.clone(),
            prefix: "default".into(),
            cache_config: Some(CacheConfig {
                cache_directory: cache_path.join("chunk-cache"),
                cache_size: 10 * 1024 * 1024 * 1024, // 10 GiB
            }),
            staging_directory: None,
        },
        shard_storage_config: StorageConfig {
            endpoint: Endpoint::Server(endpoint),
            compression: CompressionScheme::None,
            auth: auth_cfg,
            prefix: "default-merkledb".into(),
            cache_config: Some(CacheConfig {
                cache_directory: cache_path.join("shard-cache"),
                cache_size: 0, // ignored
            }),
            staging_directory: Some(shard_staging_directory.path().to_owned()),
        },
        dedup_config: Some(DedupConfig {
            repo_salt: None,
            global_dedup_policy: Default::default(),
        }),
        repo_info: Some(RepoInfo {
            repo_paths: vec!["".into()],
        }),
    };

    translator_config.validate()?;

    // Return the temp dir so that it's not dropped and thus the directory deleted.
    Ok((translator_config, shard_staging_directory))
}

pub async fn upload_async(
    threadpool: Arc<ThreadPool>,
    file_paths: Vec<String>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
    progress_updater: Option<Arc<dyn ProgressUpdater>>,
    repo_type: String,
) -> errors::Result<Vec<PointerFile>> {
    // chunk files
    // produce Xorbs + Shards
    // upload shards and xorbs
    // for each file, return the filehash
    let (config, _tempdir) = default_config(
        endpoint.unwrap_or(DEFAULT_CAS_ENDPOINT.to_string()),
        xorb_compression_for_repo_type(&repo_type).into(),
        token_info,
        token_refresher,
    )?;

    let processor = Arc::new(PointerFileTranslator::new(config, threadpool, progress_updater, false).await?);

    // for all files, clean them, producing pointer files.
    let pointers = tokio_par_for_each(file_paths, *MAX_CONCURRENT_UPLOADS, |f, _| async {
        let proc = processor.clone();
        clean_file(&proc, f).await
    })
    .await
    .map_err(|e| match e {
        ParallelError::JoinError => DataProcessingError::InternalError("Join error".to_string()),
        ParallelError::TaskError(e) => e,
    })?;

    // Push the CAS blocks and flush the mdb to disk
    processor.finalize_cleaning().await?;

    Ok(pointers.into_iter().map(|(pt, _)| pt).collect())
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
    let (config, _tempdir) =
        default_config(endpoint.unwrap_or(DEFAULT_CAS_ENDPOINT.to_string()), None, token_info, token_refresher)?;

    let updaters = match progress_updaters {
        None => vec![None; pointer_files.len()],
        Some(updaters) => updaters.into_iter().map(Some).collect(),
    };
    let pointer_files_plus = pointer_files.into_iter().zip(updaters).collect::<Vec<_>>();

    let processor = &Arc::new(PointerFileTranslator::new(config, threadpool, None, true).await?);
    let paths =
        tokio_par_for_each(pointer_files_plus, MAX_CONCURRENT_DOWNLOADS, |(pointer_file, updater), _| async move {
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

pub async fn clean_file(processor: &PointerFileTranslator, f: String) -> errors::Result<(PointerFile, u64)> {
    let mut read_buf = vec![0u8; READ_BLOCK_SIZE];
    let path = PathBuf::from(f);
    let mut reader = BufReader::new(File::open(path.clone())?);
    let handle = processor
        .start_clean(
            IDEAL_CAS_BLOCK_SIZE / READ_BLOCK_SIZE, // enough to fill one CAS block
            Some(&path),                            // for logging & telemetry
        )
        .await?;

    loop {
        let bytes = reader.read(&mut read_buf)?;
        if bytes == 0 {
            break;
        }

        handle.add_bytes(read_buf[0..bytes].to_vec()).await?;
    }

    let (pf_str, new_bytes) = handle.result().await?;
    let pf = PointerFile::init_from_string(&pf_str, path.to_str().unwrap());
    Ok((pf, new_bytes))
}

async fn smudge_file(
    proc: &PointerFileTranslator,
    pointer_file: &PointerFile,
    progress_updater: Option<Arc<dyn ProgressUpdater>>,
) -> errors::Result<String> {
    let path = PathBuf::from(pointer_file.path());
    if let Some(parent_dir) = path.parent() {
        fs::create_dir_all(parent_dir)?;
    }
    let mut f: Box<dyn Write + Send> = Box::new(File::create(&path)?);
    proc.smudge_file_from_pointer(pointer_file, &mut f, None, progress_updater)
        .await?;
    Ok(pointer_file.path().to_string())
}
