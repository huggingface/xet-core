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
use progress_tracking::item_tracking::ItemProgressUpdater;
use progress_tracking::TrackingProgressUpdater;
use tracing::{info_span, instrument, Instrument, Span};
use ulid::Ulid;
use utils::auth::{AuthConfig, TokenRefresher};

use crate::configurations::*;
use crate::constants::{INGESTION_BLOCK_SIZE, MAX_CONCURRENT_DOWNLOADS, MAX_CONCURRENT_FILE_INGESTION};
use crate::errors::DataProcessingError;
use crate::repo_salt::RepoSalt;
use crate::{errors, FileDownloader, FileUploadSession, XetFileInfo};

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
        session_id: Some(Ulid::new().to_string()),
    };

    // Return the temp dir so that it's not dropped and thus the directory deleted.
    Ok(Arc::new(translator_config))
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

    let upload_session = FileUploadSession::new(config, progress_updater).await?;
    let blobs_with_spans = add_spans(file_contents, || info_span!("clean_task"));

    // clean the bytes
    let files = tokio_par_for_each(blobs_with_spans, *MAX_CONCURRENT_FILE_INGESTION, |(blob, span), _| {
        async {
            let (xf, _metrics) = clean_bytes(upload_session.clone(), blob).await?;
            Ok(xf)
        }
        .instrument(span.unwrap_or_else(|| info_span!("unexpected_span")))
    })
    .await
    .map_err(|e| match e {
        ParallelError::JoinError => DataProcessingError::InternalError("Join error".to_string()),
        ParallelError::TaskError(e) => e,
    })?;

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

    let upload_session = FileUploadSession::new(config, progress_updater).await?;

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
    if let Some(updaters) = &progress_updaters {
        if updaters.len() != file_infos.len() {
            return Err(DataProcessingError::ParameterError(
                "updaters are not same length as pointer_files".to_string(),
            ));
        }
    }
    let config =
        default_config(endpoint.unwrap_or(DEFAULT_CAS_ENDPOINT.to_string()), None, token_info, token_refresher)?;
    Span::current().record("session_id", &config.session_id);

    let updaters = match progress_updaters {
        None => vec![None; file_infos.len()],
        Some(updaters) => updaters.into_iter().map(Some).collect(),
    };
    let file_with_progress = file_infos.into_iter().zip(updaters).collect::<Vec<_>>();
    let extended_file_info_list = add_spans(file_with_progress, || info_span!("download_file"));

    let processor = &Arc::new(FileDownloader::new(config).await?);
    let paths = tokio_par_for_each(
        extended_file_info_list,
        *MAX_CONCURRENT_DOWNLOADS,
        |(((file_info, file_path), updater), span), _| {
            async move {
                let proc = processor.clone();
                smudge_file(&proc, &file_info, &file_path, updater).await
            }
            .instrument(span.unwrap_or_else(|| info_span!("unexpected_span")))
        },
    )
    .await
    .map_err(|e| match e {
        ParallelError::JoinError => DataProcessingError::InternalError("Join error".to_string()),
        ParallelError::TaskError(e) => e,
    })?;

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

/// Adds spans to the indicated list for each element.
///
/// Although a span will be added for each element, we need an Option<Span> since
/// tokio_par_for_each requires the input list be Default, which Span isn't.
pub fn add_spans<I, F: Fn() -> Span>(v: Vec<I>, create_span: F) -> Vec<(I, Option<Span>)> {
    let spans: Vec<Option<Span>> = v.iter().map(|_| Some(create_span())).collect();
    v.into_iter().zip(spans).collect()
}

#[cfg(test)]
mod tests {
    use std::env;

    use serial_test::serial;
    use tempfile::tempdir;
    use tracing::info;
    use tracing_test::traced_test;

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
            .unwrap_or(current_dir().unwrap())
            .join(".cache")
            .join("huggingface")
            .join("xet");

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.data_config.cache_config.cache_directory.starts_with(&expected));
    }

    #[tokio::test(flavor = "multi_thread")]
    #[traced_test]
    async fn test_add_spans() {
        let outer_span = info_span!("outer_span");
        async {
            let v = vec!["a", "b", "c"];
            let expected_len = v.len();
            let v_plus = add_spans(v, || info_span!("task_span"));
            assert_eq!(v_plus.len(), expected_len);
            tokio_par_for_each(v_plus, expected_len, |(s, span), i| {
                async move {
                    info!("inside: {s},{i}");
                    Ok::<(), ()>(())
                }
                .instrument(span.unwrap())
            })
            .await
            .unwrap();
        }
        .instrument(outer_span)
        .await;

        assert!(logs_contain("inside: a,0"));
        assert!(logs_contain("inside: b,1"));
        assert!(logs_contain("inside: c,2"));
        logs_assert(|lines: &[&str]| {
            match lines
                .iter()
                .filter(|line| line.contains("task_span") && line.contains("outer_span"))
                .count()
            {
                3 => Ok(()),
                n => Err(format!("Expected 3 lines, got {n}")),
            }
        });
    }
}
