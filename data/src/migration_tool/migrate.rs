use std::sync::Arc;

use anyhow::Result;
use cas_client::build_http_client;
use cas_object::CompressionScheme;
use mdb_shard::file_structs::MDBFileInfo;
use parutils::{tokio_par_for_each, ParallelError};
use utils::auth::TokenRefresher;
use xet_threadpool::ThreadPool;

use super::hub_client::{HubClient, HubClientTokenRefresher};
use crate::data_client::{clean_file, default_config};
use crate::errors::DataProcessingError;
use crate::{FileUploadSession, PointerFile};

/// Migrate files to the Hub with external async runtime.
/// How to use:
/// ```no_run
/// let file_paths = vec!["/path/to/file1".to_string(), "/path/to/file2".to_string()];
/// let hub_endpoint = "https://huggingface.co";
/// let hub_token = "your_token";
/// let repo_type = "model";
/// let repo_id = "your_repo_id";
/// let handle = tokio::runtime::Handle::current();
/// migrate_with_external_runtime(file_paths, hub_endpoint, hub_token, repo_type, repo_id, handle)
///     .await?;
/// ```
pub async fn migrate_with_external_runtime(
    file_paths: Vec<String>,
    hub_endpoint: &str,
    hub_token: &str,
    repo_type: &str,
    repo_id: &str,
    handle: tokio::runtime::Handle,
) -> Result<()> {
    let hub_client = HubClient {
        endpoint: hub_endpoint.to_owned(),
        token: hub_token.to_owned(),
        repo_type: repo_type.to_owned(),
        repo_id: repo_id.to_owned(),
        client: build_http_client(&None)?,
    };

    let threadpool = Arc::new(ThreadPool::from_external(handle));

    migrate_files_impl(file_paths, false, hub_client, threadpool, None, false).await?;

    Ok(())
}

pub async fn migrate_files_impl(
    file_paths: Vec<String>,
    sequential: bool,
    hub_client: HubClient,
    threadpool: Arc<ThreadPool>,
    compression: Option<CompressionScheme>,
    dry_run: bool,
) -> Result<(Vec<MDBFileInfo>, Vec<(PointerFile, u64)>, u64)> {
    let token_type = "write";
    let (endpoint, jwt_token, jwt_token_expiry) = hub_client.get_jwt_token(token_type).await?;
    let token_refresher = Arc::new(HubClientTokenRefresher {
        threadpool: threadpool.clone(),
        token_type: token_type.to_owned(),
        client: Arc::new(hub_client),
    }) as Arc<dyn TokenRefresher>;

    let (config, _tempdir) =
        default_config(endpoint, compression, Some((jwt_token, jwt_token_expiry)), Some(token_refresher))?;

    let num_workers = if sequential { 1 } else { threadpool.num_worker_threads() };
    let processor = if dry_run {
        Arc::new(FileUploadSession::dry_run(config, threadpool, None).await?)
    } else {
        Arc::new(FileUploadSession::new(config, threadpool, None).await?)
    };

    let clean_ret = tokio_par_for_each(file_paths, num_workers, |f, _| async {
        let proc = processor.clone();
        clean_file(&proc, f).await
    })
    .await
    .map_err(|e| match e {
        ParallelError::JoinError => DataProcessingError::InternalError("Join error".to_string()),
        ParallelError::TaskError(e) => e,
    })?;

    let total_bytes_trans = processor.finalize_cleaning().await?;

    if dry_run {
        let all_file_info = processor.summarize_file_info_of_session().await?;
        Ok((all_file_info, clean_ret, total_bytes_trans))
    } else {
        Ok((vec![], clean_ret, total_bytes_trans))
    }
}
