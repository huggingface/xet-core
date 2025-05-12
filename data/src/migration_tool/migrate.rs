use std::sync::Arc;

use anyhow::Result;
use cas_object::CompressionScheme;
use mdb_shard::file_structs::MDBFileInfo;
use parutils::{tokio_par_for_each, ParallelError};
use tracing::{info_span, instrument, Instrument, Span};
use utils::auth::TokenRefresher;
use xet_threadpool::ThreadPool;

use super::hub_client::{HubClient, HubClientTokenRefresher};
use crate::data_client::{add_spans, clean_file, default_config};
use crate::errors::DataProcessingError;
use crate::{FileUploadSession, XetFileInfo};

/// Migrate files to the Hub with external async runtime.
/// How to use:
/// ```no_run
/// let file_paths = vec!["/path/to/file1".to_string(), "/path/to/file2".to_string()];
/// let hub_endpoint = "https://huggingface.co";
/// let hub_token = "your_token";
/// let repo_type = "model";
/// let repo_id = "your_repo_id";
/// migrate_with_external_runtime(file_paths, hub_endpoint, hub_token, repo_type, repo_id).await?;
/// ```
pub async fn migrate_with_external_runtime(
    file_paths: Vec<String>,
    hub_endpoint: &str,
    cas_endpoint: Option<String>,
    hub_token: &str,
    repo_type: &str,
    repo_id: &str,
) -> Result<()> {
    let hub_client = HubClient::new(hub_endpoint, hub_token, repo_type, repo_id)?;

    migrate_files_impl(file_paths, false, hub_client, cas_endpoint, None, false).await?;

    Ok(())
}

/// mdb file info (if dryrun), cleaned file info, total bytes uploaded
pub type MigrationInfo = (Vec<MDBFileInfo>, Vec<(XetFileInfo, u64)>, u64);

#[instrument(skip_all, name = "migrate_files", fields(session_id = tracing::field::Empty, num_files = file_paths.len()))]
pub async fn migrate_files_impl(
    file_paths: Vec<String>,
    sequential: bool,
    hub_client: HubClient,
    cas_endpoint: Option<String>,
    compression: Option<CompressionScheme>,
    dry_run: bool,
) -> Result<MigrationInfo> {
    let token_type = "write";
    let (endpoint, jwt_token, jwt_token_expiry) = hub_client.get_jwt_token(token_type).await?;
    let token_refresher = Arc::new(HubClientTokenRefresher {
        token_type: token_type.to_owned(),
        client: Arc::new(hub_client),
    }) as Arc<dyn TokenRefresher>;
    let cas = cas_endpoint.unwrap_or(endpoint);

    let config = default_config(cas, compression, Some((jwt_token, jwt_token_expiry)), Some(token_refresher))?;
    Span::current().record("session_id", &config.session_id);

    let num_workers = if sequential {
        1
    } else {
        ThreadPool::current().num_worker_threads()
    };
    let processor = if dry_run {
        FileUploadSession::dry_run(config, None).await?
    } else {
        FileUploadSession::new(config, None).await?
    };

    let file_paths_with_spans = add_spans(file_paths, || info_span!("migration::clean_file"));

    let clean_ret = tokio_par_for_each(file_paths_with_spans, num_workers, |(f, span), _| {
        async {
            let proc = processor.clone();
            let (pf, metrics) = clean_file(proc, f).await?;
            Ok((pf, metrics.new_bytes))
        }
        .instrument(span.unwrap_or_else(|| info_span!("unexpected_span")))
    })
    .await
    .map_err(|e| match e {
        ParallelError::JoinError => DataProcessingError::InternalError("Join error".to_string()),
        ParallelError::TaskError(e) => e,
    })?;

    if dry_run {
        let (metrics, all_file_info) = processor.finalize_with_file_info().await?;
        Ok((all_file_info, clean_ret, metrics.total_bytes_uploaded))
    } else {
        let metrics = processor.finalize().await?;
        Ok((vec![], clean_ret, metrics.total_bytes_uploaded as u64))
    }
}
