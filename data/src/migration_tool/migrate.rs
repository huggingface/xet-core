use std::sync::Arc;

use anyhow::Result;
use cas_object::CompressionScheme;
use hub_client::{BearerCredentialHelper, HubClient, Operation, RepoInfo};
use mdb_shard::file_structs::MDBFileInfo;
use tracing::{Instrument, Span, info_span, instrument};
use utils::auth::TokenRefresher;
use xet_runtime::XetRuntime;
use xet_runtime::utils::run_constrained;

use super::hub_client_token_refresher::HubClientTokenRefresher;
use crate::data_client::{clean_file, default_config};
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
    let cred_helper = BearerCredentialHelper::new(hub_token.to_owned(), "");
    let hub_client =
        HubClient::new(hub_endpoint, RepoInfo::try_from(repo_type, repo_id)?, None, "xtool", "", cred_helper)?;

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
    let operation = Operation::Upload;
    let jwt_info = hub_client.get_cas_jwt(operation).await?;
    let token_refresher = Arc::new(HubClientTokenRefresher {
        operation,
        client: Arc::new(hub_client),
    }) as Arc<dyn TokenRefresher>;
    let cas = cas_endpoint.unwrap_or(jwt_info.cas_url);

    let config = default_config(cas, compression, Some((jwt_info.access_token, jwt_info.exp)), Some(token_refresher))?;
    Span::current().record("session_id", &config.session_id);

    let num_workers = if sequential {
        1
    } else {
        XetRuntime::current().num_worker_threads()
    };
    let processor = if dry_run {
        FileUploadSession::dry_run(config.into(), None).await?
    } else {
        FileUploadSession::new(config.into(), None).await?
    };

    // let file_paths_with_spans = add_spans(file_paths, || info_span!("migration::clean_file"));
    let clean_futs = file_paths.into_iter().map(|file_path| {
        let proc = processor.clone();
        async move {
            let (pf, metrics) = clean_file(proc, file_path).await?;
            Ok::<(XetFileInfo, u64), DataProcessingError>((pf, metrics.new_bytes))
        }
        .instrument(info_span!("clean_file"))
    });
    let clean_ret = run_constrained(clean_futs, num_workers).await?;

    if dry_run {
        let (metrics, all_file_info) = processor.finalize_with_file_info().await?;
        Ok((all_file_info, clean_ret, metrics.total_bytes_uploaded))
    } else {
        let metrics = processor.finalize().await?;
        Ok((vec![], clean_ret, metrics.total_bytes_uploaded as u64))
    }
}
