use std::sync::Arc;

use http::header;
use tracing::{Instrument, Span, info_span, instrument};
use xet_client::cas_client::auth::TokenRefresher;
use xet_client::hub_client::{BearerCredentialHelper, CredentialHelper, HubClient, Operation, RepoInfo};
use xet_core_structures::metadata_shard::file_structs::MDBFileInfo;
use xet_runtime::core::XetContext;
use xet_runtime::core::par_utils::run_constrained;

use super::super::data_client::{clean_file, default_config};
use super::super::{FileUploadSession, Sha256Policy, XetFileInfo};
use super::hub_client_token_refresher::HubClientTokenRefresher;
use crate::error::{DataError, Result};

const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

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
    sha256s: Option<Vec<String>>,
    hub_endpoint: &str,
    cas_endpoint: Option<String>,
    hub_token: &str,
    repo_type: &str,
    repo_id: &str,
) -> Result<()> {
    let cred_helper = BearerCredentialHelper::new(hub_token.to_owned(), "");
    let mut headers = header::HeaderMap::new();
    headers.insert(header::USER_AGENT, header::HeaderValue::from_static(USER_AGENT));
    let runtime = XetContext::default()?;
    let hub_client = HubClient::new(
        runtime.clone(),
        hub_endpoint,
        RepoInfo::try_from(repo_type, repo_id)?,
        Some("main".to_owned()),
        "",
        Some(cred_helper as Arc<dyn CredentialHelper>),
        Some(headers),
    )?;

    migrate_files_impl(file_paths, sha256s, false, hub_client, cas_endpoint, false).await?;

    Ok(())
}

/// mdb file info (if dryrun), cleaned file info, total bytes uploaded
pub type MigrationInfo = (Vec<MDBFileInfo>, Vec<(XetFileInfo, u64)>, u64);

#[instrument(skip_all, name = "migrate_files", fields(session_id = tracing::field::Empty, num_files = file_paths.len()))]
pub async fn migrate_files_impl(
    file_paths: Vec<String>,
    sha256s: Option<Vec<String>>,
    sequential: bool,
    hub_client: HubClient,
    cas_endpoint: Option<String>,
    dry_run: bool,
) -> Result<MigrationInfo> {
    let operation = Operation::Upload;
    let jwt_info = hub_client.get_cas_jwt(operation).await?;
    let token_refresher = Arc::new(HubClientTokenRefresher {
        operation,
        client: Arc::new(hub_client),
    }) as Arc<dyn TokenRefresher>;
    let cas = cas_endpoint.unwrap_or(jwt_info.cas_url);

    // Create headers with USER_AGENT
    let mut headers = http::HeaderMap::new();
    headers.insert(http::header::USER_AGENT, http::HeaderValue::from_static(USER_AGENT));

    let runtime = XetContext::default()?;
    let config = default_config(
        &runtime,
        cas,
        Some((jwt_info.access_token, jwt_info.exp)),
        Some(token_refresher),
        Some(Arc::new(headers)),
    )?;
    Span::current().record("session_id", &config.session.session_id);

    let num_workers = if sequential {
        1
    } else {
        runtime.runtime.num_worker_threads()
    };
    let processor = if dry_run {
        FileUploadSession::dry_run(config.into()).await?
    } else {
        FileUploadSession::new(config.into()).await?
    };

    let sha256_policies: Vec<Sha256Policy> = match sha256s {
        Some(v) => {
            if v.len() != file_paths.len() {
                return Err(DataError::ParameterError(
                    "mismatched length of the file list and the sha256 list".to_string(),
                ));
            }
            v.iter().map(|s| Sha256Policy::from_hex(s)).collect()
        },
        None => vec![Sha256Policy::Compute; file_paths.len()],
    };

    let clean_futs = file_paths.into_iter().zip(sha256_policies).map(|(file_path, policy)| {
        let proc = processor.clone();
        async move {
            let (pf, metrics) = clean_file(proc, file_path, policy).await?;
            Ok::<(XetFileInfo, u64), DataError>((pf, metrics.new_bytes))
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
