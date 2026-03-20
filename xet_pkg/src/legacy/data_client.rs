use std::path::PathBuf;
use std::sync::Arc;

use http::header::HeaderMap;
use tracing::{Instrument, Span, info_span, instrument};
use xet_client::cas_client::auth::TokenRefresher;
pub use xet_data::processing::data_client::hash_files_async;
use xet_data::processing::data_client::{clean_bytes, default_config};
use xet_data::processing::{FileDownloadSession, FileUploadSession, Sha256Policy, XetFileInfo};
use xet_data::{DataError, Result};
use xet_runtime::core::par_utils::run_constrained_with_semaphore;
use xet_runtime::core::{XetRuntime, xet_config};

use super::progress_tracking::{GroupProgressCallbackUpdater, ItemProgressCallbackUpdater, TrackingProgressUpdater};

#[instrument(skip_all, name = "data_client::upload_bytes", fields(session_id = tracing::field::Empty, num_files=file_contents.len()))]
pub async fn upload_bytes_async(
    file_contents: Vec<Vec<u8>>,
    sha256_policies: Vec<Sha256Policy>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
    progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
    custom_headers: Option<Arc<HeaderMap>>,
) -> Result<Vec<XetFileInfo>> {
    if sha256_policies.len() != file_contents.len() {
        return Err(DataError::ParameterError(format!(
            "sha256_policies length ({}) must match file_contents length ({})",
            sha256_policies.len(),
            file_contents.len()
        )));
    }

    let config = default_config(
        endpoint.unwrap_or_else(|| xet_config().data.default_cas_endpoint.clone()),
        token_info,
        token_refresher,
        custom_headers,
    )?;

    Span::current().record("session_id", &config.session.session_id);

    let semaphore = XetRuntime::current().common().file_ingestion_semaphore.clone();
    let upload_session = FileUploadSession::new(config.into()).await?;

    let bridge = progress_updater.map(|updater| GroupProgressCallbackUpdater::start(upload_session.clone(), updater));

    let clean_futures = file_contents.into_iter().zip(sha256_policies).map(|(blob, policy)| {
        let upload_session = upload_session.clone();
        async move { clean_bytes(upload_session, blob, policy).await.map(|(xf, _metrics)| xf) }
            .instrument(info_span!("clean_task"))
    });
    let files = run_constrained_with_semaphore(clean_futures, semaphore).await?;

    let _metrics = upload_session.finalize().await?;

    if let Some(bridge) = bridge {
        bridge.finalize().await;
    }

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
    sha256_policies: Vec<Sha256Policy>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
    progress_updater: Option<Arc<dyn TrackingProgressUpdater>>,
    custom_headers: Option<Arc<HeaderMap>>,
) -> Result<Vec<XetFileInfo>> {
    if sha256_policies.len() != file_paths.len() {
        return Err(DataError::ParameterError(format!(
            "sha256_policies length ({}) must match file_paths length ({})",
            sha256_policies.len(),
            file_paths.len()
        )));
    }

    let config = default_config(
        endpoint.unwrap_or_else(|| xet_config().data.default_cas_endpoint.clone()),
        token_info,
        token_refresher,
        custom_headers,
    )?;

    let span = Span::current();

    span.record("session_id", &config.session.session_id);

    let upload_session = FileUploadSession::new(config.into()).await?;

    let bridge = progress_updater.map(|updater| GroupProgressCallbackUpdater::start(upload_session.clone(), updater));

    let files_and_sha256 = file_paths.into_iter().zip(sha256_policies.into_iter());

    let ret = upload_session.upload_files(files_and_sha256).await?;

    let metrics = upload_session.finalize().await?;

    if let Some(bridge) = bridge {
        bridge.finalize().await;
    }

    span.record("new_bytes", metrics.new_bytes);
    span.record("deduped_bytes", metrics.deduped_bytes);
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
    custom_headers: Option<Arc<HeaderMap>>,
) -> Result<Vec<String>> {
    if let Some(updaters) = &progress_updaters
        && updaters.len() != file_infos.len()
    {
        return Err(DataError::ParameterError("updaters are not same length as pointer_files".to_string()));
    }
    let config: Arc<_> = default_config(
        endpoint.unwrap_or_else(|| xet_config().data.default_cas_endpoint.clone()),
        token_info,
        token_refresher,
        custom_headers,
    )?
    .into();

    Span::current().record("session_id", &config.session.session_id);

    let updaters: Vec<Option<Arc<dyn TrackingProgressUpdater>>> = match progress_updaters {
        None => vec![None; file_infos.len()],
        Some(updaters) => updaters.into_iter().map(Some).collect(),
    };

    let session = FileDownloadSession::new(config).await?;

    let mut tasks = Vec::with_capacity(file_infos.len());
    let mut bridges: Vec<Option<ItemProgressCallbackUpdater>> = Vec::with_capacity(file_infos.len());

    for ((file_info, file_path), updater) in file_infos.into_iter().zip(updaters) {
        let path = PathBuf::from(&file_path);
        let (id, handle) = session.download_file_background(file_info, path).await?;

        let bridge = updater.map(|u| ItemProgressCallbackUpdater::start(session.clone(), id, u));

        tasks.push((file_path, handle));
        bridges.push(bridge);
    }

    let mut paths = Vec::with_capacity(tasks.len());
    for ((file_path, handle), bridge) in tasks.into_iter().zip(bridges) {
        handle.await??;

        if let Some(bridge) = bridge {
            bridge.finalize().await;
        }

        paths.push(file_path);
    }

    Ok(paths)
}
