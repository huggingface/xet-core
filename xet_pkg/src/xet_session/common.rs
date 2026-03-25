use std::sync::Arc;

use http::HeaderMap;
use xet_client::cas_client::auth::{DirectRefreshRouteTokenRefresher, TokenRefresher};
use xet_client::common::http_client::build_http_client;
use xet_data::processing::configurations::TranslatorConfig;

use super::XetSession;
use crate::error::XetError;

/// Builds a [`TranslatorConfig`] from the session's endpoint and shared settings,
/// combined with the per-commit/group token credentials supplied by the caller.
///
/// `token_info` is an optional pre-seeded `(token, expiry_unix_secs)` pair that
/// lets the first CAS request use an already-known token instead of fetching one.
///
/// `token_refresh` is an optional `(refresh_url, request_headers)` pair.  When
/// present, an HTTP client is built with those headers and wrapped in a
/// [`DirectRefreshRouteTokenRefresher`] so the commit/group can fetch a fresh CAS
/// token whenever the current one is about to expire.
pub(super) fn create_translator_config(
    session: &XetSession,
    token_info: Option<(String, u64)>,
    token_refresh: Option<&(String, Arc<HeaderMap>)>,
) -> Result<TranslatorConfig, XetError> {
    let token_refresher: Option<Arc<dyn TokenRefresher>> = token_refresh
        .map(|(url, headers)| -> Result<Arc<dyn TokenRefresher>, XetError> {
            let session_id = session.id.to_string();
            let client = build_http_client(&session_id, None, Some(headers.clone()))?;
            Ok(Arc::new(DirectRefreshRouteTokenRefresher::new(url, client, None)))
        })
        .transpose()?;

    let endpoint = session
        .endpoint
        .clone()
        .unwrap_or_else(|| session.config.data.default_cas_endpoint.clone());

    let mut config = xet_data::processing::data_client::default_config(
        endpoint,
        token_info,
        token_refresher,
        session.custom_headers.clone(),
    )?;

    let session_id = session.id.to_string();
    if !session_id.is_empty() {
        config.session.session_id = Some(session_id);
    }

    Ok(config)
}

/// State of the upload commit and download group
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum GroupState {
    Alive,
    Finished,
    Aborted,
}
