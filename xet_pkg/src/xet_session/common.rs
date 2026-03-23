use std::sync::Arc;

use xet_client::cas_client::auth::{DirectRefreshRouteTokenRefresher, TokenRefresher};
use xet_client::common::http_client::build_http_client;
use xet_data::processing::configurations::TranslatorConfig;

use super::XetSession;
use crate::error::XetError;

// Helper function to create TranslatorConfig
pub(super) fn create_translator_config(session: &XetSession) -> Result<TranslatorConfig, XetError> {
    let endpoint = session
        .endpoint
        .clone()
        .unwrap_or_else(|| session.config.data.default_cas_endpoint.clone());

    let session_id = session.id.to_string();

    let token_refresher: Option<Arc<dyn TokenRefresher>> = session
        .token_refresh
        .as_ref()
        .map(|(url, headers)| -> Result<Arc<dyn TokenRefresher>, XetError> {
            let client = build_http_client(&session_id, None, Some(headers.clone()))?;
            Ok(Arc::new(DirectRefreshRouteTokenRefresher::new(url, client, None)))
        })
        .transpose()?;

    let mut config = xet_data::processing::data_client::default_config(
        endpoint,
        session.token_info.clone(),
        token_refresher,
        session.custom_headers.clone(),
    )?;

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
