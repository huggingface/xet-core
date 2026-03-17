use xet_data::processing::configurations::TranslatorConfig;

use super::session::XetSession;
use crate::error::XetError;

// Helper function to create TranslatorConfig
pub(super) fn create_translator_config(session: &XetSession) -> Result<TranslatorConfig, XetError> {
    let endpoint = session
        .endpoint
        .clone()
        .unwrap_or_else(|| session.config.data.default_cas_endpoint.clone());

    let mut config = xet_data::processing::data_client::default_config(
        endpoint,
        session.token_info.clone(),
        session.token_refresher.clone(),
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
