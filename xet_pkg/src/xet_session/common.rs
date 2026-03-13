use xet_data::processing::configurations::TranslatorConfig;

use super::{SessionError, XetSession};

// Helper function to create TranslatorConfig
pub(crate) fn create_translator_config(session: &XetSession) -> Result<TranslatorConfig, SessionError> {
    let endpoint = session
        .endpoint
        .clone()
        .unwrap_or_else(|| session.config.data.default_cas_endpoint.clone());

    Ok(xet_data::processing::data_client::default_config(
        endpoint,
        None, // xorb_compression
        session.token_info.clone(),
        session.token_refresher.clone(),
        session.custom_headers.clone(),
    )?
    .with_session_id(&session.id.to_string()))
}

/// State of the upload commit and download group
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum GroupState {
    Alive,
    Finished,
    Aborted,
}
