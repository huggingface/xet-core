use xet_data::processing::configurations::TranslatorConfig;

use super::XetSession;
use crate::error::XetError;

// Helper function to create TranslatorConfig
pub(super) fn create_translator_config(session: &XetSession) -> Result<TranslatorConfig, XetError> {
    let endpoint = session
        .inner
        .endpoint
        .clone()
        .unwrap_or_else(|| session.inner.config.data.default_cas_endpoint.clone());

    let mut config = xet_data::processing::data_client::default_config(
        endpoint,
        session.inner.token_info.clone(),
        session.inner.token_refresher.clone(),
        session.inner.custom_headers.clone(),
    )?;

    let session_id = session.inner.id.to_string();
    if !session_id.is_empty() {
        config.session.session_id = Some(session_id);
    }

    Ok(config)
}
