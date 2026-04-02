use std::sync::Arc;

use xet_client::cas_client::auth::{DirectRefreshRouteTokenRefresher, TokenRefresher};
use xet_client::common::http_client::build_http_client;
use xet_data::processing::configurations::TranslatorConfig;

use super::XetSession;
use super::auth_group_builder::AuthOptions;
use crate::error::XetError;

/// Builds a [`TranslatorConfig`] from the session defaults and the per-commit/group
/// [`AuthOptions`] supplied by the caller.
///
/// Endpoint resolution order:
/// 1. `auth_options.endpoint`, if set.
/// 2. If `token_refresh` is set but `endpoint` is not, the refresher is called once via `get_cas_jwt()` to obtain the
///    CAS URL. The resulting token is also stored as the initial `token_info` only if no `token_info` was already
///    provided.
/// 3. The session's `default_cas_endpoint` from its configuration.
///
/// `token_info` is an optional pre-seeded `(token, expiry_unix_secs)` pair.
///
/// `token_refresh` is an optional `(refresh_url, request_headers)` pair that is
/// wrapped in a [`DirectRefreshRouteTokenRefresher`] to keep the token fresh.
///
/// `custom_headers` are forwarded with every CAS HTTP request.
pub(super) async fn create_translator_config(
    session: &XetSession,
    auth_options: AuthOptions,
) -> Result<TranslatorConfig, XetError> {
    let session_id = session.inner.id.to_string();

    let AuthOptions {
        mut endpoint,
        custom_headers,
        mut token_info,
        token_refresh,
    } = auth_options;

    // Build token refresher
    let token_refresher: Option<Arc<dyn TokenRefresher>> = if let Some((url, token_refresh_headers)) = token_refresh {
        let client = build_http_client(&session_id, None, Some(Arc::new(token_refresh_headers)))?;
        let direct_route_refresher = DirectRefreshRouteTokenRefresher::new(url, client, None);

        // CAS endpoint is not provided but CAS token refresh endpoint is provided, we
        // refresh once to get the CAS endpoint, and fill the token info is nothing is provided.
        if endpoint.is_none() {
            let jwt_info = direct_route_refresher.get_cas_jwt().await?;

            if token_info.is_none() {
                token_info = Some((jwt_info.access_token, jwt_info.exp));
            }
            endpoint = Some(jwt_info.cas_url);
        }

        Some(Arc::new(direct_route_refresher))
    } else {
        None
    };

    let endpoint = endpoint.unwrap_or_else(|| session.inner.config.data.default_cas_endpoint.clone());

    let mut config = xet_data::processing::data_client::default_config(
        endpoint,
        token_info,
        token_refresher,
        custom_headers.map(Arc::new),
    )?;

    if !session_id.is_empty() {
        config.session.session_id = Some(session_id);
    }

    Ok(config)
}
