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
        let client = build_http_client(&session.inner.ctx, &session_id, None, Some(Arc::new(token_refresh_headers)))?;
        let direct_route_refresher =
            DirectRefreshRouteTokenRefresher::new(session.inner.ctx.clone(), url, client, None);

        // CAS endpoint is not provided but CAS token refresh endpoint is provided, we
        // refresh once to get the CAS endpoint, and fill the token info if nothing is provided.
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

    let endpoint = endpoint.unwrap_or_else(|| session.inner.ctx.config.data.default_cas_endpoint.clone());

    let mut config = xet_data::processing::data_client::default_config(
        &session.inner.ctx,
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

#[cfg(test)]
mod tests {
    use http::HeaderMap;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::xet_session::XetSessionBuilder;
    use crate::xet_session::auth_group_builder::AuthOptions;

    /// Pattern A: when `endpoint` is set, the token refresh route must not be called
    /// during `build` — the endpoint is used as-is and the first refresh is deferred
    /// until the token actually expires.
    #[tokio::test]
    async fn test_endpoint_provided_skips_eager_refresh() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "casUrl": "https://should-not-be-used.example.com",
                "exp": 9_999_999_999u64,
                "accessToken": "should-not-be-fetched",
            })))
            .expect(0)
            .mount(&server)
            .await;

        let refresh_url = format!("{}/token", server.uri());
        let session = XetSessionBuilder::new().build().unwrap();

        let auth_options = AuthOptions {
            endpoint: Some("https://cas.example.com".to_string()),
            custom_headers: None,
            token_info: None,
            token_refresh: Some((refresh_url, HeaderMap::new())),
        };

        let config = create_translator_config(&session, auth_options).await.unwrap();

        assert_eq!(config.session.endpoint, "https://cas.example.com");
    }

    /// Pattern B: when `endpoint` is not set but `token_refresh` is set, `create_translator_config`
    /// calls the refresh URL exactly once, uses the returned `cas_url` as the endpoint,
    /// and seeds `token_info` from the response when none was pre-supplied.
    #[tokio::test]
    async fn test_eager_refresh_sets_endpoint_and_token() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "casUrl": "https://cas.example.com",
                "exp": 9_999_999_999u64,
                "accessToken": "eagerly-fetched-token",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let refresh_url = format!("{}/token", server.uri());
        let session = XetSessionBuilder::new().build().unwrap();

        let auth_options = AuthOptions {
            endpoint: None,
            custom_headers: None,
            token_info: None,
            token_refresh: Some((refresh_url, HeaderMap::new())),
        };

        let config = create_translator_config(&session, auth_options).await.unwrap();

        assert_eq!(config.session.endpoint, "https://cas.example.com");
        let auth = config.session.auth.expect("auth config should be set");
        assert_eq!(auth.token, "eagerly-fetched-token");
        assert_eq!(auth.token_expiration, 9_999_999_999);
    }

    /// Pattern B: when `token_info` is already provided alongside `token_refresh` but no `endpoint`,
    /// the refresh is still called once to obtain the `cas_url`, but the pre-supplied
    /// token is preserved — it must NOT be overwritten by the refresh response.
    #[tokio::test]
    async fn test_eager_refresh_preserves_existing_token_info() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "casUrl": "https://cas.example.com",
                "exp": 9_999_999_999u64,
                "accessToken": "eagerly-fetched-token",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let refresh_url = format!("{}/token", server.uri());
        let session = XetSessionBuilder::new().build().unwrap();

        let auth_options = AuthOptions {
            endpoint: None,
            custom_headers: None,
            token_info: Some(("pre-supplied-token".to_string(), 1_000_000_000)),
            token_refresh: Some((refresh_url, HeaderMap::new())),
        };

        let config = create_translator_config(&session, auth_options).await.unwrap();

        assert_eq!(config.session.endpoint, "https://cas.example.com");
        let auth = config.session.auth.expect("auth config should be set");
        assert_eq!(auth.token, "pre-supplied-token");
        assert_eq!(auth.token_expiration, 1_000_000_000);
    }
}
