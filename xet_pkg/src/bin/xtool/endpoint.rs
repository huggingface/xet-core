use std::sync::Arc;

use anyhow::Result;
use http::header::{self, HeaderMap, HeaderValue};
use xet_client::hub_client::{BearerCredentialHelper, CredentialHelper, HubClient, Operation, RepoInfo};
use xet_runtime::core::XetContext;

use super::Cli;

const USER_AGENT: &str = concat!("xtool", "/", env!("CARGO_PKG_VERSION"));

/// Hub credentials stored so commands can construct their own HubClient when needed.
pub struct HubInfo {
    pub hub_endpoint: String,
    pub hub_token: String,
    pub repo_type: String,
    pub repo_id: String,
}

impl HubInfo {
    /// Build a fresh HubClient from the stored credentials.
    pub fn build_hub_client(&self, ctx: &XetContext) -> Result<HubClient> {
        build_hub_client(ctx, &self.hub_endpoint, &self.hub_token, &self.repo_type, &self.repo_id)
    }

    /// Build the Hub token-refresh endpoint for the given operation.
    pub fn token_refresh_url(&self, operation: Operation) -> String {
        format!(
            "{}/api/{}s/{}/xet-{}-token/main",
            self.hub_endpoint,
            self.repo_type,
            self.repo_id,
            operation.token_type()
        )
    }

    /// Build auth headers for Hub token refresh calls.
    pub fn token_refresh_headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        headers.insert(header::USER_AGENT, HeaderValue::from_static(USER_AGENT));

        if !self.hub_token.is_empty() {
            let auth = HeaderValue::from_str(&format!("Bearer {}", self.hub_token))
                .map_err(|e| anyhow::anyhow!("invalid hub token for auth header: {e}"))?;
            headers.insert(header::AUTHORIZATION, auth);
        }

        Ok(headers)
    }
}

/// Resolved endpoint configuration used by all commands.
pub struct EndpointConfig {
    /// The CAS endpoint URL (either specified directly or resolved from Hub JWT).
    pub cas_endpoint: String,
    /// Auth token and expiry for CAS access.
    pub token_info: Option<(String, u64)>,
    /// Present when resolved via Hub mode; commands that need a HubClient
    /// (e.g. dedup for token refresh) can call `hub_info.build_hub_client()`.
    pub hub_info: Option<HubInfo>,
}

impl EndpointConfig {
    pub fn token_info(&self) -> Option<(String, u64)> {
        self.token_info.clone()
    }

    pub fn token_refresh(&self, operation: Operation) -> Result<Option<(String, HeaderMap)>> {
        if let Some(hub_info) = &self.hub_info {
            Ok(Some((hub_info.token_refresh_url(operation), hub_info.token_refresh_headers()?)))
        } else {
            Ok(None)
        }
    }

    /// Resolve the endpoint from CLI options.
    ///
    /// Two mutually exclusive modes:
    /// - **Direct**: `--endpoint` without `--repo-type`/`--repo-id`. The endpoint is the CAS endpoint directly.
    /// - **Hub**: `--repo-type` + `--repo-id` (with optional `--endpoint` for Hub URL). Builds a `HubClient`, calls
    ///   `get_cas_jwt` to resolve the CAS endpoint and token.
    pub async fn resolve(cli: &Cli, ctx: &XetContext, operation: Operation) -> Result<Self> {
        if cli.is_hub_mode() {
            let repo_type = cli
                .repo_type
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("--repo-type is required when --repo-id is set"))?;
            let repo_id = cli
                .repo_id
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("--repo-id is required when --repo-type is set"))?;

            let hub_endpoint = cli.resolved_endpoint();
            let hub_token = cli.resolved_token().unwrap_or_default();
            let normalized_repo_type = RepoInfo::try_from(repo_type, repo_id)?.repo_type.as_str().to_owned();

            let hub_client = build_hub_client(ctx, &hub_endpoint, &hub_token, repo_type, repo_id)?;
            let jwt_info = hub_client.get_cas_jwt(operation).await?;

            let hub_info = HubInfo {
                hub_endpoint,
                hub_token,
                repo_type: normalized_repo_type,
                repo_id: repo_id.to_owned(),
            };

            Ok(EndpointConfig {
                cas_endpoint: jwt_info.cas_url,
                token_info: Some((jwt_info.access_token, jwt_info.exp)),
                hub_info: Some(hub_info),
            })
        } else {
            let endpoint = cli.resolved_endpoint();
            let token = cli.resolved_token();

            Ok(EndpointConfig {
                cas_endpoint: endpoint,
                token_info: token.map(|t| (t, u64::MAX)),
                hub_info: None,
            })
        }
    }
}

fn build_hub_client(
    ctx: &XetContext,
    hub_endpoint: &str,
    token: &str,
    repo_type: &str,
    repo_id: &str,
) -> Result<HubClient> {
    let mut headers = HeaderMap::new();
    headers.insert(header::USER_AGENT, HeaderValue::from_static(USER_AGENT));

    let cred_helper = BearerCredentialHelper::new(token.to_owned(), "");
    HubClient::new(
        ctx.clone(),
        hub_endpoint,
        RepoInfo::try_from(repo_type, repo_id)?,
        Some("main".to_owned()),
        "",
        Some(cred_helper as Arc<dyn CredentialHelper>),
        Some(headers),
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hub_refresh_url_uses_repo_and_operation() {
        let hub = HubInfo {
            hub_endpoint: "https://huggingface.co".to_owned(),
            hub_token: "hf_test_token".to_owned(),
            repo_type: "model".to_owned(),
            repo_id: "org/repo".to_owned(),
        };

        assert_eq!(
            hub.token_refresh_url(Operation::Upload),
            "https://huggingface.co/api/models/org/repo/xet-write-token/main"
        );
        assert_eq!(
            hub.token_refresh_url(Operation::Download),
            "https://huggingface.co/api/models/org/repo/xet-read-token/main"
        );
    }

    #[test]
    fn test_endpoint_config_preserves_token_expiry() {
        let ep = EndpointConfig {
            cas_endpoint: "https://cas.example.com".to_owned(),
            token_info: Some(("cas_jwt".to_owned(), 1_735_000_000)),
            hub_info: None,
        };

        assert_eq!(ep.token_info(), Some(("cas_jwt".to_owned(), 1_735_000_000)));
    }

    #[test]
    fn test_endpoint_config_builds_hub_refresh_config() {
        let ep = EndpointConfig {
            cas_endpoint: "https://cas.example.com".to_owned(),
            token_info: Some(("cas_jwt".to_owned(), 1_735_000_000)),
            hub_info: Some(HubInfo {
                hub_endpoint: "https://huggingface.co".to_owned(),
                hub_token: "hf_test_token".to_owned(),
                repo_type: "model".to_owned(),
                repo_id: "org/repo".to_owned(),
            }),
        };

        let (refresh_url, headers) = ep.token_refresh(Operation::Upload).unwrap().unwrap();
        assert_eq!(refresh_url, "https://huggingface.co/api/models/org/repo/xet-write-token/main");
        assert_eq!(headers.get(header::AUTHORIZATION).unwrap(), "Bearer hf_test_token");
        assert_eq!(headers.get(header::USER_AGENT).unwrap(), USER_AGENT);
    }
}
