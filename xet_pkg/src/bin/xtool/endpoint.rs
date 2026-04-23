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
}

/// Resolved endpoint configuration used by all commands.
pub struct EndpointConfig {
    /// The CAS endpoint URL (either specified directly or resolved from Hub JWT).
    pub cas_endpoint: String,
    /// Auth token for CAS access.
    pub token: Option<String>,
    /// Present when resolved via Hub mode; commands that need a HubClient
    /// (e.g. dedup for token refresh) can call `hub_info.build_hub_client()`.
    pub hub_info: Option<HubInfo>,
}

impl EndpointConfig {
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

            let hub_client = build_hub_client(ctx, &hub_endpoint, &hub_token, repo_type, repo_id)?;
            let jwt_info = hub_client.get_cas_jwt(operation).await?;

            let hub_info = HubInfo {
                hub_endpoint,
                hub_token,
                repo_type: repo_type.to_owned(),
                repo_id: repo_id.to_owned(),
            };

            Ok(EndpointConfig {
                cas_endpoint: jwt_info.cas_url,
                token: Some(jwt_info.access_token),
                hub_info: Some(hub_info),
            })
        } else {
            let endpoint = cli.resolved_endpoint();
            let token = cli.resolved_token();

            Ok(EndpointConfig {
                cas_endpoint: endpoint,
                token,
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
