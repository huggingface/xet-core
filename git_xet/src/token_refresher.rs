use std::sync::Arc;

use async_trait::async_trait;
use cas_client::{Api, RetryConfig, build_http_client};
use hub_client::{CasJWTInfo, CredentialHelper, HubClient, Operation};
use reqwest::header;
use reqwest_middleware::ClientWithMiddleware;
use utils::auth::{TokenInfo, TokenRefresher};
use utils::errors::AuthError;

use crate::auth::get_credential;
use crate::constants::GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM;
use crate::errors::Result;
use crate::git_repo::GitRepo;
use crate::git_url::GitUrl;

pub struct HubClientTokenRefresher {
    operation: Operation,
    client: Arc<HubClient>,
}

impl HubClientTokenRefresher {
    pub fn new(
        repo: &GitRepo,
        remote_url: Option<GitUrl>,
        token_endpoint: Option<String>,
        operation: Operation,
        session_id: &str,
    ) -> Result<Self> {
        let remote_url = match remote_url {
            Some(r) => r,
            None => repo.remote_url()?,
        };
        let repo_info = remote_url.repo_info()?;

        let endpoint = match token_endpoint {
            Some(e) => e,
            None => remote_url.to_derived_http_host_url()?,
        };

        let cred_helper = get_credential(repo, &remote_url, operation)?;

        let client = HubClient::new(
            &endpoint,
            repo_info,
            repo.branch_name()?,
            GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM,
            session_id,
            cred_helper,
        )?;

        Ok(Self {
            operation,
            client: Arc::new(client),
        })
    }
}

#[async_trait]
impl TokenRefresher for HubClientTokenRefresher {
    async fn refresh(&self) -> std::result::Result<TokenInfo, AuthError> {
        let jwt_info = self
            .client
            .get_cas_jwt(self.operation)
            .await
            .map_err(AuthError::token_refresh_failure)?;

        Ok((jwt_info.access_token, jwt_info.exp))
    }
}

pub struct DirectRefreshRouteTokenRefresher {
    refresh_route: String,
    client: ClientWithMiddleware,
    cred_helper: Arc<dyn CredentialHelper>,
}

impl DirectRefreshRouteTokenRefresher {
    pub fn new(
        repo: &GitRepo,
        remote_url: Option<GitUrl>,
        refresh_route: &str,
        operation: Operation,
        session_id: &str,
    ) -> Result<Self> {
        let remote_url = match remote_url {
            Some(r) => r,
            None => repo.remote_url()?,
        };

        let cred_helper = get_credential(repo, &remote_url, operation)?;

        Ok(Self {
            refresh_route: refresh_route.to_owned(),
            client: build_http_client(RetryConfig::default(), session_id)?,
            cred_helper,
        })
    }
}

#[async_trait]
impl TokenRefresher for DirectRefreshRouteTokenRefresher {
    async fn refresh(&self) -> std::result::Result<TokenInfo, AuthError> {
        let req = self
            .client
            .get(&self.refresh_route)
            .with_extension(Api("xet-token"))
            .header(header::USER_AGENT, GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM);
        let req = self
            .cred_helper
            .fill_credential(req)
            .await
            .map_err(AuthError::token_refresh_failure)?;
        let response = req.send().await.map_err(AuthError::token_refresh_failure)?;

        let jwt_info: CasJWTInfo = response.json().await.map_err(AuthError::token_refresh_failure)?;

        Ok((jwt_info.access_token, jwt_info.exp))
    }
}
