use std::sync::Arc;

use async_trait::async_trait;
use cas_client::exports::ClientWithMiddleware;
use cas_client::{RetryConfig, build_http_client};
use utils::auth::{TokenInfo, TokenRefresher};
use utils::errors::AuthError;

use crate::auth::{CredentialHelper, Operation, get_creds};
use crate::constants::{
    GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM, XET_ACCESS_TOKEN_HEADER, XET_CAS_URL_HEADER, XET_TOKEN_EXPIRATION_HEADER,
};
use crate::errors::*;
use crate::git_repo::GitRepo;
use crate::git_url::GitUrl;

pub struct HubClient {
    endpoint: String,
    repo_type: String,
    repo_id: String,
    client: ClientWithMiddleware,
    cred_helper: Arc<dyn CredentialHelper>,
}

impl HubClient {
    pub fn new(
        endpoint: &str,
        repo_type: &str,
        repo_id: &str,
        cred_helper: Arc<dyn CredentialHelper>,
        session_id: &str,
    ) -> Result<Self> {
        Ok(HubClient {
            endpoint: endpoint.to_owned(),
            repo_type: repo_type.to_owned(),
            repo_id: repo_id.to_owned(),
            client: build_http_client(RetryConfig::default(), session_id)?,
            cred_helper,
        })
    }

    // Get CAS access token from Hub access token. "token_type" is either "read" or "write".
    pub async fn get_jwt_token(&self, token_type: &str) -> Result<(String, String, u64)> {
        let endpoint = self.endpoint.as_str();
        let repo_type = self.repo_type.as_str();
        let repo_id = self.repo_id.as_str();

        let url = format!("{endpoint}/api/{repo_type}s/{repo_id}/xet-{token_type}-token/main");

        let req = self.client.get(url).header("user-agent", GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM);
        let req = self.cred_helper.fill_creds(req).await?;
        let response = req.send().await.map_err(internal)?;

        let headers = response.headers();
        let cas_endpoint = headers
            .get(XET_CAS_URL_HEADER)
            .ok_or(GitXetError::InternalError("".to_owned()))?
            .to_str()
            .map_err(internal)?
            .to_owned();
        let jwt_token: String = headers
            .get(XET_ACCESS_TOKEN_HEADER)
            .ok_or(GitXetError::InternalError("".to_owned()))?
            .to_str()
            .map_err(internal)?
            .to_owned();
        let jwt_token_expiry: u64 = headers
            .get(XET_TOKEN_EXPIRATION_HEADER)
            .ok_or(GitXetError::InternalError("".to_owned()))?
            .to_str()
            .map_err(internal)?
            .parse()
            .map_err(internal)?;

        Ok((cas_endpoint, jwt_token, jwt_token_expiry))
    }
}

pub struct HubClientTokenRefresher {
    token_type: &'static str,
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

        let cred_helper = get_creds(repo, &remote_url, operation)?;

        let client =
            HubClient::new(&endpoint, repo_info.repo_type.as_str(), &repo_info.full_name, cred_helper, session_id)?;

        Ok(Self {
            token_type: operation.token_type(),
            client: Arc::new(client),
        })
    }
}

#[async_trait]
impl TokenRefresher for HubClientTokenRefresher {
    async fn refresh(&self) -> std::result::Result<TokenInfo, AuthError> {
        let (_, jwt_token, jwt_token_expiry) = self
            .client
            .get_jwt_token(self.token_type)
            .await
            .map_err(AuthError::token_refresh_failure)?;

        Ok((jwt_token, jwt_token_expiry))
    }
}
