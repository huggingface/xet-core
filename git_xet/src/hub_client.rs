use std::sync::Arc;

use async_trait::async_trait;
use cas_client::exports::ClientWithMiddleware;
use cas_client::{RetryConfig, build_http_client};
use reqwest::RequestBuilder;
use utils::auth::{TokenInfo, TokenRefresher};
use utils::errors::AuthError;

use crate::constants::{GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM, XET_ACCESS_TOKEN_HEADER, XET_TOKEN_EXPIRATION_HEADER};
use crate::errors::*;

#[derive(Debug)]
pub struct HubClient {
    pub endpoint: String,
    pub repo_type: String,
    pub repo_id: String,
    pub client: ClientWithMiddleware,
}

impl HubClient {
    pub fn new(endpoint: &str, repo_type: &str, repo_id: &str) -> Result<Self> {
        Ok(HubClient {
            endpoint: endpoint.to_owned(),
            repo_type: repo_type.to_owned(),
            repo_id: repo_id.to_owned(),
            client: build_http_client(RetryConfig::default(), "")?,
        })
    }

    // Get CAS access token from Hub access token. "token_type" is either "read" or "write".
    pub async fn get_jwt_token(&self, token_type: &str) -> Result<(String, String, u64)> {
        let endpoint = self.endpoint.as_str();
        let repo_type = self.repo_type.as_str();
        let repo_id = self.repo_id.as_str();

        let url = format!("{endpoint}/api/{repo_type}s/{repo_id}/xet-{token_type}-token/main");

        let response = self
            .client
            .get(url)
            .header("user-agent", GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM)
            //.bearer_auth(&self.token)
            .send()
            .await
            .map_err(internal)?;

        let headers = response.headers();
        let cas_endpoint = headers
            .get("X-Xet-Cas-Url")
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

    async fn refresh_jwt_token(&self, token_type: &str) -> Result<(String, u64)> {
        let (_, jwt_token, jwt_token_expiry) = self.get_jwt_token(token_type).await?;

        Ok((jwt_token, jwt_token_expiry))
    }
}

#[derive(Debug)]
pub struct HubClientTokenRefresher {
    pub token_type: String,
    pub client: Arc<HubClient>,
}

#[async_trait]
impl TokenRefresher for HubClientTokenRefresher {
    async fn refresh(&self) -> std::result::Result<TokenInfo, AuthError> {
        let client = self.client.clone();
        let token_type = self.token_type.clone();
        client
            .refresh_jwt_token(&token_type)
            .await
            .map_err(AuthError::token_refresh_failure)
    }
}
