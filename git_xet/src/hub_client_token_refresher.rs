use std::sync::Arc;

use async_trait::async_trait;
use hub_client::{HubClient, Operation};
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
            repo_info.repo_type.as_str(),
            &repo_info.full_name,
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
