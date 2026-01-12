use std::sync::Arc;

use async_trait::async_trait;
use cas_client::retry_wrapper::RetryWrapper;
use cas_client::{Api, build_http_client};
use hub_client::{CasJWTInfo, CredentialHelper, Operation};
use reqwest_middleware::ClientWithMiddleware;
use utils::auth::{TokenInfo, TokenRefresher};
use utils::errors::AuthError;

use crate::auth::get_credential;
use crate::errors::Result;
use crate::git_repo::GitRepo;
use crate::git_url::GitUrl;

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
        user_agent: &str,
    ) -> Result<Self> {
        let remote_url = match remote_url {
            Some(r) => r,
            None => repo.remote_url()?,
        };

        let cred_helper = get_credential(repo, &remote_url, operation)?;

        Ok(Self {
            refresh_route: refresh_route.to_owned(),
            client: build_http_client(session_id, user_agent)?,
            cred_helper,
        })
    }
}

#[async_trait]
impl TokenRefresher for DirectRefreshRouteTokenRefresher {
    async fn refresh(&self) -> std::result::Result<TokenInfo, AuthError> {
        let client = self.client.clone();
        let refresh_route = self.refresh_route.clone();
        let cred_helper = self.cred_helper.clone();

        let jwt_info: CasJWTInfo = RetryWrapper::new("xet-token")
            .run_and_extract_json(move |_| {
                let refresh_route = refresh_route.clone();
                let client = client.clone();
                let cred_helper = cred_helper.clone();
                async move {
                    let req = client.get(&refresh_route).with_extension(Api("xet-token"));
                    let req = cred_helper
                        .fill_credential(req)
                        .await
                        .map_err(reqwest_middleware::Error::Middleware)?;
                    req.send().await
                }
            })
            .await
            .map_err(AuthError::token_refresh_failure)?;

        Ok((jwt_info.access_token, jwt_info.exp))
    }
}
