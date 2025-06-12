use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use cas_client::exports::ClientWithMiddleware;
use cas_client::{build_http_client, Api, RetryConfig};
use utils::auth::{TokenInfo, TokenRefresher};
use utils::errors::AuthError;

#[derive(Debug)]
pub struct HubClient {
    pub endpoint: String,
    pub token: String,
    pub repo_type: String,
    pub repo_id: String,
    pub client: ClientWithMiddleware,
}

impl HubClient {
    pub fn new(endpoint: &str, token: &str, repo_type: &str, repo_id: &str) -> Result<Self> {
        Ok(HubClient {
            endpoint: endpoint.to_owned(),
            token: token.to_owned(),
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
            .with_extension(Api("xet-token"))
            .bearer_auth(&self.token)
            .header("user-agent", "xtool")
            .send()
            .await?;

        let headers = response.headers();
        let cas_endpoint = headers
            .get("X-Xet-Cas-Url")
            .ok_or(anyhow!("CAS url not found"))?
            .to_str()?
            .to_owned();
        let jwt_token: String = headers
            .get("X-Xet-Access-Token")
            .ok_or(anyhow!("access token not found"))?
            .to_str()?
            .to_owned();
        let jwt_token_expiry: u64 = headers
            .get("X-Xet-Token-Expiration")
            .ok_or(anyhow!("token expiration not found"))?
            .to_str()?
            .parse()?;

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

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use cas_client::{build_http_client, RetryConfig};

    use super::HubClient;

    #[tokio::test]
    #[ignore = "need valid token"]
    async fn test_get_jwt_token() -> Result<()> {
        let hub_client = HubClient {
            endpoint: "https://xethub-poc.us.dev.moon.huggingface.tech".to_owned(),
            token: "[MASKED]".to_owned(),
            repo_type: "dataset".to_owned(),
            repo_id: "test/t2".to_owned(),
            client: build_http_client(RetryConfig::default(), "")?,
        };

        let (cas_endpoint, jwt_token, jwt_token_expiry) = hub_client.get_jwt_token("read").await?;

        println!("{cas_endpoint}, {jwt_token}, {jwt_token_expiry}");

        println!("{:?}", hub_client.refresh_jwt_token("write").await?);

        Ok(())
    }
}
