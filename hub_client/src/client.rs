use std::sync::Arc;

use cas_client::exports::ClientWithMiddleware;
use cas_client::{Api, RetryConfig, build_http_client};
use http::header;

use crate::auth::CredentialHelper;
use crate::errors::*;
use crate::types::CasJWTInfo;

/// The type of operation to perform, either to upload files or to download files.
/// Different operations lead to CAS access token with different authorization levels.
#[derive(Clone, Copy)]
pub enum Operation {
    Upload,
    Download,
}

impl Operation {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::Download => "download",
        }
    }

    pub fn token_type(&self) -> &'static str {
        match self {
            Self::Upload => "write",
            Self::Download => "read",
        }
    }
}

pub struct HubClient {
    endpoint: String,
    repo_type: String,
    repo_id: String,
    user_agent: String,
    client: ClientWithMiddleware,
    cred_helper: Arc<dyn CredentialHelper>,
}

impl HubClient {
    pub fn new(
        endpoint: &str,
        repo_type: &str,
        repo_id: &str,
        user_agent: &str,
        session_id: &str,
        cred_helper: Arc<dyn CredentialHelper>,
    ) -> Result<Self> {
        Ok(HubClient {
            endpoint: endpoint.to_owned(),
            repo_type: repo_type.to_owned(),
            repo_id: repo_id.to_owned(),
            user_agent: user_agent.to_owned(),
            client: build_http_client(RetryConfig::default(), session_id)?,
            cred_helper,
        })
    }

    // Get CAS access token from Hub access token.
    pub async fn get_cas_jwt(&self, operation: Operation) -> Result<CasJWTInfo> {
        let endpoint = self.endpoint.as_str();
        let repo_type = self.repo_type.as_str();
        let repo_id = self.repo_id.as_str();
        let token_type = operation.token_type();

        // note that this API doesn't take a Basic auth
        let url = format!("{endpoint}/api/{repo_type}s/{repo_id}/xet-{token_type}-token/main");

        let req = self
            .client
            .get(url)
            .with_extension(Api("xet-token"))
            .header(header::USER_AGENT, &self.user_agent);
        let req = self
            .cred_helper
            .fill_credential(req)
            .await
            .map_err(HubClientError::CredentialHelper)?;
        let response = req.send().await?;

        let info: CasJWTInfo = response.json().await?;

        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use super::HubClient;
    use crate::errors::Result;
    use crate::{BearerCredentialHelper, Operation};

    #[tokio::test]
    #[ignore = "need valid token"]
    async fn test_get_jwt_token() -> Result<()> {
        let cred_helper = BearerCredentialHelper::new("[hf_token]".to_owned(), "");
        let hub_client = HubClient::new("https://huggingface.co", "model", "seanses/tm", "xtool", "", cred_helper)?;

        let read_info = hub_client.get_cas_jwt(Operation::Download).await?;

        println!("{:?}", read_info);

        Ok(())
    }
}
