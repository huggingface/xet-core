use std::sync::Arc;

use cas_client::exports::ClientWithMiddleware;
use cas_client::{Api, ResponseErrorLogger, RetryConfig, build_http_client};
use http::header;
use urlencoding::encode;

use crate::auth::CredentialHelper;
use crate::errors::*;
use crate::types::{CasJWTInfo, RepoInfo};

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
    repo_info: RepoInfo,
    reference: Option<String>,
    user_agent: String,
    client: ClientWithMiddleware,
    cred_helper: Arc<dyn CredentialHelper>,
}

impl HubClient {
    pub fn new(
        endpoint: &str,
        repo_info: RepoInfo,
        reference: Option<String>,
        user_agent: &str,
        session_id: &str,
        cred_helper: Arc<dyn CredentialHelper>,
    ) -> Result<Self> {
        Ok(HubClient {
            endpoint: endpoint.to_owned(),
            repo_info,
            reference,
            user_agent: user_agent.to_owned(),
            client: build_http_client(RetryConfig::default(), session_id)?,
            cred_helper,
        })
    }

    // Get CAS access token from Hub access token.
    pub async fn get_cas_jwt(&self, operation: Operation) -> Result<CasJWTInfo> {
        let endpoint = self.endpoint.as_str();
        let repo_type = self.repo_info.repo_type.as_str();
        let repo_id = self.repo_info.full_name.as_str();
        let token_type = operation.token_type();

        // The reference may contain "/" but the "xet-[]-token" API only parses "rev" from a single component,
        // thus we encode the reference. It defaults to "main" if not specified by caller because the
        // API route expects a "rev" component.
        let rev = encode(self.reference.as_deref().unwrap_or("main"));

        // Clients can get a xet write token, if
        // - the "rev" is a regular branch, with a HF write token;
        // - the "rev" is a pr branch, with a HF write or read token;
        // - it intends to create a pr and repo is enabled for discussion, with a HF write or read token.
        let query = if matches!(operation, Operation::Upload) && self.reference.is_none() {
            "?create_pr=1"
        } else {
            ""
        };

        // note that this API doesn't take a Basic auth
        let url = format!("{endpoint}/api/{repo_type}s/{repo_id}/xet-{token_type}-token/{rev}{query}");

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
        let response = req.send().await.process_error("xet-write-token")?;

        let info: CasJWTInfo = response.json().await?;

        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use super::HubClient;
    use crate::errors::Result;
    use crate::{BearerCredentialHelper, HFRepoType, Operation, RepoInfo};

    #[tokio::test]
    #[ignore = "need valid write token"]
    async fn test_get_jwt_token_with_hf_write_token() -> Result<()> {
        let cred_helper = BearerCredentialHelper::new("[hf_write_token]".to_owned(), "");
        let hub_client = HubClient::new(
            "https://huggingface.co",
            RepoInfo {
                repo_type: HFRepoType::Model,
                full_name: "seanses/tm".into(),
            },
            Some("main".into()),
            "xtool",
            "",
            cred_helper,
        )?;

        let read_info = hub_client.get_cas_jwt(Operation::Upload).await?;

        assert!(read_info.access_token.len() > 0);
        assert!(read_info.cas_url.len() > 0);
        assert!(read_info.exp > 0);

        Ok(())
    }

    #[tokio::test]
    #[ignore = "need valid read token and pr created on hub"]
    async fn test_get_jwt_token_with_hf_read_token_pr_branch() -> Result<()> {
        let cred_helper = BearerCredentialHelper::new("[hf_read_token]".to_owned(), "");
        let hub_client = HubClient::new(
            "https://huggingface.co",
            RepoInfo {
                repo_type: HFRepoType::Model,
                full_name: "seanses/tm".into(),
            },
            Some("refs/pr/1".into()),
            "xtool",
            "",
            cred_helper,
        )?;

        let read_info = hub_client.get_cas_jwt(Operation::Upload).await?;

        assert!(read_info.access_token.len() > 0);
        assert!(read_info.cas_url.len() > 0);
        assert!(read_info.exp > 0);

        Ok(())
    }

    #[tokio::test]
    #[ignore = "need valid read token"]
    async fn test_get_jwt_token_with_hf_read_token_create_pr() -> Result<()> {
        let cred_helper = BearerCredentialHelper::new("[hf_read_token]".to_owned(), "");
        let hub_client = HubClient::new(
            "https://huggingface.co",
            RepoInfo {
                repo_type: HFRepoType::Model,
                full_name: "seanses/tm".into(),
            },
            None,
            "xtool",
            "",
            cred_helper,
        )?;

        let read_info = hub_client.get_cas_jwt(Operation::Upload).await?;

        assert!(read_info.access_token.len() > 0);
        assert!(read_info.cas_url.len() > 0);
        assert!(read_info.exp > 0);

        Ok(())
    }
}
