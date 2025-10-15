use cas_client::{Api, ResponseErrorLogger};
use http::header;
use urlencoding::encode;

use crate::{CasJWTInfo, HubClient, HubClientError, Operation};

#[async_trait::async_trait]
pub trait HubXetTokenTrait {
    // Get CAS access token from Hub access token.
    async fn get_xet_token(&self, operation: Operation) -> crate::Result<CasJWTInfo>;
}

#[async_trait::async_trait]
impl HubXetTokenTrait for HubClient {
    async fn get_xet_token(&self, operation: Operation) -> crate::Result<CasJWTInfo> {
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
