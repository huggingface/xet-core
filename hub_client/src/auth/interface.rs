use anyhow::Result;
use async_trait::async_trait;
use reqwest_middleware::RequestBuilder;

#[async_trait]
pub trait CredentialHelper: Send + Sync {
    async fn fill_credential(&self, req: RequestBuilder) -> Result<RequestBuilder>;
    // Used in tests to identify the source of the credential.
    fn whoami(&self) -> &str;
}
