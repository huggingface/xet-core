use anyhow::Result;
use async_trait::async_trait;
use reqwest_middleware::RequestBuilder;

#[async_trait]
pub trait CredentialHelper: Send + Sync {
    async fn fill_credential(&self, req: RequestBuilder) -> Result<RequestBuilder>;

    /// Fill credentials into a HeaderMap (for Unix socket requests).
    fn fill_headers(&self, headers: &mut reqwest::header::HeaderMap) {
        let _ = headers; // Default implementation does nothing
    }

    // Used in tests to identify the source of the credential.
    fn whoami(&self) -> &str;
}
