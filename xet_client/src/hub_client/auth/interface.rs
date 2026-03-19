use async_trait::async_trait;
use reqwest_middleware::RequestBuilder;

use crate::error::ClientError;

#[async_trait]
pub trait CredentialHelper: Send + Sync {
    async fn fill_credential(&self, req: RequestBuilder) -> Result<RequestBuilder, ClientError>;

    fn whoami(&self) -> &str;
}
