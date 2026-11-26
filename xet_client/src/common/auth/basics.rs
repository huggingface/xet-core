use std::sync::Arc;

use async_trait::async_trait;
use reqwest_middleware::RequestBuilder;

use super::CredentialHelper;
use crate::error::ClientError;

pub struct NoopCredentialHelper {}

impl NoopCredentialHelper {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl CredentialHelper for NoopCredentialHelper {
    async fn fill_credential(&self, req: RequestBuilder) -> Result<RequestBuilder, ClientError> {
        Ok(req)
    }
}

pub struct BearerCredentialHelper {
    pub hf_token: String,
}

impl BearerCredentialHelper {
    pub fn new(hf_token: String) -> Arc<Self> {
        Arc::new(Self { hf_token })
    }
}

#[async_trait]
impl CredentialHelper for BearerCredentialHelper {
    async fn fill_credential(&self, req: RequestBuilder) -> Result<RequestBuilder, ClientError> {
        Ok(req.bearer_auth(&self.hf_token))
    }
}
