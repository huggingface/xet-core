use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use reqwest_middleware::RequestBuilder;

use super::CredentialHelper;

pub struct NoopCredentialHelper {}

impl NoopCredentialHelper {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl CredentialHelper for NoopCredentialHelper {
    async fn fill_credential(&self, req: RequestBuilder) -> Result<RequestBuilder> {
        Ok(req)
    }

    fn whoami(&self) -> &str {
        "noop"
    }
}

pub struct BearerCredentialHelper {
    pub hf_token: String,

    _whoami: &'static str,
}

impl BearerCredentialHelper {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(hf_token: String, whoami: &'static str) -> Arc<Self> {
        Arc::new(Self {
            hf_token,
            _whoami: whoami,
        })
    }
}

#[async_trait]
impl CredentialHelper for BearerCredentialHelper {
    async fn fill_credential(&self, req: RequestBuilder) -> Result<RequestBuilder> {
        Ok(req.bearer_auth(&self.hf_token))
    }

    fn whoami(&self) -> &str {
        self._whoami
    }
}
