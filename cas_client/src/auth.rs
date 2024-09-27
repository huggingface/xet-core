use anyhow::anyhow;
use cas::auth::{AuthConfig, TokenProvider};
use reqwest::header::HeaderValue;
use reqwest::header::AUTHORIZATION;
use reqwest::{Request, Response};
use reqwest_middleware::{Middleware, Next};
use std::sync::{Arc, Mutex};
use tracing::info;

/// AuthMiddleware is a thread-safe middleware that adds a CAS auth token to outbound requests.
/// If the token it holds is expired, it will automatically be refreshed.
pub struct AuthMiddleware {
    token_provider: Option<Arc<Mutex<TokenProvider>>>,
}

impl AuthMiddleware {
    fn get_token(provider_ref: &Arc<Mutex<TokenProvider>>) -> Result<String, anyhow::Error> {
        let mut provider = provider_ref
            .lock()
            .map_err(|e| anyhow!("lock error: {e:?}"))?;
        provider
            .get_valid_token()
            .map_err(|e| anyhow!("couldn't get token: {e:?}"))
    }
}

impl From<Option<&AuthConfig>> for AuthMiddleware {
    fn from(value: Option<&AuthConfig>) -> Self {
        if value.is_none() {
            info!("CAS auth disabled");
        }
        Self {
            token_provider: value.map(|cfg| Arc::new(Mutex::new(TokenProvider::new(cfg)))),
        }
    }
}

#[async_trait::async_trait]
impl Middleware for AuthMiddleware {
    async fn handle(
        &self,
        mut req: Request,
        extensions: &mut hyper::http::Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<Response> {
        if let Some(ref provider) = self.token_provider {
            let token = Self::get_token(provider).map_err(reqwest_middleware::Error::Middleware)?;

            let headers = req.headers_mut();
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
            );
        }
        next.run(req, extensions).await
    }
}
