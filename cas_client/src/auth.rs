use anyhow::anyhow;
use cas::auth::{AuthConfig, TokenProvider};
use reqwest::header::HeaderValue;
use reqwest::header::AUTHORIZATION;
use reqwest::{Request, Response};
use reqwest_middleware::{Middleware, Next};
use std::sync::{Arc, Mutex};

/// AuthMiddleware is a thread-safe middleware that adds a CAS auth token to outbound requests.
/// If the token it holds is expired, it will automatically be refreshed.
pub struct AuthMiddleware {
    token_provider: Arc<Mutex<TokenProvider>>,
}

impl AuthMiddleware {
    fn get_token(&self) -> Result<String, anyhow::Error> {
        let mut provider = self
            .token_provider
            .lock()
            .map_err(|e| anyhow!("lock error: {e:?}"))?;
        provider
            .get_valid_token()
            .map_err(|e| anyhow!("couldn't get token: {e:?}"))
    }
}

impl From<&AuthConfig> for AuthMiddleware {
    fn from(cfg: &AuthConfig) -> Self {
        Self {
            token_provider: Arc::new(Mutex::new(TokenProvider::new(cfg))),
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
        let token = self
            .get_token()
            .map_err(reqwest_middleware::Error::Middleware)?;

        let headers = req.headers_mut();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
        );
        next.run(req, extensions).await
    }
}
