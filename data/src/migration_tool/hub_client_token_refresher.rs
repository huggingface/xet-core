use std::sync::Arc;

use hub_client::{HubClient, Operation};
use utils::auth::{TokenInfo, TokenRefresher};
use utils::errors::AuthError;

pub struct HubClientTokenRefresher {
    pub operation: Operation,
    pub client: Arc<HubClient>,
}

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl TokenRefresher for HubClientTokenRefresher {
    async fn refresh(&self) -> std::result::Result<TokenInfo, AuthError> {
        let jwt_info = self
            .client
            .get_cas_jwt(self.operation)
            .await
            .map_err(AuthError::token_refresh_failure)?;

        Ok((jwt_info.access_token, jwt_info.exp))
    }
}
