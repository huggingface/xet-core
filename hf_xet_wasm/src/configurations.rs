use cas_object::CompressionScheme;
use utils::auth::AuthConfig;

/// Default prefix for CAS and shard operations in WASM.
pub const DEFAULT_PREFIX: &str = "default";

/// Session-specific configuration for WASM uploads.
/// Simpler than the native data crate configurations as WASM doesn't need disk-based paths.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub endpoint: String,
    pub user_agent: String,
    pub auth: Option<AuthConfig>,
    pub session_id: String,
    pub compression: Option<CompressionScheme>,
    pub prefix: String,
}

impl SessionConfig {
    pub fn new(endpoint: String, session_id: String) -> Self {
        Self {
            endpoint,
            user_agent: String::new(),
            auth: None,
            session_id,
            compression: None,
            prefix: DEFAULT_PREFIX.to_string(),
        }
    }

    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = user_agent.into();
        self
    }

    pub fn with_auth(mut self, auth: Option<AuthConfig>) -> Self {
        self.auth = auth;
        self
    }

    pub fn with_compression(mut self, compression: Option<CompressionScheme>) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }
}

#[derive(Debug)]
pub struct TranslatorConfig {
    pub session: SessionConfig,
}
