// Naming convention
pub const GIT_EXECUTABLE: &str = "git";
pub const GIT_LFS_CUSTOM_TRANSFER_AGENT_NAME: &str = "xet";
pub const GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM: &str = "git-xet";

// The current version of executable
pub const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");

// Moon-landing Xet service headers
pub const XET_CAS_URL: &str = "X-Xet-Cas-Url";
pub const XET_ACCESS_TOKEN_HEADER: &str = "X-Xet-Access-Token";
pub const XET_TOKEN_EXPIRATION_HEADER: &str = "X-Xet-Token-Expiration";
pub const XET_SESSION_ID: &str = "X-Xet-Session-Id";

// Environment variable names
pub const HF_TOKEN_ENV: &str = "HF_TOKEN";
pub const HF_ENDPOINT_ENV: &str = "HF_ENDPOINT";
