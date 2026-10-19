pub const GIT_HF_PROGRAM: &str = "git-hf";

/// Environment variable holding a Hugging Face token.
pub const HF_TOKEN_ENV: &str = "HF_TOKEN";
/// Older name for [`HF_TOKEN_ENV`], used when it is unset or blank.
pub const LEGACY_HF_TOKEN_ENV: &str = "HUGGING_FACE_HUB_TOKEN";
/// Environment variable overriding the token file's path.
pub const HF_TOKEN_PATH_ENV: &str = "HF_TOKEN_PATH";
/// Environment variable overriding the Hugging Face home directory.
pub const HF_HOME_ENV: &str = "HF_HOME";

/// How to provide credentials, appended to authentication errors.
pub const AUTH_GUIDANCE: &str = "set HF_TOKEN, run `hf auth login`, or store a token with a Git credential helper";
