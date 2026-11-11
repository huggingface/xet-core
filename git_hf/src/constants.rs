pub const GIT_HF_PROGRAM: &str = "git-hf";

/// Environment variable holding a Hugging Face token.
pub const HF_TOKEN_ENV: &str = "HF_TOKEN";
/// Older name for [`HF_TOKEN_ENV`], used when it is unset or blank.
pub const LEGACY_HF_TOKEN_ENV: &str = "HUGGING_FACE_HUB_TOKEN";
/// Environment variable overriding the token file's path.
pub const HF_TOKEN_PATH_ENV: &str = "HF_TOKEN_PATH";
/// Environment variable overriding the Hugging Face home directory.
pub const HF_HOME_ENV: &str = "HF_HOME";
/// Environment variable naming a Hub endpoint other than the defaults, such as a staging Hub.
pub const HF_ENDPOINT_ENV: &str = "HF_ENDPOINT";
/// Hosts of the public and staging Hub, matching huggingface_hub's `HF_URL_HOSTS`.
pub const HUB_HOSTS: [&str; 3] = ["huggingface.co", "hf.co", "hub-ci.huggingface.co"];

/// How to provide credentials for an HTTP(S) remote, appended to authentication errors.
pub const AUTH_GUIDANCE: &str =
    "store a token with a Git credential helper, for example with `hf auth login --add-to-git-credential`";
/// How to provide credentials for an SSH remote, appended to authentication errors.
pub const SSH_AUTH_GUIDANCE: &str = "add your SSH key to your account";
