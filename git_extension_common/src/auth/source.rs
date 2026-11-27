use std::fmt;
use std::path::PathBuf;

/// Where a credential for Hub requests came from, for messages and tests. Never includes the secret itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialSource {
    RemoteUrl,
    /// The environment variable that held the token.
    Environment(&'static str),
    TokenFile(PathBuf),
    Netrc,
    Ssh,
    GitCredentialHelper,
    Anonymous,
}

impl fmt::Display for CredentialSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RemoteUrl => write!(f, "the token in the remote URL"),
            Self::Environment(name) => write!(f, "{name}"),
            Self::TokenFile(path) => write!(f, "the token file {}", path.display()),
            Self::Netrc => write!(f, ".netrc"),
            Self::Ssh => write!(f, "SSH authentication"),
            Self::GitCredentialHelper => write!(f, "the Git credential helper"),
            Self::Anonymous => write!(f, "no credentials"),
        }
    }
}
