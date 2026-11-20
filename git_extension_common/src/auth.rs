//! Credential helpers for the Xet CAS token API on HF Hub. Choosing which helper applies to a
//! repository is each client's policy.
//!
//! - [`GitCredentialHelper`] reads a token for the remote's HTTP(S) host through `git credential fill`, which may
//!   prompt the user unless called with [`Interaction::Never`].
//! - [`SSHCredentialHelper`] runs `git-lfs-authenticate` over an SSH channel to the remote, as Git LFS
//!   does to authorize batch API requests. See https://github.com/git-lfs/git-lfs/blob/main/docs/api/authentication.md
//!   and https://github.com/git-lfs/git-lfs/blob/463ed9727d21155585721489fdb8cc81030f979b/lfshttp/ssh.go#L76
//!   for details of this command. The HF Xet CAS token API on HF Hub accepts this type of authorization.
mod git;
mod ssh;

pub use git::GitCredentialHelper;
pub use ssh::SSHCredentialHelper;
#[cfg(any(test, feature = "test-utils"))]
pub use ssh::{GitLFSAuthentationResponseHeader, GitLFSAuthenticateResponse};

/// Whether obtaining a credential may interact with the user.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Interaction {
    /// Prompts are acceptable, as in an explicit `git push`.
    Allowed,
    /// Never prompt or block on input, as in a Git filter running inside `git add`. Sources that need
    /// input are skipped.
    Never,
}
