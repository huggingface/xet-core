use std::str::FromStr;

use cas_client::exports::reqwest::RequestBuilder;

use crate::errors::*;

// The lfs.<url>.access configuration.
// If set to "basic" then credentials will be requested before making batch requests to this url,
// otherwise a public request will initially be attempted.
// If set to "none" then credentials are not needed. For this case we don't want to prompt the user
// for any crendential.
pub enum Access {
    None,
    Basic,
    Private,
    Negotiate,
    Empty,
}

impl FromStr for Access {
    type Err = GitXetError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "none" => Ok(Access::None),
            "basic" => Ok(Access::Basic),
            "private" => Ok(Access::Private),
            "negotiate" => Ok(Access::Negotiate),
            "" => Ok(Access::Empty),
            _ => Err(GitXetError::GitConfigError(format!("invalid \"lfs.<url>.access\" type: {s}"))),
        }
    }
}

fn do_request_with_auth(remote: String) {}

// getCreds fills the authorization header for the given request if possible,
// from the following sources:
//
// 1. Existing Authorization or ?token query tells LFS that the request is ready.
// 2. Netrc based on the hostname.
// 3. URL authentication on the Endpoint URL or the Git Remote URL.
// 4. Git Credential Helper, potentially prompting the user.
//
// There are three URLs in play, that make this a little confusing.
//
//  1. The request URL, which should be something like "https://git.com/repo.git/info/lfs/objects/batch"
//  2. The LFS API URL, which should be something like "https://git.com/repo.git/info/lfs"
//     This URL used for the "lfs.URL.access" git config key, which determines
//     what kind of auth the LFS server expects. Could be BasicAccess,
//     NTLMAccess, NegotiateAccess, or NoneAccess, in which the Git Credential
//     Helper step is skipped. We do not want to prompt the user for a password
//     to fetch public repository data.
//  3. The Git Remote URL, which should be something like "https://git.com/repo.git"
//     This URL is used for the Git Credential Helper. This way existing https
//     Git remote credentials can be re-used for LFS.
fn get_creds(req: &mut RequestBuilder) -> Result<()> {
    todo!()
}
