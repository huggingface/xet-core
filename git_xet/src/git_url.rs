use std::str::FromStr;

use git_url_parse::GitUrl as innerGitUrl;
pub use git_url_parse::Scheme;
use hub_client::{HFRepoType, RepoInfo};

use crate::errors::{GitXetError, Result};

// This mod implements funtionalities to handle Git remote URLs, especially tailored for
// Git LFS and Hugging Face repo needs, including deriving Git LFS server endpoint from
// Git remote URL and handling HF specific repo types.

// `GitUrl` wraps inside a parsed Git remote URL and extends its capability for Git LFS
// and HF repo specific needs.
#[derive(Debug, Clone)]
pub struct GitUrl {
    _raw: String,
    inner: innerGitUrl,
}

impl FromStr for GitUrl {
    type Err = GitXetError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let raw = s.to_owned();
        let inner = innerGitUrl::parse(s)?;

        Ok(Self { _raw: raw, inner })
    }
}

impl GitUrl {
    #[allow(unused)]
    pub fn as_str(&self) -> &str {
        self._raw.as_str()
    }

    // Determine the LFS server endpoint from Git remote URL according to
    // https://github.com/git-lfs/git-lfs/blob/main/docs/api/server-discovery.md#server-discovery
    // Returns:
    // Ok(url) if a valid LFS server endpoint can be determined,
    // Err(e) if an error occurred.
    pub fn to_default_lfs_endpoint(&self) -> Result<String> {
        let canonical_http_format = self.to_derived_http_format_url()?;
        Ok(format!("{canonical_http_format}/info/lfs"))
    }

    fn to_derived_http_format_url(&self) -> Result<String> {
        let (scheme, translated) = self.to_derived_http_scheme()?;

        let auth_str = match self.inner.scheme {
            Scheme::Http | Scheme::Https => match (&self.inner.user, &self.inner.token) {
                (Some(user), Some(token)) => format!("{}:{}@", user, token),
                (Some(user), None) => format!("{}@", user),
                (None, Some(token)) => format!("{}@", token),
                (None, None) => "".to_owned(),
            },
            _ => "".to_owned(),
        };

        let host = self
            .inner
            .host
            .as_ref()
            .ok_or_else(|| GitXetError::config_error("remote URL missing host name"))?;

        let port = self.inner.port;
        let port_str = if translated || port.is_none() {
            // if translated then there may be a port change that we can't guess
            "".to_owned()
        } else {
            format!(":{}", port.unwrap())
        };

        let path = self.inner.path.trim_start_matches('/').trim_end_matches(".git");

        Ok(format!("{scheme}://{auth_str}{host}{port_str}/{path}.git"))
    }

    // Returns the same host as the LFS server endpoint determined from this Git remote URL.
    // This helps find credentials stored under this host name.
    pub fn to_derived_http_host_url(&self) -> Result<String> {
        let (scheme, translated) = self.to_derived_http_scheme()?;

        let host = self
            .inner
            .host
            .as_ref()
            .ok_or_else(|| GitXetError::config_error("remote URL missing host name"))?;

        let port = self.inner.port;
        let port_str = if translated || port.is_none() {
            // if translated then there's a port change that we can't guess
            "".to_owned()
        } else {
            format!(":{}", port.unwrap())
        };

        Ok(format!("{scheme}://{host}{port_str}"))
    }

    // Returns a tuple (scheme, translated) meaning if the original scheme was
    // translated (true) to the returned scheme, or not changed (false).
    fn to_derived_http_scheme(&self) -> Result<(&str, bool)> {
        match self.inner.scheme {
            Scheme::Http => Ok(("http", false)),
            Scheme::Https => Ok(("https", false)),
            Scheme::File | Scheme::Ftp | Scheme::Ftps => Err(GitXetError::not_supported(format!(
                "cannot convert from scheme \"{}://\" to \"http(s)://\"",
                self.inner.scheme
            ))),
            Scheme::Git | Scheme::GitSsh | Scheme::Ssh => Ok(("https", true)),
            Scheme::Unspecified => {
                Err(GitXetError::not_supported("cannot convert from unspecified scheme to \"http(s)://\""))
            },
        }
    }

    // Returns the scheme of the Git remote URL.
    pub fn scheme(&self) -> Scheme {
        self.inner.scheme
    }

    // Returns the front part of the Git remote URL removing repo path,
    // e.g. (scheme://)(auth)[host_name](:port)
    pub fn host_url(&self) -> Result<String> {
        let scheme_str = if self.inner.scheme_prefix {
            format!("{}://", self.inner.scheme)
        } else {
            "".to_owned()
        };

        let auth_str = match self.inner.scheme {
            Scheme::Http | Scheme::Https | Scheme::Ssh | Scheme::GitSsh => {
                match (&self.inner.user, &self.inner.token) {
                    (Some(user), Some(token)) => format!("{}:{}@", user, token),
                    (Some(user), None) => format!("{}@", user),
                    (None, Some(token)) => format!("{}@", token),
                    (None, None) => "".to_owned(),
                }
            },
            _ => "".to_owned(),
        };

        let host = self
            .inner
            .host
            .as_ref()
            .ok_or_else(|| GitXetError::config_error("remote URL missing host name"))?;

        let port_str = if let Some(p) = self.inner.port {
            format!(":{}", p)
        } else {
            "".to_owned()
        };

        Ok(format!("{scheme_str}{auth_str}{host}{port_str}"))
    }

    pub fn port(&self) -> Option<u16> {
        self.inner.port
    }

    // Returns the full repo path,
    // e.g. [repo_type/](owner)/(repo_name)
    pub fn full_repo_path(&self) -> String {
        self.inner.path.trim_start_matches('/').trim_end_matches(".git").to_owned()
    }

    // Returns the credential embedded in the Git remote URL as
    // (user, token).
    pub fn credential(&self) -> (Option<String>, Option<String>) {
        (self.inner.user.clone(), self.inner.token.clone())
    }

    // Returns the parsed full repo path into `RepoInfo`.
    #[allow(unused)]
    pub fn repo_info(&self) -> Result<RepoInfo> {
        let path = self.full_repo_path();
        let full_name = self.inner.fullname.clone(); // The full name of the repo, formatted as "owner/name"

        let repo_type = HFRepoType::from_str(path.trim_end_matches(&full_name).trim_end_matches('/'))?;

        Ok(RepoInfo { repo_type, full_name })
    }
}

#[cfg(test)]
mod test_lfs_server_discovery {
    use super::GitUrl;
    use crate::errors::{GitXetError, Result};

    #[test]
    fn test_canonicalize_to_https() -> Result<()> {
        for repo_type in [
            "", // empty repo_type default to model
            "models/",
            "datasets/",
            "spaces/",
        ] {
            let expected_full = format!("https://server.co/{repo_type}foo/bar.git");
            let expected_host = "https://server.co";

            let inputs = [
                format!("https://server.co/{repo_type}foo/bar"), // 1. https scheme without suffix ".git"
                format!("https://server.co/{repo_type}foo/bar.git"), // 2. https scheme with suffix ".git"
                format!("git@server.co:{repo_type}foo/bar"),     // 3. git scheme without suffix ".git"
                format!("git@server.co:{repo_type}foo/bar.git"), // 4. git scheme with suffix ".git"
                format!("ssh://server.co/{repo_type}foo/bar"),   // 5. ssh scheme without suffix ".git"
                format!("ssh://server.co/{repo_type}foo/bar.git"), // 6. ssh scheme with suffix ".git"
                format!("git+ssh://server.co/{repo_type}foo/bar"), // 7. git+ssh scheme without suffix ".git"
                format!("git+ssh://server.co/{repo_type}foo/bar.git"), // 8. git+ssh scheme with suffix ".git"
            ];

            for input in inputs {
                let parsed_url: GitUrl = input.parse()?;
                let translated = parsed_url.to_derived_http_format_url()?;
                assert_eq!(translated, expected_full);
                let translated = parsed_url.to_derived_http_host_url()?;
                assert_eq!(translated, expected_host);
            }
        }

        Ok(())
    }

    #[test]
    fn test_canonicalize_to_http() -> Result<()> {
        for repo_type in [
            "", // empty repo_type default to model
            "models/",
            "datasets/",
            "spaces/",
        ] {
            let expected_full = format!("http://server.co/{repo_type}foo/bar.git");
            let expected_host = "http://server.co";

            let inputs = [
                format!("http://server.co/{repo_type}foo/bar"), // 1. http scheme without suffix ".git"
                format!("http://server.co/{repo_type}foo/bar.git"), // 2. http scheme with suffix ".git"
            ];

            for input in inputs {
                let parsed_url: GitUrl = input.parse()?;
                let translated = parsed_url.to_derived_http_format_url()?;
                assert_eq!(translated, expected_full);
                let translated = parsed_url.to_derived_http_host_url()?;
                assert_eq!(translated, expected_host);
            }
        }

        Ok(())
    }

    #[test]
    fn test_canonicalize_to_https_with_port_number() -> Result<()> {
        for repo_type in [
            "", // empty repo_type default to model
            "models/",
            "datasets/",
            "spaces/",
        ] {
            let expected_full = format!("https://server.co:5564/{repo_type}foo/bar.git");
            let expected_host = "https://server.co:5564";

            let inputs = [
                format!("https://server.co:5564/{repo_type}foo/bar"), // 1. https scheme without suffix ".git"
                format!("https://server.co:5564/{repo_type}foo/bar.git"), // 2. https scheme with suffix ".git"
            ];

            for input in inputs {
                let parsed_url: GitUrl = input.parse()?;
                let translated = parsed_url.to_derived_http_format_url()?;
                assert_eq!(translated, expected_full);
                let translated = parsed_url.to_derived_http_host_url()?;
                assert_eq!(translated, expected_host)
            }
        }

        Ok(())
    }

    #[test]
    fn test_canonicalize_to_http_with_port_number() -> Result<()> {
        for repo_type in [
            "", // empty repo_type default to model
            "models/",
            "datasets/",
            "spaces/",
        ] {
            let expected_full = format!("http://server.co:5564/{repo_type}foo/bar.git");
            let expected_host = "http://server.co:5564";

            let inputs = [
                format!("http://server.co:5564/{repo_type}foo/bar"), // 1. http scheme without suffix ".git"
                format!("http://server.co:5564/{repo_type}foo/bar.git"), // 2. http scheme with suffix ".git"
            ];

            for input in inputs {
                let parsed_url: GitUrl = input.parse()?;
                let translated = parsed_url.to_derived_http_format_url()?;
                assert_eq!(translated, expected_full);
                let translated = parsed_url.to_derived_http_host_url()?;
                assert_eq!(translated, expected_host);
            }
        }

        Ok(())
    }

    #[test]
    fn test_canonicalize_ssh_to_https_with_port_number() -> Result<()> {
        for repo_type in [
            "", // empty repo_type default to model
            "models/",
            "datasets/",
            "spaces/",
        ] {
            let expected_full = format!("https://localhost/{repo_type}foo/bar.git");
            let expected_host = "https://localhost";

            let inputs = [
                format!("ssh://git@localhost:2222/{repo_type}foo/bar"),
                format!("ssh://git@localhost:2222/{repo_type}foo/bar.git"),
            ];

            for input in inputs {
                let parsed_url: GitUrl = input.parse()?;
                assert_eq!(parsed_url.inner.port, Some(2222));
                let translated = parsed_url.to_derived_http_format_url()?;
                assert_eq!(translated, expected_full);
                let translated = parsed_url.to_derived_http_host_url()?;
                assert_eq!(translated, expected_host);
            }
        }
        Ok(())
    }

    #[test]
    fn test_canonicalize_unsupported_to_https_with_port_number() -> Result<()> {
        for repo_type in [
            "", // empty repo_type default to model
            "models/",
            "datasets/",
            "spaces/",
        ] {
            let inputs = [
                format!("file:///{repo_type}foo/bar"),
                format!("ftp://server.co/{repo_type}foo/bar.git"),
            ];

            for input in inputs {
                let parsed_url: GitUrl = input.parse()?;
                let translated = parsed_url.to_derived_http_format_url();
                assert!(matches!(translated, Err(GitXetError::NotSupported(_))));
                let translated = parsed_url.to_derived_http_host_url();
                assert!(matches!(translated, Err(GitXetError::NotSupported(_))));
            }
        }
        Ok(())
    }

    #[test]
    fn test_canonicalize_to_https_with_auth() -> Result<()> {
        for repo_type in [
            "", // empty repo_type default to model
            "models/",
            "datasets/",
            "spaces/",
        ] {
            // auth with only user
            let input = format!("https://user@server.co:235/{repo_type}foo/bar");
            let expected = format!("https://user@server.co:235/{repo_type}foo/bar.git");

            let parsed_url: GitUrl = input.parse()?;
            let translated = parsed_url.to_derived_http_format_url()?;
            assert_eq!(translated, expected);

            // auth with user and token
            let input = format!("https://user:token@server.co:235/{repo_type}foo/bar");
            let expected = format!("https://user:token@server.co:235/{repo_type}foo/bar.git");

            let parsed_url: GitUrl = input.parse()?;
            let translated = parsed_url.to_derived_http_format_url()?;
            assert_eq!(translated, expected);
        }

        Ok(())
    }

    #[test]
    fn test_to_default_lfs_endpoint() -> Result<()> {
        for repo_type in [
            "", // empty repo_type default to model
            "models/",
            "datasets/",
            "spaces/",
        ] {
            let expected = format!("https://git-server.com/{repo_type}foo/bar.git/info/lfs");

            let inputs = [
                format!("https://git-server.com/{repo_type}foo/bar"),
                format!("https://git-server.com/{repo_type}foo/bar.git"),
                format!("git@git-server.com:{repo_type}foo/bar.git"),
                format!("ssh://git-server.com/{repo_type}foo/bar.git"),
            ];

            for input in inputs {
                let parsed_url: GitUrl = input.parse()?;
                let translated = parsed_url.to_default_lfs_endpoint()?;
                assert_eq!(translated, expected);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test_repo_info_extraction {
    use git_url_parse::Scheme;

    use super::HFRepoType::*;
    use super::{GitUrl, RepoInfo};
    use crate::errors::Result;

    #[test]
    fn test_get_scheme() -> Result<()> {
        let inputs = [
            "https://user:token@server.co/fo-o/bar.v2.5",
            "ssh://server.co/models/foo/bar-2.0.git",
            "ssh://git@server.co:2222/models/foo/bar-2.0.git",
            "git@server.co:datasets/foo/_bar",
            "http://user@server.co/spaces/foo/bar-4.8M.git",
        ];

        let expecteds = [Scheme::Https, Scheme::Ssh, Scheme::Ssh, Scheme::Ssh, Scheme::Http];

        for (input, expected) in inputs.iter().zip(expecteds) {
            let parsed_url: GitUrl = input.parse()?;
            assert_eq!(expected, parsed_url.scheme());
        }

        Ok(())
    }

    #[test]
    fn test_get_host_url() -> Result<()> {
        let inputs = [
            "https://user:token@server.co/fo-o/bar.v2.5",
            "ssh://server.co/models/foo/bar-2.0.git",
            "ssh://git@server.co:2222/models/foo/bar-2.0.git",
            "git@server.co:datasets/foo/_bar",
            "http://user@server.co/spaces/foo/bar-4.8M.git",
        ];

        let expecteds = [
            "https://user:token@server.co",
            "ssh://server.co",
            "ssh://git@server.co:2222",
            "git@server.co",
            "http://user@server.co",
        ];

        for (input, expected) in inputs.iter().zip(expecteds) {
            let parsed_url: GitUrl = input.parse()?;
            assert_eq!(expected, parsed_url.host_url()?);
        }

        Ok(())
    }

    #[test]
    fn test_get_full_repo_path() -> Result<()> {
        let inputs = [
            "https://user:token@server.co/fo-o/bar.v2.5",
            "ssh://server.co/models/foo/bar-2.0.git",
            "ssh://git@server.co:2222/models/foo/bar-2.0.git",
            "git@server.co:datasets/foo/_bar",
            "http://user@server.co/spaces/foo/bar-4.8M.git",
        ];

        let expecteds = [
            "fo-o/bar.v2.5",
            "models/foo/bar-2.0",
            "models/foo/bar-2.0",
            "datasets/foo/_bar",
            "spaces/foo/bar-4.8M",
        ];

        for (input, expected) in inputs.iter().zip(expecteds) {
            let parsed_url: GitUrl = input.parse()?;
            assert_eq!(expected, parsed_url.full_repo_path());
        }

        Ok(())
    }

    #[test]
    fn test_get_credential() -> Result<()> {
        let inputs = [
            "https://user:token@server.co/fo-o/bar.v2.5",
            "ssh://server.co/models/foo/bar-2.0.git",
            "ssh://git@server.co:2222/models/foo/bar-2.0.git",
            "git@server.co:datasets/foo/_bar",
            "http://user@server.co/spaces/foo/bar-4.8M.git",
        ];

        let expecteds = [
            (Some("user".to_owned()), Some("token".to_owned())),
            (None, None),
            (Some("git".to_owned()), None),
            (Some("git".to_owned()), None),
            (Some("user".to_owned()), None),
        ];

        for (input, expected) in inputs.iter().zip(expecteds) {
            let parsed_url: GitUrl = input.parse()?;
            assert_eq!(expected, parsed_url.credential());
        }

        Ok(())
    }

    #[test]
    fn test_get_repo_info() -> Result<()> {
        let inputs = [
            "https://user:token@server.co/fo-o/bar.v2.5",
            "ssh://server.co/models/foo/bar-2.0.git",
            "ssh://git@server.co:2222/models/foo/bar-2.0.git",
            "git@server.co:datasets/foo/_bar",
            "http://user@server.co/spaces/foo/bar-4.8M.git",
        ];

        let expecteds = [
            RepoInfo {
                repo_type: Model,
                full_name: "fo-o/bar.v2.5".to_owned(),
            },
            RepoInfo {
                repo_type: Model,
                full_name: "foo/bar-2.0".to_owned(),
            },
            RepoInfo {
                repo_type: Model,
                full_name: "foo/bar-2.0".to_owned(),
            },
            RepoInfo {
                repo_type: Dataset,
                full_name: "foo/_bar".to_owned(),
            },
            RepoInfo {
                repo_type: Space,
                full_name: "foo/bar-4.8M".to_owned(),
            },
        ];

        for (input, expected) in inputs.iter().zip(expecteds) {
            let parsed_url: GitUrl = input.parse()?;
            assert_eq!(expected, parsed_url.repo_info()?);
        }

        Ok(())
    }
}
