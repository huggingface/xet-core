use std::str::FromStr;

use git_url_parse::GitUrl as innerGitUrl;
use git_url_parse::Scheme;

use crate::errors::*;

pub struct GitUrl {
    raw: String,
    inner: innerGitUrl,
}

impl FromStr for GitUrl {
    type Err = GitXetError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let raw = s.to_owned();
        let inner = innerGitUrl::parse(s)?;

        Ok(Self { raw, inner })
    }
}

impl GitUrl {
    pub fn as_str(&self) -> &str {
        self.raw.as_str()
    }

    pub fn to_canonical_http_format_url(&self) -> Result<String> {
        let invalid_remote_err = |str: &str| GitXetError::GitConfigError(str.to_owned());

        let scheme = match self.inner.scheme {
            Scheme::Http => "http",
            _ => "https",
        };

        let host = self
            .inner
            .host
            .as_ref()
            .ok_or_else(|| invalid_remote_err("remote URL missing host name"))?;
        let port = self.inner.port;
        let owner = self
            .inner
            .owner
            .as_ref()
            .ok_or_else(|| invalid_remote_err("remote URL missing owner"))?;
        let repo = self.inner.name.as_str();

        let port_str = if let Some(p) = port {
            format!(":{p}")
        } else {
            "".to_owned()
        };

        let canonical_url = format!("{scheme}://{host}{port_str}/{owner}/{repo}.git");

        Ok(canonical_url)
    }

    pub fn to_canonical_http_host_url(&self) -> Result<String> {
        let scheme = match self.inner.scheme {
            Scheme::Http => "http",
            _ => "https",
        };
        let host = self
            .inner
            .host
            .as_ref()
            .ok_or_else(|| GitXetError::GitConfigError("remote URL missing host name".to_owned()))?;
        let port = self.inner.port;

        let port_str = if let Some(p) = port {
            format!(":{p}")
        } else {
            "".to_owned()
        };

        let host_url = format!("{scheme}://{host}{port_str}");

        Ok(host_url)
    }

    pub fn to_default_lfs_endpoint(&self) -> Result<String> {
        let canonical_http_format = self.to_canonical_http_format_url()?;
        Ok(format!("{canonical_http_format}/info/lfs"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::errors::Result;

    #[test]
    fn test_canonicalize_to_https() -> Result<()> {
        let expected_full = "https://server.co/foo/bar.git";
        let expected_host = "https://server.co";

        let inputs = [
            "https://server.co/foo/bar",       // 1. https scheme without suffix ".git"
            "https://server.co/foo/bar.git",   // 2. https scheme with suffix ".git"
            "git@server.co:foo/bar",           // 3. git scheme without suffix ".git"
            "git@server.co:foo/bar.git",       // 4. git scheme with suffix ".git"
            "ssh://server.co/foo/bar",         // 5. ssh scheme without suffix ".git"
            "ssh://server.co/foo/bar.git",     // 6. ssh scheme with suffix ".git"
            "git+ssh://server.co/foo/bar",     // 7. git+ssh scheme without suffix ".git"
            "git+ssh://server.co/foo/bar.git", // 8. git+ssh scheme with suffix ".git"
        ];

        for input in inputs {
            let parsed_url: GitUrl = input.parse()?;
            let translated = parsed_url.to_canonical_http_format_url()?;
            assert_eq!(translated, expected_full);
            let translated = parsed_url.to_canonical_http_host_url()?;
            assert_eq!(translated, expected_host);
        }

        Ok(())
    }

    #[test]
    fn test_canonicalize_to_http() -> Result<()> {
        let expected_full = "http://server.co/foo/bar.git";
        let expected_host = "http://server.co";

        let inputs = [
            "http://server.co/foo/bar",     // 1. http scheme without suffix ".git"
            "http://server.co/foo/bar.git", // 2. http scheme with suffix ".git"
        ];

        for input in inputs {
            let parsed_url: GitUrl = input.parse()?;
            let translated = parsed_url.to_canonical_http_format_url()?;
            assert_eq!(translated, expected_full);
            let translated = parsed_url.to_canonical_http_host_url()?;
            assert_eq!(translated, expected_host);
        }

        Ok(())
    }

    #[test]
    fn test_canonicalize_to_https_with_port_number() -> Result<()> {
        let expected_full = "https://server.co:5564/foo/bar.git";
        let expected_host = "https://server.co:5564";

        let inputs = [
            "https://server.co:5564/foo/bar",     // 1. https scheme without suffix ".git"
            "https://server.co:5564/foo/bar.git", // 2. https scheme with suffix ".git"
        ];

        for input in inputs {
            let parsed_url: GitUrl = input.parse()?;
            let translated = parsed_url.to_canonical_http_format_url()?;
            assert_eq!(translated, expected_full);
            let translated = parsed_url.to_canonical_http_host_url()?;
            assert_eq!(translated, expected_host)
        }

        Ok(())
    }

    #[test]
    fn test_canonicalize_to_http_with_port_number() -> Result<()> {
        let expected_full = "http://server.co:5564/foo/bar.git";
        let expected_host = "http://server.co:5564";

        let inputs = [
            "http://server.co:5564/foo/bar",     // 1. http scheme without suffix ".git"
            "http://server.co:5564/foo/bar.git", // 2. http scheme with suffix ".git"
        ];

        for input in inputs {
            let parsed_url: GitUrl = input.parse()?;
            let translated = parsed_url.to_canonical_http_format_url()?;
            assert_eq!(translated, expected_full);
            let translated = parsed_url.to_canonical_http_host_url()?;
            assert_eq!(translated, expected_host);
        }

        Ok(())
    }

    #[test]
    fn test_to_default_lfs_endpoint() -> Result<()> {
        let expected = "https://git-server.com/foo/bar.git/info/lfs";

        let inputs = [
            "https://git-server.com/foo/bar",
            "https://git-server.com/foo/bar.git",
            "git@git-server.com:foo/bar.git",
            "ssh://git-server.com/foo/bar.git",
        ];

        for input in inputs {
            let parsed_url: GitUrl = input.parse()?;
            let translated = parsed_url.to_default_lfs_endpoint()?;
            assert_eq!(translated, expected);
        }

        Ok(())
    }
}
