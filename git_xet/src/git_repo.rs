use std::path::PathBuf;
use std::sync::Arc;

use git2::{Config, Repository};

use crate::{errors::*, git_url::GitUrl};

#[derive(Clone)]
pub struct GitRepo {
    repo: Arc<Repository>,
}

impl GitRepo {
    pub fn open_from_cur_dir() -> Result<Self> {
        Self::open(None)
    }

    pub fn open(path: Option<PathBuf>) -> Result<Self> {
        let start_path = match path {
            Some(p) => p,
            None => std::env::current_dir()?,
        };

        let raw_repo = Repository::discover(&start_path).map_err(|e| GitXetError::NoGitRepo {
            path: start_path,
            source: e,
        })?;

        Ok(Self {
            repo: Arc::new(raw_repo),
        })
    }

    // Returns the remote based on:
    // 1. The currently tracked remote branch, if present
    // 2. The value of remote.lfsdefault.
    // 3. Any other SINGLE remote defined in .git/config
    // 4. Use "origin" as a fallback.
    pub fn remote_name(&self) -> Result<String> {
        let maybe_head_ref = self.repo.head();
        let maybe_branch_name = maybe_head_ref.ok().and_then(|head_ref| {
            if head_ref.is_branch() {
                head_ref
                    .name()
                    .and_then(|refs_heads_branch| refs_heads_branch.rsplit('/').next())
                    .map(|branch| branch.to_owned())
            } else {
                None
            }
        });

        let config = self.repo.config()?.snapshot()?;

        // try tracking remote
        if let Some(branch) = maybe_branch_name {
            if let Ok(remote) = config.get_string(&format!("branch.{}.remote", branch)) {
                return Ok(remote);
            }
        }

        // try lfsdefault remote
        if let Ok(remote) = config.get_string("remote.lfsdefault") {
            return Ok(remote);
        }

        // use only remote if there is only 1
        let remotes = self.repo.remotes()?;
        if remotes.len() == 1 {
            if let Some(remote) = remotes.get(0) {
                return Ok(remote.to_string());
            }
        }

        // fall back to default if all above lookup failed,
        // "origin" seems to be the convention
        Ok("origin".to_string())
    }

    // Returns the URL for a specific remote name.
    pub fn remote_name_to_url(&self, remote: &str) -> Result<GitUrl> {
        let url: GitUrl = self
            .repo
            .find_remote(remote)?
            .url()
            .map(|s| s.to_string())
            .ok_or_else(|| GitXetError::GitConfigError(format!("no url for remote \"{remote}\"")))?
            .parse()?;

        Ok(url)
    }

    // Returns a snapshot of the current Git repo config.
    pub fn config(&self) -> Result<Config> {
        Ok(self.repo.config()?.snapshot()?)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{git_repo::GitRepo, test_utils::test_repo::*};

    #[test]
    fn test_get_remote_from_local_config() -> Result<()> {
        let test_repo = TestRepo::new("main")?;

        let repo = GitRepo::open(Some(test_repo.repo_path.clone()))?;

        // test "origin" as fallback
        assert_eq!(repo.remote_name()?, "origin".to_owned());

        // test SINGLE remote if exists
        test_repo.set_remote("upstream", "http://hf.co/foo/bar")?;
        assert_eq!(repo.remote_name()?, "upstream".to_owned());

        // test value of "remote.lfsdefault"
        test_repo.set_config("remote.lfsdefault", "lfsremote")?;
        assert_eq!(repo.remote_name()?, "lfsremote".to_owned());

        Ok(())
    }

    #[test]
    fn test_get_remote_from_repo_tracking() -> Result<()> {
        // set two remote repos
        let remote_repo_1 = TestRepo::new("main")?;
        remote_repo_1.new_commit("data", "hello".as_bytes(), "add new file")?;
        let remote_repo_2 = TestRepo::new("main")?;
        remote_repo_2.new_commit("data", "world".as_bytes(), "add new file")?;

        // set local repo tracking two remotes
        let test_repo = TestRepo::clone_from(&remote_repo_1.repo_path)?;
        test_repo.set_remote("remote2", remote_repo_2.repo_path.as_path().to_str().unwrap())?;

        let repo = GitRepo::open(Some(test_repo.repo_path.clone()))?;

        // test the tracked remote branch
        test_repo.new_branch_tracking_remote("remote2", "main", "featurex")?;
        assert_eq!(repo.remote_name()?, "remote2".to_owned());

        Ok(())
    }

    #[test]
    fn test_get_remote_url() -> Result<()> {
        let test_repo = TestRepo::new("main")?;

        let repo = GitRepo::open(Some(test_repo.repo_path.clone()))?;

        test_repo.set_remote("upstream", "http://hf.co/foo/bar")?;

        let remote_name = repo.remote_name()?;
        let remote_url = repo.remote_name_to_url(&remote_name)?;
        assert_eq!(remote_url.as_str(), "http://hf.co/foo/bar".to_owned());

        Ok(())
    }
}
