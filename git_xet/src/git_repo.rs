use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use git2::{Config, Repository};

use crate::errors::{GitXetError, Result};
use crate::git_url::GitUrl;

#[derive(Clone)]
pub struct GitRepo {
    // Repository does not impl Sync, so we leverage Mutex
    // to give it the Sync capability.
    repo: Arc<Mutex<Repository>>,
}

impl GitRepo {
    pub fn open_from_cur_dir() -> Result<Self> {
        Self::open(std::env::current_dir()?)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let start_path = path.as_ref();

        let raw_repo = Repository::discover(start_path).map_err(|e| GitXetError::NoGitRepo {
            path: start_path.to_path_buf(),
            source: e,
        })?;

        Ok(Self {
            repo: Arc::new(Mutex::new(raw_repo)),
        })
    }

    // Returns the path to the .git folder for normal repositories or the repository itself for bare repositories.
    pub fn git_path(&self) -> Result<PathBuf> {
        let repo = self.repo.lock().map_err(GitXetError::internal)?;
        Ok(repo.path().to_path_buf())
    }

    // Resolves the reference pointed at by HEAD, returns branch name if it is a branch.
    pub fn branch_name(&self) -> Result<Option<String>> {
        let repo = self.repo.lock().map_err(GitXetError::internal)?;

        let maybe_head_ref = repo.head();
        Ok(maybe_head_ref.ok().and_then(|head_ref| {
            if head_ref.is_branch() {
                head_ref
                    .name()
                    .and_then(|refs_heads_branch| refs_heads_branch.strip_prefix("refs/heads/"))
                    .map(|branch| branch.to_owned())
            } else {
                None
            }
        }))
    }

    // Returns the remote that a git push/fetch/pull operation
    // is targeted at, based on:
    // 1. The currently tracked remote branch, if present
    // 2. The value of remote.lfsdefault.
    // 3. Any other SINGLE remote defined in .git/config
    // 4. Use "origin" as a fallback.
    pub fn remote_name(&self) -> Result<String> {
        let maybe_branch_name = self.branch_name()?;

        let repo = self.repo.lock().map_err(GitXetError::internal)?;
        let config = repo.config()?.snapshot()?;

        // try tracking remote
        if let Some(branch) = maybe_branch_name
            && let Ok(remote) = config.get_string(&format!("branch.{}.remote", branch))
        {
            return Ok(remote);
        }

        // try lfsdefault remote
        if let Ok(remote) = config.get_string("remote.lfsdefault") {
            return Ok(remote);
        }

        // use only remote if there is only 1
        let remotes = repo.remotes()?;
        if remotes.len() == 1
            && let Some(remote) = remotes.get(0)
        {
            return Ok(remote.to_string());
        }

        // fall back to default if all above lookup failed,
        // "origin" seems to be the convention
        Ok("origin".to_string())
    }

    // Returns the URL for a specific remote name.
    pub fn remote_name_to_url(&self, remote: &str) -> Result<GitUrl> {
        let repo = self.repo.lock().map_err(GitXetError::internal)?;

        let url: GitUrl = repo
            .find_remote(remote)?
            .url()
            .map(|s| s.to_string())
            .ok_or_else(|| GitXetError::config_error(format!("no url for remote \"{remote}\"")))?
            .parse()?;

        Ok(url)
    }

    // A convenient function to get the remote URL that a git push/fetch/pull operation
    // is targeted at. This combines the functionality of `remote_name` and `remote_name_to_url`.
    pub fn remote_url(&self) -> Result<GitUrl> {
        let remote = self.remote_name()?;
        self.remote_name_to_url(&remote)
    }

    // Returns a snapshot of the current Git repo config.
    pub fn config(&self) -> Result<Config> {
        let repo = self.repo.lock().map_err(GitXetError::internal)?;

        Ok(repo.config()?.snapshot()?)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use serial_test::serial;

    use crate::git_repo::GitRepo;
    use crate::test_utils::TestRepo;

    #[test]
    #[serial(env_var_write_read)]
    fn test_get_ref_name() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let repo = GitRepo::open(test_repo.path())?;

        test_repo.new_commit("data", "hello".as_bytes(), "add new file")?;
        assert_eq!(repo.branch_name()?, Some("main".to_owned()));

        test_repo.new_branch("pr/1", "main")?;
        test_repo.new_commit("data", "world".as_bytes(), "update file")?;
        assert_eq!(repo.branch_name()?, Some("pr/1".to_owned()));

        test_repo.checkout(&["HEAD^"])?;
        assert_eq!(repo.branch_name()?, None);

        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_get_remote_from_local_config() -> Result<()> {
        let test_repo = TestRepo::new("main")?;

        let repo = GitRepo::open(test_repo.path())?;

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
    #[serial(env_var_write_read)]
    fn test_get_remote_from_repo_tracking() -> Result<()> {
        // set two remote repos
        let remote_repo_1 = TestRepo::new("main")?;
        remote_repo_1.new_commit("data", "hello".as_bytes(), "add new file")?;
        let remote_repo_2 = TestRepo::new("main")?;
        remote_repo_2.new_commit("data", "world".as_bytes(), "add new file")?;

        // set local repo tracking two remotes
        let test_repo = TestRepo::clone_from(&remote_repo_1)?;
        test_repo.set_remote("remote2", remote_repo_2.path().to_str().unwrap())?;

        let repo = GitRepo::open(test_repo.path())?;

        // test the tracked remote branch
        test_repo.new_branch_tracking_remote("remote2", "main", "featurex")?;
        assert_eq!(repo.remote_name()?, "remote2".to_owned());

        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_get_remote_url() -> Result<()> {
        let test_repo = TestRepo::new("main")?;

        let repo = GitRepo::open(test_repo.path())?;

        test_repo.set_remote("upstream", "http://hf.co/foo/bar")?;

        let remote_name = repo.remote_name()?;
        let remote_url = repo.remote_name_to_url(&remote_name)?;
        assert_eq!(remote_url.as_str(), "http://hf.co/foo/bar".to_owned());

        Ok(())
    }
}
