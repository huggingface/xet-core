use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
use tempfile::{TempDir, tempdir};

use crate::git_process_wrapping::run_git_captured;

pub struct TestRepo {
    pub repo_path: PathBuf,
    _tempdir: TempDir,
}

impl TestRepo {
    pub fn new(default_branch: &str) -> Result<Self> {
        let tempdir = tempdir()?;

        let repo_name = "repo";
        let repo_path = std::path::absolute(tempdir.path().join(repo_name))?;
        std::fs::create_dir_all(&repo_path)?;

        run_git_captured(&repo_path, "init", &["-b", default_branch])?;

        Ok(Self {
            repo_path,
            _tempdir: tempdir,
        })
    }

    pub fn clone_from(remote: &Path) -> Result<Self> {
        let tempdir = tempdir()?;

        let repo_name = "repo";
        let repo_path = std::path::absolute(tempdir.path().join(repo_name))?;

        let remote_path_str = remote.to_str().ok_or(anyhow!("bad remote path"))?;
        run_git_captured(tempdir.path(), "clone", &[remote_path_str, repo_name])?;

        Ok(Self {
            repo_path,
            _tempdir: tempdir,
        })
    }

    pub fn new_commit(&self, file_name: &str, data: &[u8], commit_msg: &str) -> Result<()> {
        let file_name = self.repo_path.join(file_name);
        std::fs::write(file_name, data)?;
        run_git_captured(&self.repo_path, "add", &["-A"])?;
        run_git_captured(&self.repo_path, "commit", &["-m", commit_msg])?;

        Ok(())
    }

    pub fn set_remote(&self, name: &str, url: &str) -> Result<()> {
        run_git_captured(&self.repo_path, "remote", &["add", name, url])?;

        Ok(())
    }

    pub fn new_branch_tracking_remote(
        &self,
        remote: &str,
        remote_branch_name: &str,
        local_branch_name: &str,
    ) -> Result<()> {
        run_git_captured(&self.repo_path, "fetch", &[remote, remote_branch_name])?;
        run_git_captured(&self.repo_path, "checkout", &["-b", local_branch_name])?;
        run_git_captured(
            &self.repo_path,
            "branch",
            &[
                &format!("--set-upstream-to={remote}/{remote_branch_name}"),
                local_branch_name,
            ],
        )?;

        Ok(())
    }

    pub fn set_config(&self, key: &str, value: &str) -> Result<()> {
        run_git_captured(&self.repo_path, "config", &["--local", key, value])?;

        Ok(())
    }
}
