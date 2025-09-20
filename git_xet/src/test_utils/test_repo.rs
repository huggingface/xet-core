#![cfg(test)]
use std::path::{Path, PathBuf};

use anyhow::Result;
use tempfile::TempDir;

use crate::git_process_wrapping::run_git_captured;
use crate::test_utils::TempHome;

// A test utility to create a repo at a temporary directory with a clean global config.
// It allows performing a varies of git operations in this repo.
pub struct TestRepo {
    home: TempHome,
    repo_path: PathBuf,
}

impl TestRepo {
    // Create a repo under a temporary directory with the default branch name set to `default_branch`.
    pub fn new(default_branch: &str) -> Result<Self> {
        let home = TempHome::new()?.with_default_git_config()?;
        let tempdir = TempDir::new_in(home.path())?.keep(); // drop of temp home will clean this up

        let repo_name = "repo";
        let repo_path = std::path::absolute(tempdir.join(repo_name))?;
        std::fs::create_dir_all(&repo_path)?;

        run_git_captured(&repo_path, "init", &["-b", default_branch])?;

        Ok(Self { home, repo_path })
    }

    // Create a repo under a temporary directory by cloning from a `remote` repo.
    pub fn clone_from(remote: &TestRepo) -> Result<Self> {
        let home = remote.home.clone();
        let tempdir = TempDir::new_in(home.path())?.keep(); // drop of temp home will clean this up

        let repo_name = "repo";
        let repo_path = std::path::absolute(tempdir.join(repo_name))?;

        run_git_captured(tempdir, "clone", &[remote.path().as_os_str(), repo_name.as_ref()])?;

        Ok(Self {
            home: remote.home.clone(),
            repo_path,
        })
    }

    // Return the working directory of this repo.
    pub fn path(&self) -> &Path {
        self.repo_path.as_path()
    }

    // Create a new file `file_name` with `data` at the root of this repo's working directory
    // and create a new commit on top of HEAD tracking this file with a commit message.
    pub fn new_commit(&self, file_name: &str, data: &[u8], commit_msg: &str) -> Result<()> {
        let file_name = self.repo_path.join(file_name);
        std::fs::write(file_name, data)?;
        run_git_captured(&self.repo_path, "add", &["-A"])?;
        run_git_captured(&self.repo_path, "commit", &["-m", commit_msg])?;

        Ok(())
    }

    // Add a remote `name`: `url` to this repo.
    pub fn set_remote(&self, name: &str, url: &str) -> Result<()> {
        run_git_captured(&self.repo_path, "remote", &["add", name, url])?;

        Ok(())
    }

    // Create a new branch `new_branch_name` off `base`.
    pub fn new_branch(&self, new_branch_name: &str, base: &str) -> Result<()> {
        run_git_captured(&self.repo_path, "checkout", &[base, "-b", new_branch_name])?;

        Ok(())
    }

    // Run the versatile checkout command.
    pub fn checkout(&self, args: &[&str]) -> Result<()> {
        run_git_captured(&self.repo_path, "checkout", args)?;

        Ok(())
    }

    // Create a new local branch `local_branch_name` off HEAD in this repo that tracks a remote branch
    // `remote`:`remote_branch_name`.
    pub fn new_branch_tracking_remote(
        &self,
        remote: &str,
        remote_branch_name: &str,
        local_branch_name: &str,
    ) -> Result<()> {
        self.new_branch(local_branch_name, "HEAD")?;
        run_git_captured(&self.repo_path, "fetch", &[remote, remote_branch_name])?;
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

    // Add a config `key` = `value` to this repo.
    pub fn set_config(&self, key: &str, value: &str) -> Result<()> {
        run_git_captured(&self.repo_path, "config", &[key, value])?;

        Ok(())
    }
}
