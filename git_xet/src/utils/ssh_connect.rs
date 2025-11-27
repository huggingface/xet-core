// A utility to help establish SSH connection to a remote Git server.

use crate::errors::{GitXetError, Result};
use crate::git_repo::GitRepo;

// The type of SSH program to use for SSH connections, valid values are given
// at https://git-scm.com/docs/git-config#Documentation/git-config.txt-sshvariant.
enum Variant {
    Auto,
    Ssh,
    Simple,
    Putty,
    Tortoise,
}

impl From<&str> for Variant {
    fn from(value: &str) -> Variant {
        match value.to_ascii_lowercase().as_str() {
            "" | "auto" => Variant::Auto,
            "simple" => Variant::Simple,
            "putty" | "plink" => Variant::Putty,
            "tortoiseplink" => Variant::Tortoise,
            _ => Variant::Ssh,
        }
    }
}

pub struct SSHMetadata {
    pub user_and_host: String,
    pub port: Option<u16>,
    pub arg_list: Vec<String>,
}

const DEFAULT_SSH_CMD: &str = "ssh";
const DEFAULT_GIT_BASH: &str = "sh";

// Return the executable name to execute an SSH command on this machine and the base args.
// Format the args as needed if the command needs to run in a shell.
//
// Git allows using a list of environment variables and git configs to control how to establish
// an SSH connection to the remote server.
//
// 1. Env vars $GIT_SSH_COMMAND and $GIT_SSH and git config entry "core.sshCommand" define
// which ssh executable to use for SSH connection. $GIT_SSH_COMMAND takes precedence over "core.sshCommand"
// and both are interpreted by the shell, which allows additional arguments to be included. They both takes
// precedence over $GIT_SSH, which on the other hand must be just the path to a program (which can be a wrapper
// shell script, if additional arguments are needed).
//
// 2. Env var $GIT_SSH_VARIANT takes precedence over git config entry "ssh.variant" and they both define whether
// $GIT_SSH/$GIT_SSH_COMMAND/core.sshCommand refer to OpenSSH, plink/putty or tortoiseplink, or instruct git to
// automatically detect the ssh program type. Valid values are "ssh" (to use OpenSSH options), "plink", "putty",
// "tortoiseplink", "simple" (no options except the host and remote command). The default auto-detection can be
// explicitly requested using the value "auto". Any other value is treated as "ssh".
//
// This implementation follows how the same functionality is handled in
// git-lfs (https://github.com/git-lfs/git-lfs/blob/071e19e8ea03b1e40b181706909fb8c18d928e29/ssh/ssh.go#L41).
pub fn get_sshcmd_and_args(meta: &SSHMetadata, repo: &GitRepo) -> Result<(String, Vec<String>)> {
    let (cmd, args, need_shell) = get_sshexe_and_args(meta, repo)?;

    if !need_shell {
        Ok((cmd, args))
    } else {
        format_for_shell_execution(&cmd, &args)
    }
}

// Return the executable name for ssh on this machine and the base args.
// Base args includes port settings, user/host, everything pre the command to execute.
//
// This implementation follows how the same functionality is handled in
// git (https://github.com/git/git/blob/dc70283dfcdc420d330547fc1d3cba0d29bfd2d0/connect.c#L1367) and
// git-lfs (https://github.com/git-lfs/git-lfs/blob/071e19e8ea03b1e40b181706909fb8c18d928e29/ssh/ssh.go#L127).
pub fn get_sshexe_and_args(meta: &SSHMetadata, repo: &GitRepo) -> Result<(String, Vec<String>, bool)> {
    let repo_config = repo.config()?;

    let sshexe = std::env::var("GIT_SSH").unwrap_or_default();
    let ssh_cmd = std::env::var("GIT_SSH_COMMAND").unwrap_or_default();

    let (mut sshexe, mut cmd, mut need_shell) = parse_shell_command(&ssh_cmd, &sshexe);
    if sshexe.is_empty() {
        let ssh_cmd = repo_config.get_string("core.sshcommand").unwrap_or_default();
        (sshexe, cmd, need_shell) = parse_shell_command(&ssh_cmd, DEFAULT_SSH_CMD);
    }

    let variant = get_ssh_variant(&repo_config, &sshexe);

    if matches!(variant, Variant::Simple) {
        return Err(GitXetError::not_supported(
            "unable to construct an ssh command using an ssh program of \"simple\" variant. Please
            use an advanced ssh program and update environment variables \"GIT_SSH_COMMAND\", \"GIT_SSH\",
            \"GIT_SSH_VARIANT\" and git config entries \"core.sshCommand\" and \"ssh.variant\" accordingly.
            For details, see https://git-scm.com/docs/git-config#Documentation/git-config.txt-sshvariant.
            ",
        ));
    }

    if cmd.is_empty() {
        cmd = sshexe;
    }

    let mut args = Vec::<String>::new();

    if matches!(variant, Variant::Tortoise) {
        // TortoisePlink requires the -batch argument to behave like ssh/plink
        args.push("-batch".into());
    }

    if let Some(p) = meta.port {
        if matches!(variant, Variant::Putty | Variant::Tortoise) {
            args.push("-P".into());
        } else {
            args.push("-p".into());
        }
        args.push(p.to_string());
    }

    args.push(meta.user_and_host.clone());
    args.extend_from_slice(&meta.arg_list);

    Ok((cmd, args, need_shell))
}

// Parse command, and if it looks like a valid command, return the ssh executable
// name, the command to run, and whether we need a shell.  If not, return
// existing as the ssh binary name.
fn parse_shell_command(command: &str, existing: &str) -> (String, String, bool) {
    let parsed_command = shell_words::split(command);
    // Is it a valid command?
    if let Ok(mut p) = parsed_command
        && !p.is_empty()
    {
        // We don't need the rest of the parsed result, so do a quick removal
        // that doesn't preserve the elements order.
        (p.swap_remove(0), command.into(), true)
    } else {
        (existing.into(), "".into(), false)
    }
}

// Find out which type of SSH program is used, this allows constructing the call args accordingly.
// See https://git-scm.com/docs/git-config#Documentation/git-config.txt-sshvariant for details.
fn get_ssh_variant(repo_config: &git2::Config, sshexe: &str) -> Variant {
    let variant_str = std::env::var("GIT_SSH_VARIANT")
        .or_else(|_| repo_config.get_string("ssh.variant"))
        .unwrap_or_default();
    let variant = Variant::from(variant_str.as_str());

    if matches!(variant, Variant::Auto)
        && let Some(base_exe_name) = std::path::Path::new(sshexe).file_stem()
    {
        match base_exe_name.to_ascii_lowercase().to_str() {
            Some("plink") => return Variant::Putty,
            Some("tortoiseplink") => return Variant::Tortoise,
            _ => (),
        }

        Variant::Ssh
    } else {
        variant
    }
}

// Format a shell command and a subsequent list of args to a syntax correct command
// to be executed by a shell.
// Return the executable name and the args to execute this shell command.
fn format_for_shell_execution(command: &str, args: &[String]) -> Result<(String, Vec<String>)> {
    let parsed_command = shell_words::split(command)
        .map_err(|e| GitXetError::config_error(format!("parsing ssh command failed with {e}")))?;
    let complete_shell_command = shell_words::join(parsed_command.iter().chain(args.iter()));

    Ok((DEFAULT_GIT_BASH.into(), vec!["-c".into(), complete_shell_command]))
}

#[cfg(test)]
mod tests {
    use anyhow::{Ok, Result};
    use serial_test::serial;
    use utils::EnvVarGuard;

    use super::*;
    use crate::git_repo::GitRepo;
    use crate::test_utils::TestRepo;

    #[test]
    #[serial(env_var_write_read)]
    fn test_get_ssh_variant_explicit() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let repo = GitRepo::open(test_repo.path())?;

        {
            let _env = EnvVarGuard::set("GIT_SSH_VARIANT", "putty");
            let repo_config = repo.config()?;
            let variant = get_ssh_variant(&repo_config, "/usr/bin/plink");
            assert!(matches!(variant, Variant::Putty));
        }

        {
            let _env = EnvVarGuard::set("GIT_SSH_VARIANT", "putty");
            let repo_config = repo.config()?;
            let variant = get_ssh_variant(&repo_config, r#"C:\Program Files\Putty\plink.exe"#);
            assert!(matches!(variant, Variant::Putty));
        }

        {
            test_repo.set_config("ssh.variant", "ssh")?;
            let repo_config = repo.config()?;
            let variant = get_ssh_variant(&repo_config, "openssh");
            assert!(matches!(variant, Variant::Ssh));
        }

        {
            test_repo.set_config("ssh.variant", "openssh")?;
            let repo_config = repo.config()?;
            let variant = get_ssh_variant(&repo_config, "Openssh.exe");
            assert!(matches!(variant, Variant::Ssh));
        }

        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_get_ssh_variant_explicit_override() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let repo = GitRepo::open(test_repo.path())?;

        {
            // $GIT_SSH_VARIANT (putty) overrides "ssh.variant" (ssh).
            let _env = EnvVarGuard::set("GIT_SSH_VARIANT", "putty");
            test_repo.set_config("ssh.variant", "ssh")?;
            let repo_config = repo.config()?;
            let variant = get_ssh_variant(&repo_config, "/usr/bin/plink");
            assert!(matches!(variant, Variant::Putty));
        }

        {
            // $GIT_SSH_VARIANT (simple) overrides "ssh.variant" (auto).
            let _env = EnvVarGuard::set("GIT_SSH_VARIANT", "simple");
            test_repo.set_config("ssh.variant", "auto")?;
            let repo_config = repo.config()?;
            let variant = get_ssh_variant(&repo_config, r#"C:\sshexe"#);
            assert!(matches!(variant, Variant::Simple));
        }

        Ok(())
    }

    #[test]
    fn test_parse_shell_command() {
        // Test case 1: Empty command string
        let (sshexe, cmd, need_shell) = parse_shell_command("", "default_ssh");
        assert_eq!(sshexe, "default_ssh");
        assert_eq!(cmd, "");
        assert!(!need_shell);

        // Test case 2: Simple command
        let (sshexe, cmd, need_shell) = parse_shell_command("ssh", "default_ssh");
        assert_eq!(sshexe, "ssh");
        assert_eq!(cmd, "ssh");
        assert!(need_shell);

        // Test case 3: Command with arguments
        let (sshexe, cmd, need_shell) = parse_shell_command("ssh -i ~/.ssh/id_rsa", "default_ssh");
        assert_eq!(sshexe, "ssh");
        assert_eq!(cmd, "ssh -i ~/.ssh/id_rsa");
        assert!(need_shell);

        // Test case 4: Command with quoted arguments
        let (sshexe, cmd, need_shell) = parse_shell_command("ssh -o \"StrictHostKeyChecking no\"", "default_ssh");
        assert_eq!(sshexe, "ssh");
        assert_eq!(cmd, "ssh -o \"StrictHostKeyChecking no\"");
        assert!(need_shell);

        // Test case 5: Command with multiple spaces
        let (sshexe, cmd, need_shell) = parse_shell_command("  ssh   -v  ", "default_ssh");
        assert_eq!(sshexe, "ssh");
        assert_eq!(cmd, "  ssh   -v  ");
        assert!(need_shell);

        // Test case 6: Command that is just whitespace
        let (sshexe, cmd, need_shell) = parse_shell_command("   ", "default_ssh");
        assert_eq!(sshexe, "default_ssh");
        assert_eq!(cmd, "");
        assert!(!need_shell);

        // Test case 7: Command with a different executable name
        let (sshexe, cmd, need_shell) = parse_shell_command("/usr/bin/custom_ssh", "ssh");
        assert_eq!(sshexe, "/usr/bin/custom_ssh");
        assert_eq!(cmd, "/usr/bin/custom_ssh");
        assert!(need_shell);
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_get_sshexe_and_args_no_port() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let repo = GitRepo::open(test_repo.path())?;

        let meta = SSHMetadata {
            user_and_host: "git@hf.co".into(),
            port: None,
            arg_list: vec!["auth".into(), "org/repo".into(), "upload".into()],
        };

        // Test with default SSH variant (OpenSSH)
        let (cmd, args, need_shell) = get_sshexe_and_args(&meta, &repo)?;
        assert_eq!(cmd, DEFAULT_SSH_CMD);
        assert_eq!(args, vec!["git@hf.co", "auth", "org/repo", "upload"]);
        assert!(!need_shell);

        // Test with GIT_SSH_COMMAND
        {
            let _env = EnvVarGuard::set("GIT_SSH_COMMAND", "ssh -i ~/.ssh/id_rsa");
            let (cmd, args, need_shell) = get_sshexe_and_args(&meta, &repo)?;
            assert_eq!(cmd, "ssh -i ~/.ssh/id_rsa");
            assert_eq!(args, vec!["git@hf.co", "auth", "org/repo", "upload"]);
            assert!(need_shell);
        }

        // Test with GIT_SSH
        {
            let _env = EnvVarGuard::set("GIT_SSH", "/usr/bin/custom_ssh");
            let (cmd, args, need_shell) = get_sshexe_and_args(&meta, &repo)?;
            assert_eq!(cmd, "/usr/bin/custom_ssh");
            assert_eq!(args, vec!["git@hf.co", "auth", "org/repo", "upload"]);
            assert!(!need_shell);
        }

        // Test with ssh variant
        {
            let _env_ssh = EnvVarGuard::set("GIT_SSH", r#"C:\Program Files\Tortoiseplink.exe"#);
            let _env_variant = EnvVarGuard::set("GIT_SSH_VARIANT", "tortoiseplink");
            let (cmd, args, need_shell) = get_sshexe_and_args(&meta, &repo)?;
            assert_eq!(cmd, r#"C:\Program Files\Tortoiseplink.exe"#);
            assert_eq!(args, vec!["-batch", "git@hf.co", "auth", "org/repo", "upload"]);
            assert!(!need_shell);
        }

        // Test with core.sshCommand
        {
            test_repo.set_config("core.sshCommand", "ssh -v")?;
            let repo = GitRepo::open(test_repo.path())?;
            let (cmd, args, need_shell) = get_sshexe_and_args(&meta, &repo)?;
            assert_eq!(cmd, "ssh -v");
            assert_eq!(args, vec!["git@hf.co", "auth", "org/repo", "upload"]);
            assert!(need_shell);
        }

        // Test with core.sshCommand with quotes
        {
            test_repo.set_config("core.sshCommand", "ssh -o \"StrictHostKeyChecking no\"")?;
            let repo = GitRepo::open(test_repo.path())?;
            let (cmd, args, need_shell) = get_sshexe_and_args(&meta, &repo)?;
            assert_eq!(cmd, "ssh -o \"StrictHostKeyChecking no\"");
            assert_eq!(args, vec!["git@hf.co", "auth", "org/repo", "upload"]);
            assert!(need_shell);
        }

        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_get_sshexe_and_args_with_port() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let repo = GitRepo::open(test_repo.path())?;

        let meta = SSHMetadata {
            user_and_host: "git@hf.co".to_string(),
            port: Some(2222),
            arg_list: vec!["auth".into(), "org/repo".into(), "upload".into()],
        };

        // Test with default SSH variant (OpenSSH)
        let (cmd, args, need_shell) = get_sshexe_and_args(&meta, &repo)?;
        assert_eq!(cmd, DEFAULT_SSH_CMD);
        assert_eq!(args, vec!["-p", "2222", "git@hf.co", "auth", "org/repo", "upload"]);
        assert!(!need_shell);

        // Test with Putty variant
        {
            let _env = EnvVarGuard::set("GIT_SSH_VARIANT", "putty");
            let (cmd, args, need_shell) = get_sshexe_and_args(&meta, &repo)?;
            assert_eq!(cmd, DEFAULT_SSH_CMD);
            assert_eq!(args, vec!["-P", "2222", "git@hf.co", "auth", "org/repo", "upload"]);
            assert!(!need_shell);
        }

        // Test with Tortoise variant
        {
            let _env = EnvVarGuard::set("GIT_SSH_VARIANT", "tortoiseplink");
            let (cmd, args, need_shell) = get_sshexe_and_args(&meta, &repo)?;
            assert_eq!(cmd, DEFAULT_SSH_CMD);
            assert_eq!(args, vec!["-batch", "-P", "2222", "git@hf.co", "auth", "org/repo", "upload"]);
            assert!(!need_shell);
        }

        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_get_sshexe_and_args_simple_variant_error() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let repo = GitRepo::open(test_repo.path())?;

        let meta = SSHMetadata {
            user_and_host: "git@github.com".to_string(),
            port: None,
            arg_list: vec!["auth".into(), "org/repo".into(), "upload".into()],
        };

        // Test with 'simple' variant, which should error
        {
            let _env = EnvVarGuard::set("GIT_SSH_VARIANT", "simple");
            let result = get_sshexe_and_args(&meta, &repo);
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), GitXetError::NotSupported(_)));
        }

        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_get_sshcmd_and_args_no_port() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let repo = GitRepo::open(test_repo.path())?;

        let meta = SSHMetadata {
            user_and_host: "git@hf.co".into(),
            port: None,
            arg_list: vec!["auth".into(), "org/repo".into(), "upload".into()],
        };

        // Test with default SSH variant (OpenSSH)
        let (cmd, args) = get_sshcmd_and_args(&meta, &repo)?;
        assert_eq!(cmd, DEFAULT_SSH_CMD);
        assert_eq!(args, vec!["git@hf.co", "auth", "org/repo", "upload"]);

        // Test with GIT_SSH_COMMAND
        {
            let _env = EnvVarGuard::set("GIT_SSH_COMMAND", "ssh -i ~/.ssh/id_rsa");
            let (cmd, args) = get_sshcmd_and_args(&meta, &repo)?;
            assert_eq!(cmd, "sh");
            assert_eq!(args, vec!["-c", "ssh -i ~/.ssh/id_rsa git@hf.co auth org/repo upload"]);
        }

        // Test with GIT_SSH
        {
            let _env = EnvVarGuard::set("GIT_SSH", "/usr/bin/custom_ssh");
            let (cmd, args) = get_sshcmd_and_args(&meta, &repo)?;
            assert_eq!(cmd, "/usr/bin/custom_ssh");
            assert_eq!(args, vec!["git@hf.co", "auth", "org/repo", "upload"]);
        }

        // Test with ssh variant
        {
            let _env_ssh = EnvVarGuard::set("GIT_SSH", r#"C:\Program Files\Tortoiseplink.exe"#);
            let _env_variant = EnvVarGuard::set("GIT_SSH_VARIANT", "tortoiseplink");
            let (cmd, args) = get_sshcmd_and_args(&meta, &repo)?;
            assert_eq!(cmd, r#"C:\Program Files\Tortoiseplink.exe"#);
            assert_eq!(args, vec!["-batch", "git@hf.co", "auth", "org/repo", "upload"]);
        }

        // Test with core.sshCommand
        {
            test_repo.set_config("core.sshCommand", "ssh -v")?;
            let repo = GitRepo::open(test_repo.path())?;
            let (cmd, args) = get_sshcmd_and_args(&meta, &repo)?;
            assert_eq!(cmd, "sh");
            assert_eq!(args, vec!["-c", "ssh -v git@hf.co auth org/repo upload"]);
        }

        // Test with core.sshCommand with quotes
        {
            test_repo.set_config("core.sshCommand", "ssh -o \"StrictHostKeyChecking no\"")?;
            let repo = GitRepo::open(test_repo.path())?;
            let (cmd, args) = get_sshcmd_and_args(&meta, &repo)?;
            assert_eq!(cmd, "sh");
            assert_eq!(args, vec!["-c", "ssh -o 'StrictHostKeyChecking no' git@hf.co auth org/repo upload"]);
        }

        Ok(())
    }

    #[test]
    #[serial(env_var_write_read)]
    fn test_get_sshcmd_and_args_with_port() -> Result<()> {
        let test_repo = TestRepo::new("main")?;
        let repo = GitRepo::open(test_repo.path())?;

        let meta = SSHMetadata {
            user_and_host: "git@hf.co".to_string(),
            port: Some(2222),
            arg_list: vec!["auth".into(), "org/repo".into(), "upload".into()],
        };

        // Test with default SSH variant (OpenSSH)
        let (cmd, args) = get_sshcmd_and_args(&meta, &repo)?;
        assert_eq!(cmd, DEFAULT_SSH_CMD);
        assert_eq!(args, vec!["-p", "2222", "git@hf.co", "auth", "org/repo", "upload"]);

        // Test with Putty variant
        {
            let _env = EnvVarGuard::set("GIT_SSH_VARIANT", "putty");
            let (cmd, args) = get_sshcmd_and_args(&meta, &repo)?;
            assert_eq!(cmd, DEFAULT_SSH_CMD);
            assert_eq!(args, vec!["-P", "2222", "git@hf.co", "auth", "org/repo", "upload"]);
        }

        // Test with Tortoise variant
        {
            let _env = EnvVarGuard::set("GIT_SSH_VARIANT", "tortoiseplink");
            let (cmd, args) = get_sshcmd_and_args(&meta, &repo)?;
            assert_eq!(cmd, DEFAULT_SSH_CMD);
            assert_eq!(args, vec!["-batch", "-P", "2222", "git@hf.co", "auth", "org/repo", "upload"]);
        }

        Ok(())
    }
}
