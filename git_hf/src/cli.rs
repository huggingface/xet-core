use std::ffi::OsString;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::Result;
use crate::constants::GIT_HF_PROGRAM;

mod install;
mod uninstall;

/// Native Xet V2 Git client
#[derive(Debug, Parser)]
#[command(name = "git-hf", version, propagate_version = true)]
pub struct GitHfApp {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Stage files concurrently without replacing worktree contents with pointers.
    Add(AddArgs),
    /// Explain automatic selection of files for Xet storage.
    #[command(hide = true)]
    Track,
    /// Register the required hf process filter.
    Install(InstallArgs),
    /// Remove hf filter configuration.
    Uninstall(UninstallArgs),
    /// Clone a V2 repository and materialize its Xet files.
    Clone(CloneArgs),
    /// Cache Xet metadata and xorb ranges for a local Git ref, without checkout.
    Fetch(FetchArgs),
    /// Reconstruct indexed Xet files from the local cache, without network access.
    Checkout(CheckoutArgs),
    /// Fetch Xet content for the current checkout and materialize it; does not run git pull.
    Pull(PullArgs),
    /// Internal required Git process filter.
    #[command(hide = true)]
    FilterProcess(FilterArgs),
    /// Internal pre-push hook; reads ref updates from stdin.
    #[command(hide = true)]
    PrePush(PrePushArgs),
}

#[derive(Debug, Args)]
struct AddArgs {
    /// Paths to stage; file selection for Xet storage remains automatic.
    #[arg(required = true)]
    paths: Vec<PathBuf>,
}

#[derive(Debug, Args)]
struct InstallArgs {
    /// Use system configuration.
    #[arg(long, conflicts_with_all = ["local", "global"])]
    system: bool,
    /// Use the current repository's configuration instead of the default global scope.
    #[arg(long, conflicts_with = "global")]
    local: bool,
    /// Use global configuration (the default).
    #[arg(long)]
    global: bool,
}

#[derive(Debug, Args)]
struct UninstallArgs {
    /// Remove configuration from system, global, and current repository scopes.
    #[arg(long, conflicts_with_all = ["system", "local", "global"])]
    all: bool,
    /// Use system configuration.
    #[arg(long, conflicts_with_all = ["local", "global"])]
    system: bool,
    /// Use the current repository's configuration.
    #[arg(long, conflicts_with = "global")]
    local: bool,
    /// Use global configuration (the default).
    #[arg(long)]
    global: bool,
}

#[derive(Debug, Args)]
struct CloneArgs {
    /// Arguments for git clone; Git will validate its options when execution is implemented.
    #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
    git_args: Vec<OsString>,
}

#[derive(Debug, Args)]
struct FetchArgs {
    /// Git remote to use; defaults to the repository's selected remote.
    remote: Option<String>,
    /// Local Git ref or revision whose Xet content should be cached; no Git refs are fetched.
    #[arg(default_value = "HEAD")]
    revision: String,
}

#[derive(Debug, Args)]
struct CheckoutArgs {
    /// Permit replacement of modified tracked regular files.
    #[arg(long)]
    force: bool,
    /// Limit materialization to these tracked paths; defaults to all eligible index entries.
    paths: Vec<PathBuf>,
}

#[derive(Debug, Args)]
struct PullArgs {
    /// Git remote to use; defaults to the repository's selected remote.
    remote: Option<String>,
    /// Permit replacement of modified tracked regular files during checkout.
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Args)]
struct FilterArgs {
    /// Explicitly request identity smudge (the only Phase 1 smudge mode).
    #[arg(long)]
    smudge_passthrough: bool,
}

#[derive(Debug, Args)]
struct PrePushArgs {
    /// Remote name supplied by Git.
    remote: String,
    /// Remote location supplied by Git, including a possible push URL.
    location: OsString,
}

impl GitHfApp {
    /// Parses process arguments, printing Clap help/version or usage errors as appropriate.
    pub fn parse_args() -> Self {
        Self::try_parse_args_from(std::env::args_os()).unwrap_or_else(|error| error.exit())
    }

    /// Parses arguments while preserving the original Git clone argument list.
    pub fn try_parse_args_from(args: impl IntoIterator<Item = OsString>) -> std::result::Result<Self, clap::Error> {
        let args: Vec<_> = args.into_iter().collect();
        let mut app = Self::try_parse_from(&args)?;
        if let Command::Clone(clone_args) = &mut app.command {
            // Clap consumes an initial `--`; retain the original tail for forwarding to Git.
            clone_args.git_args = args.into_iter().skip(2).collect();
        }
        Ok(app)
    }

    /// Executes the selected command.
    pub fn run(self) -> Result<()> {
        match self.command {
            Command::Add(_) => todo!(),
            Command::Track => todo!(),
            Command::Install(args) => install_command(args),
            Command::Uninstall(args) => uninstall_command(args),
            Command::Clone(_) => todo!(),
            Command::Fetch(_) => todo!(),
            Command::Checkout(_) => todo!(),
            Command::Pull(_) => todo!(),
            Command::FilterProcess(_) => todo!(),
            Command::PrePush(_) => todo!(),
        }
    }
}

fn install_command(args: InstallArgs) -> Result<()> {
    if args.system {
        install::system()?;
        println!("{GIT_HF_PROGRAM} installed to system config!");
    } else if args.local {
        install::local()?;
        println!("{GIT_HF_PROGRAM} installed to local repository config!");
    } else {
        install::global()?;
        println!("{GIT_HF_PROGRAM} installed to global config!");
    }
    Ok(())
}

fn uninstall_command(args: UninstallArgs) -> Result<()> {
    if args.all {
        uninstall::all()?;
        println!("{GIT_HF_PROGRAM} uninstalled from all config!");
    } else if args.system {
        uninstall::system()?;
        println!("{GIT_HF_PROGRAM} uninstalled from system config!");
    } else if args.local {
        uninstall::local()?;
        println!("{GIT_HF_PROGRAM} uninstalled from local repository config!");
    } else {
        uninstall::global()?;
        println!("{GIT_HF_PROGRAM} uninstalled from global config!");
    }
    Ok(())
}
