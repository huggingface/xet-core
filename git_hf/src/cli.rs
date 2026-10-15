use std::ffi::OsString;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::Result;

/// Native Xet V2 Git client
#[derive(Debug, Parser)]
#[command(name = "git-hf", version, propagate_version = true)]
pub struct GitHfApp {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Stage files concurrently without replacing worktree contents with pointers (not yet available).
    Add(AddArgs),
    /// Explain automatic selection of files for Xet storage.
    #[command(hide = true)]
    Track,
    /// Register the required hf process filter (not yet available).
    Install(ConfigArgs),
    /// Remove git-hf-owned configuration (not yet available).
    Uninstall(ConfigArgs),
    /// Clone a V2 repository and materialize its Xet files (not yet available).
    Clone(CloneArgs),
    /// Cache Xet metadata and xorb ranges for a local Git ref, without checkout (not yet available).
    Fetch(FetchArgs),
    /// Reconstruct indexed Xet files from the local cache, without network access (not yet available).
    Checkout(CheckoutArgs),
    /// Fetch Xet content for the current checkout and materialize it; does not run git pull (not yet available).
    Pull(PullArgs),
    /// Internal required Git process filter (not yet available).
    #[command(hide = true)]
    FilterProcess(FilterArgs),
    /// Internal pre-push hook; reads ref updates from stdin (not yet available).
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
struct ConfigArgs {
    /// Use repository-local configuration instead of the default global scope.
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

    /// Executes the selected command (not yet implemented).
    pub fn run(self) -> Result<()> {
        match self.command {
            Command::Add(_) => todo!(),
            Command::Track => todo!(),
            Command::Install(_) => todo!(),
            Command::Uninstall(_) => todo!(),
            Command::Clone(_) => todo!(),
            Command::Fetch(_) => todo!(),
            Command::Checkout(_) => todo!(),
            Command::Pull(_) => todo!(),
            Command::FilterProcess(_) => todo!(),
            Command::PrePush(_) => todo!(),
        }
    }
}
