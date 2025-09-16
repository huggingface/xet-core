use std::path::PathBuf;

use clap::{ArgAction, Args, Parser, Subcommand};
use xet_agent::XetAgent;

use crate::constants::{CURRENT_VERSION, GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM};
use crate::errors::Result;
use crate::lfs_agent_protocol::lfs_protocol_loop;

mod install;
mod uninstall;
mod xet_agent;

#[derive(Subcommand, Debug)]
#[non_exhaustive]
enum Command {
    /// Install this program as a LFS custom transfer agent.
    #[clap(long_about = r#"Perform the following action to register Git-Xet:

Set up the "xet" transfer agent under the name "lfs.customtransfer.xet" in
global Git config with the below values
    path = git-xet
    args = transfer
    concurrent = <according to value from option "--concurrency" (default true)>

If "--concurrency" is specified, set "lfs.concurrenttransfers" in the global
Git config to this number."#)]
    Install(InstallArg),

    /// Remove the custom transfer agent configuration for this program.
    #[clap(long_about = r#"Perform the following action to unregister Git-Xet:

Remove "lfs.customtransfer.xet" entirely from the global Git config.

Remove "lfs.concurrenttransfers" from the global Git config."#)]
    Uninstall(UninstallArg),

    /// Run this program as a LFS custom transfer agent. This is not meant
    /// to be used directly by users, but instead to be invoked by git-lfs.
    Transfer,
}

#[derive(Args, Debug)]
struct InstallArg {
    /// Set the "xet" transfer agent and the number of concurrent LFS transfers in the
    /// system git config, e.g. /etc/gitconfig instead of the global git config (~/.gitconfig).
    #[clap(long)]
    system: bool,

    /// Set the "xet" transfer agent and the number of concurrent LFS transfers in the
    /// local repository's git config, instead of the global git config (~/.gitconfig).
    #[clap(long)]
    local: bool,

    /// The path to the repository. This argument only applies when "--local" is specified.
    #[clap(long)]
    path: Option<PathBuf>,

    /// The number of concurrent LFS uploads/downloads. Default 8.
    #[clap(long)]
    concurrency: Option<u32>,
}

#[derive(Args, Debug)]
struct UninstallArg {
    /// Remove the "xet" transfer agent and the number of concurrent LFS transfers configuration
    /// from all git config locations, including the system, the global and the local repository.
    #[clap(long)]
    all: bool,

    /// Remove the "xet" transfer agent and the number of concurrent LFS transfers configuration
    /// from the system git config, e.g. /etc/gitconfig instead of the global git config (~/.gitconfig).
    #[clap(long)]
    system: bool,

    /// Remove the "xet" transfer agent and the number of concurrent LFS transfers configuration
    /// from the local repository's git config, instead of the global git config (~/.gitconfig).
    #[clap(long)]
    local: bool,

    /// The path to the repository. This argument only applies when "--local" is specified.
    #[clap(long)]
    path: Option<PathBuf>,
}

#[derive(Args, Debug)]
struct CliOverrides {
    /// Increase verbosity of output (-v, -vv, etc.)
    #[clap(long, short = 'v', action = ArgAction::Count)]
    pub verbose: u8,

    /// Set the output log directory. Writes to stderr if not provided.
    #[clap(long, short)]
    pub log: Option<PathBuf>,
}

/// Git-Xet is a Git LFS custom transfer agent that implements upload and download of files using the Xet protocol.
/// To start, run "git-xet install".
///
/// Git-Xet works by registering itself as a custom transfer agent to Git LFS by name "xet". On "git push", "git fetch"
/// or "git pull", git-lfs negotiates with the remote server to determine the transfer agent to use. During this
/// process, git-lfs sends to the server all locally registered agent names in the Batch API request, and the server
/// replies with exactly one agent name in the response. Should "xet" be picked, git-lfs delegates the uploading or
/// downloading operation to Git-Xet through a sequential protocol.
///
/// For more details, see https://github.com/git-lfs/git-lfs/blob/main/docs/api/batch.md and
/// https://github.com/git-lfs/git-lfs/blob/main/docs/custom-transfers.md.
#[derive(Parser, Debug)]
#[clap(name = GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM, version = CURRENT_VERSION, propagate_version = true, verbatim_doc_comment)]
pub struct XetAgentApp {
    #[clap(flatten)]
    overrides: CliOverrides,

    #[clap(subcommand)]
    command: Command,
}

impl XetAgentApp {
    pub async fn run(self) -> Result<()> {
        self.command.run().await
    }
}

impl Command {
    pub async fn run(self) -> Result<()> {
        match self {
            Command::Install(args) => install_command(args),
            Command::Uninstall(args) => uninstall_command(args),
            Command::Transfer => transfer_command().await,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Command::Install(_) => "install",
            Command::Uninstall(_) => "uninstall",
            Command::Transfer => "transfer",
        }
    }
}

fn install_command(args: InstallArg) -> Result<()> {
    if args.system as u8 + args.local as u8 > 1 {
        eprintln!("Error: at most one configuration location can be specified.");
        return Ok(());
    }

    if let Some(c) = args.concurrency
        && c == 0
    {
        eprintln!(r#"Error: "--concurrency" should be a number greater than 0."#);
        return Ok(());
    }

    if args.system {
        install::system(args.concurrency)?;
        println!("{} installed to system config!", GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM);
    } else if args.local {
        install::local(args.path, args.concurrency)?;
        println!("{} installed to local repository config!", GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM);
    } else {
        install::global(args.concurrency)?;
        println!("{} installed to global config!", GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM);
    }

    Ok(())
}

fn uninstall_command(args: UninstallArg) -> Result<()> {
    if args.all as u8 + args.system as u8 + args.local as u8 > 1 {
        eprintln!("Error: at most one configuration location can be specified.");
        return Ok(());
    }

    if args.system {
        uninstall::system()?;
        println!("{} uninstalled from system config!", GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM);
    } else if args.local {
        uninstall::local(args.path)?;
        println!("{} uninstalled from local repository config!", GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM);
    } else if args.all {
        uninstall::all()?;
        println!("{} uninstalled from all config!", GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM);
    } else {
        uninstall::global()?;
        println!("{} uninstalled from global config!", GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM);
    }

    Ok(())
}

async fn transfer_command() -> Result<()> {
    let mut agent = XetAgent::default();

    let input = std::io::stdin();
    let output = std::io::stdout();

    lfs_protocol_loop(input.lock(), output, &mut agent).await?;

    Ok(())
}
