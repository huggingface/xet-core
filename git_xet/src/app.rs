use std::path::PathBuf;

use clap::{ArgAction, Args, Parser, Subcommand};
use xet_agent::XetAgent;

use crate::constants::{CURRENT_VERSION, GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM};
use crate::errors::Result;
use crate::lfs_agent_protocol::lfs_protocol_loop;

mod install;
mod xet_agent;

#[derive(Subcommand, Debug)]
#[non_exhaustive]
enum Command {
    /// Install this as a LFS custom transfer agent.
    Install(InstallArg),
    /// Run this as a LFS custom transfer agent.
    Transfer,
}

#[derive(Args, Debug)]
struct InstallArg {
    /// Install in system git configuration file, if not specified global is the default.
    #[clap(long)]
    system: bool,

    /// Install in local git configuration file, if not specified global is the default.
    #[clap(long)]
    local: bool,

    /// The path to the repo. This argument only applies when "--local" is specified.
    #[clap(long)]
    path: Option<PathBuf>,
}

#[derive(Args, Debug)]
struct CliOverrides {
    /// Increase verbosity of output (-v, -vv, etc.)
    #[clap(long, short = 'v', action = ArgAction::Count)]
    pub verbose: u8,

    /// Set the output log file. Writes to stderr if not provided.
    #[clap(long, short)]
    pub log: Option<PathBuf>,
}

#[derive(Parser, Debug)]
#[clap(name=GIT_LFS_CUSTOM_TRANSFER_AGENT_PROGRAM, version = CURRENT_VERSION, propagate_version = true)]
pub struct XetApp {
    #[clap(flatten)]
    overrides: CliOverrides,

    #[clap(subcommand)]
    command: Command,
}

impl XetApp {
    pub async fn run(self) -> Result<()> {
        self.command.run().await
    }
}

impl Command {
    pub async fn run(self) -> Result<()> {
        match self {
            Command::Install(args) => install_command(args),
            Command::Transfer => transfer_command().await,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Command::Install(_) => "install",
            Command::Transfer => "transfer",
        }
    }
}

fn install_command(args: InstallArg) -> Result<()> {
    if args.system as u8 + args.local as u8 > 1 {
        eprintln!("Error: at most one configuration location can be specified.");
        return Ok(());
    }

    if args.system {
        install::system()
    } else if args.local {
        install::local(args.path)
    } else {
        install::global()
    }
}

async fn transfer_command() -> Result<()> {
    let mut agent = XetAgent::default();

    let input = std::io::stdin();
    let output = std::io::stdout();

    lfs_protocol_loop(input.lock(), output, &mut agent).await?;

    Ok(())
}
