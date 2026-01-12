use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use data::configurations::*;
use data::{FileUploadSession, XetFileInfo};
use xet_runtime::XetRuntime;

#[derive(Parser)]
struct XCommand {
    #[clap(subcommand)]
    command: Command,
}

impl XCommand {
    async fn run(&self) -> Result<()> {
        self.command.run().await
    }
}

#[derive(Subcommand)]
enum Command {
    /// Translate a file on disk to a pointer file and upload data to a configured remote.
    Clean(CleanArg),
    /// Hydrate a pointer file to disk.
    Smudge(SmudgeArg),
}

#[derive(Args)]
struct CleanArg {
    /// The file to translate.
    file: PathBuf,
    /// Path to write the pointer file. If not set, will print to stdout.
    #[clap(short, long)]
    dest: Option<PathBuf>,
}

#[derive(Args)]
struct SmudgeArg {
    /// the pointer file to hydrate. If not set, will read from stdin.
    #[clap(short, long)]
    file: Option<PathBuf>,
    /// Path to write the hydrated file.
    dest: PathBuf,
}

impl Command {
    async fn run(&self) -> Result<()> {
        match self {
            Command::Clean(arg) => clean_file(arg).await,
            Command::Smudge(arg) => smudge_file(arg).await,
        }
    }
}

fn get_threadpool() -> Arc<XetRuntime> {
    static THREADPOOL: OnceLock<Arc<XetRuntime>> = OnceLock::new();
    THREADPOOL
        .get_or_init(|| XetRuntime::new().expect("Error starting multithreaded runtime."))
        .clone()
}

fn main() {
    let cli = XCommand::parse();
    let _ = get_threadpool()
        .external_run_async_task(async move { cli.run().await })
        .unwrap();
}

async fn clean_file(arg: &CleanArg) -> Result<()> {
    let file_reader = File::open(&arg.file)?;
    let file_size = file_reader.metadata()?.len();

    let writer: Box<dyn Write + Send> = match &arg.dest {
        Some(path) => Box::new(File::options().create(true).write(true).truncate(true).open(path)?),
        None => Box::new(std::io::stdout()),
    };

    clean(file_reader, writer, file_size).await
}

async fn clean(mut reader: impl Read, mut writer: impl Write, size: u64) -> Result<()> {
    const READ_BLOCK_SIZE: usize = 1024 * 1024;

    let mut read_buf = vec![0u8; READ_BLOCK_SIZE];

    let translator =
        FileUploadSession::new(TranslatorConfig::local_config(std::env::current_dir()?)?.into(), None).await?;

    let mut size_read = 0;
    let mut handle = translator.start_clean(None, size, None).await;

    loop {
        let bytes = reader.read(&mut read_buf)?;
        if bytes == 0 {
            break;
        }

        handle.add_data(&read_buf[0..bytes]).await?;
        size_read += bytes as u64;
    }

    debug_assert_eq!(size_read, size);

    let (file_info, _) = handle.finish().await?;

    translator.finalize().await?;

    writer.write_all(file_info.as_pointer_file()?.as_bytes())?;

    Ok(())
}

async fn smudge_file(arg: &SmudgeArg) -> Result<()> {
    let reader: Box<dyn Read + Send> = match &arg.file {
        Some(path) => Box::new(File::open(path)?),
        None => Box::new(std::io::stdin()),
    };

    smudge(arg.dest.to_string_lossy().into(), reader, arg.dest.clone()).await?;

    Ok(())
}

async fn smudge(name: Arc<str>, mut reader: impl Read, output_path: PathBuf) -> Result<()> {
    use data::configurations::TranslatorConfig;
    use file_reconstruction::DataOutput;

    let mut input = String::new();
    reader.read_to_string(&mut input)?;

    let xet_file: XetFileInfo = serde_json::from_str(&input)
        .map_err(|_| anyhow::anyhow!("Failed to parse xet file info. Please check the format."))?;

    // Use local config pointing to current directory
    let cas_path = std::env::current_dir()?;
    let config = TranslatorConfig::local_config(cas_path)?;
    let downloader = data::FileDownloader::new(config.into()).await?;

    let output = DataOutput::write_in_file(&output_path);

    downloader
        .smudge_file_from_hash(
            &xet_file.merkle_hash().map_err(|_| anyhow::anyhow!("Xet hash is corrupted"))?,
            name,
            output,
            None,
            None,
        )
        .await?;

    Ok(())
}
