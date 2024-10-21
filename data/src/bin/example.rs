use anyhow::Result;
use cas_client::CacheConfig;
use clap::{Args, Parser, Subcommand};
use data::{configurations::*, SMALL_FILE_THRESHOLD};
use data::{PointerFile, PointerFileTranslator};
use std::env::current_dir;
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::{
    fs::File,
    io::{BufReader, BufWriter},
};

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

#[tokio::main]
async fn main() {
    let cli = XCommand::parse();
    cli.run().await.unwrap();
}

fn default_clean_config() -> Result<TranslatorConfig> {
    let path = current_dir()?.join("xet");
    fs::create_dir_all(&path)?;

    let translator_config = TranslatorConfig {
        file_query_policy: Default::default(),
        cas_storage_config: StorageConfig {
            endpoint: Endpoint::FileSystem(path.join("xorbs")),
            auth: None,
            prefix: "default".into(),
            cache_config: Some(CacheConfig {
                cache_directory: path.join("cache"),
                cache_size: 10 * 1024 * 1024 * 1024, // 10 GiB
            }),
            staging_directory: None,
        },
        shard_storage_config: StorageConfig {
            endpoint: Endpoint::FileSystem(path.join("xorbs")),
            auth: None,
            prefix: "default-merkledb".into(),
            cache_config: Some(CacheConfig {
                cache_directory: path.join("shard-cache"),
                cache_size: 0,      // ignored
            }),
            staging_directory: Some(path.join("shard-session")),
        },
        dedup_config: Some(DedupConfig {
            repo_salt: None,
            small_file_threshold: SMALL_FILE_THRESHOLD,
            global_dedup_policy: Default::default(),
        }),
        repo_info: Some(RepoInfo {
            repo_paths: vec!["".into()],
        }),
    };

    translator_config.validate()?;

    Ok(translator_config)
}

fn default_smudge_config() -> Result<TranslatorConfig> {
    let path = current_dir()?.join("xet");
    fs::create_dir_all(&path)?;

    let translator_config = TranslatorConfig {
        file_query_policy: Default::default(),
        cas_storage_config: StorageConfig {
            endpoint: Endpoint::FileSystem(path.join("xorbs")),
            auth: None,
            prefix: "default".into(),
            cache_config: Some(CacheConfig {
                cache_directory: path.join("cache"),
                cache_size: 10 * 1024 * 1024 * 1024, // 10 GiB
            }),
            staging_directory: None,
        },
        shard_storage_config: StorageConfig {
            endpoint: Endpoint::FileSystem(path.join("xorbs")),
            auth: None,
            prefix: "default-merkledb".into(),
            cache_config: Some(CacheConfig {
                cache_directory: path.join("shard-cache"),
                cache_size: 0,      // ignored
            }),
            staging_directory: Some(path.join("shard-session")),
        },
        dedup_config: None,
        repo_info: Some(RepoInfo {
            repo_paths: vec!["".into()],
        }),
    };

    translator_config.validate()?;

    Ok(translator_config)
}

async fn clean_file(arg: &CleanArg) -> Result<()> {
    let reader = BufReader::new(File::open(&arg.file)?);
    let writer: Box<dyn Write> = match &arg.dest {
        Some(path) => Box::new(
            File::options()
                .create(true)
                .write(true)
                .truncate(true)
                .open(path)?,
        ),
        None => Box::new(std::io::stdout()),
    };

    clean(reader, writer).await
}

async fn clean(mut reader: impl Read, mut writer: impl Write) -> Result<()> {
    const READ_BLOCK_SIZE: usize = 1024 * 1024;

    let mut read_buf = vec![0u8; READ_BLOCK_SIZE];

    let translator = PointerFileTranslator::new(default_clean_config()?).await?;

    let handle = translator.start_clean(1024, None).await?;

    loop {
        let bytes = reader.read(&mut read_buf)?;
        if bytes == 0 {
            break;
        }

        handle.add_bytes(read_buf[0..bytes].to_vec()).await?;
    }

    let pointer_file = handle.result().await?;

    translator.finalize_cleaning().await?;

    writer.write_all(pointer_file.as_bytes())?;

    Ok(())
}

async fn smudge_file(arg: &SmudgeArg) -> Result<()> {
    let reader: Box<dyn Read> = match &arg.file {
        Some(path) => Box::new(File::open(path)?),
        None => Box::new(std::io::stdin()),
    };
    let mut writer: Box<dyn Write + Send> = Box::new(BufWriter::new(
        File::options()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&arg.dest)?,
    ));

    smudge(reader, &mut writer).await?;

    writer.flush()?;

    Ok(())
}

async fn smudge(mut reader: impl Read, writer: &mut Box<dyn Write + Send>) -> Result<()> {
    let mut input = String::new();
    reader.read_to_string(&mut input)?;

    let pointer_file = PointerFile::init_from_string(&input, "");

    // not a pointer file, leave it as it is.
    if !pointer_file.is_valid() {
        return Ok(());
    }

    let translator = PointerFileTranslator::new(default_smudge_config()?).await?;

    translator
        .smudge_file_from_pointer(&pointer_file, writer, None)
        .await?;

    Ok(())
}
