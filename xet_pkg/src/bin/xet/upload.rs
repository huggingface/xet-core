use std::io::Read;
use std::path::PathBuf;

use anyhow::Result;
use clap::Args;
use xet::xet_session::{XetFileMetadata, XetSession};
use xet_data::processing::Sha256Policy;
use xet_runtime::config::XetConfig;

use super::Cli;

#[derive(Args)]
pub struct UploadArgs {
    /// Files to upload. Use "-" to read from stdin.
    pub files: Vec<String>,

    /// Skip SHA-256 hash computation during upload.
    #[arg(long)]
    pub no_sha256: bool,

    /// Print deduplication/compression statistics after upload.
    #[arg(long)]
    pub dump_stats: bool,

    /// Write JSON results to this file instead of stdout.
    #[arg(long)]
    pub output: Option<PathBuf>,
}

pub async fn run(cli: &Cli, config: XetConfig, args: &UploadArgs) -> Result<()> {
    let session = super::session::build_xet_session(&cli.resolved_endpoint(), cli.resolved_token(), config)?;
    let results = run_upload(&session, args).await?;

    if let Some(ref output_path) = args.output {
        let json = serde_json::to_string_pretty(&results)?;
        std::fs::write(output_path, json)?;
    } else if !cli.quiet {
        for meta in &results {
            eprintln!(
                "{}  hash={}  size={}  sha256={}",
                meta.tracking_name.as_deref().unwrap_or("<stdin>"),
                meta.xet_info.hash,
                meta.xet_info.file_size.unwrap_or(0),
                meta.xet_info.sha256.as_deref().unwrap_or("-")
            );
        }
    }
    Ok(())
}

pub async fn run_upload(session: &XetSession, args: &UploadArgs) -> Result<Vec<XetFileMetadata>> {
    let sha256 = if args.no_sha256 {
        Sha256Policy::Skip
    } else {
        Sha256Policy::Compute
    };

    let commit = session.new_upload_commit().await?;
    let mut handles = vec![];

    for file_arg in &args.files {
        if file_arg == "-" {
            let mut data = Vec::new();
            std::io::stdin().read_to_end(&mut data)?;
            let handle = commit.upload_bytes(data, sha256, Some("<stdin>".into())).await?;
            handles.push(("<stdin>".to_owned(), handle));
        } else {
            let path = PathBuf::from(file_arg);
            let handle = commit.upload_from_path(path.clone(), sha256).await?;
            handles.push((file_arg.clone(), handle));
        }
    }

    let mut output = vec![];
    let mut had_error = false;

    for (name, handle) in &handles {
        match handle.finalize_ingestion().await {
            Ok(meta) => {
                if args.dump_stats {
                    eprintln!(
                        "[stats] {}  size={}  hash={}",
                        meta.tracking_name.as_deref().unwrap_or(name),
                        meta.xet_info.file_size.unwrap_or(0),
                        meta.xet_info.hash
                    );
                }
                output.push(meta);
            },
            Err(e) => {
                eprintln!("ERROR: {name}: {e}");
                had_error = true;
            },
        }
    }

    if had_error {
        commit.abort()?;
        anyhow::bail!("one or more files failed to upload");
    }
    commit.commit().await?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::session::build_xet_session;

    fn local_session(cas_dir: &tempfile::TempDir) -> (String, XetSession) {
        let endpoint = format!("local://{}", cas_dir.path().display());
        let session = build_xet_session(&endpoint, None, XetConfig::new()).unwrap();
        (endpoint, session)
    }

    #[tokio::test]
    async fn test_upload_single_file() {
        let cas_dir = tempdir().unwrap();
        let (_endpoint, session) = local_session(&cas_dir);

        let src_dir = tempdir().unwrap();
        let src = src_dir.path().join("hello.txt");
        std::fs::write(&src, b"hello xet world").unwrap();

        let args = UploadArgs {
            files: vec![src.to_str().unwrap().to_owned()],
            no_sha256: false,
            dump_stats: false,
            output: None,
        };
        let results = run_upload(&session, &args).await.unwrap();

        assert_eq!(results.len(), 1);
        let meta = &results[0];
        assert_eq!(meta.xet_info.file_size, Some(15));
        assert!(!meta.xet_info.hash.is_empty());
        assert!(meta.xet_info.sha256.is_some());
    }

    #[tokio::test]
    async fn test_upload_multiple_files() {
        let cas_dir = tempdir().unwrap();
        let (_endpoint, session) = local_session(&cas_dir);

        let src_dir = tempdir().unwrap();
        let files: Vec<String> = (0..5)
            .map(|i| {
                let path = src_dir.path().join(format!("file_{i}.bin"));
                std::fs::write(&path, format!("content for file {i}").as_bytes()).unwrap();
                path.to_str().unwrap().to_owned()
            })
            .collect();

        let args = UploadArgs {
            files,
            no_sha256: true,
            dump_stats: false,
            output: None,
        };
        let results = run_upload(&session, &args).await.unwrap();

        assert_eq!(results.len(), 5);
        for (i, meta) in results.iter().enumerate() {
            assert_eq!(meta.xet_info.file_size, Some(format!("content for file {i}").len() as u64));
            assert!(!meta.xet_info.hash.is_empty());
        }
    }

    #[tokio::test]
    async fn test_upload_sha256_policy() {
        let cas_dir = tempdir().unwrap();
        let src_dir = tempdir().unwrap();
        let src = src_dir.path().join("data.bin");
        std::fs::write(&src, b"sha256 test data").unwrap();
        let src_str = src.to_str().unwrap().to_owned();

        let (_, session) = local_session(&cas_dir);
        let args = UploadArgs {
            files: vec![src_str.clone()],
            no_sha256: false,
            dump_stats: false,
            output: None,
        };
        let with_sha = run_upload(&session, &args).await.unwrap();
        assert!(with_sha[0].xet_info.sha256.is_some());

        let cas_dir2 = tempdir().unwrap();
        let (_, session2) = local_session(&cas_dir2);
        let args = UploadArgs {
            files: vec![src_str],
            no_sha256: true,
            dump_stats: false,
            output: None,
        };
        let without_sha = run_upload(&session2, &args).await.unwrap();
        assert!(without_sha[0].xet_info.sha256.is_none());
    }

    #[tokio::test]
    async fn test_upload_json_output() {
        let cas_dir = tempdir().unwrap();
        let (_endpoint, session) = local_session(&cas_dir);

        let src_dir = tempdir().unwrap();
        let src = src_dir.path().join("json_test.txt");
        std::fs::write(&src, b"json output test").unwrap();

        let out_dir = tempdir().unwrap();
        let json_path = out_dir.path().join("results.json");

        let args = UploadArgs {
            files: vec![src.to_str().unwrap().to_owned()],
            no_sha256: false,
            dump_stats: false,
            output: Some(json_path.clone()),
        };
        let results = run_upload(&session, &args).await.unwrap();
        let json = serde_json::to_string_pretty(&results).unwrap();
        std::fs::write(&json_path, &json).unwrap();

        let parsed: Vec<XetFileMetadata> = serde_json::from_str(&std::fs::read_to_string(&json_path).unwrap()).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].xet_info.file_size, Some(16));
        assert!(!parsed[0].xet_info.hash.is_empty());
    }
}
