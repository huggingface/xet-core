use crate::config::default_config;
use utils::auth::TokenRefresher;
use data::errors::DataProcessingError;
use data::{errors, PointerFile, PointerFileTelemetry, PointerFileTranslator};
use parutils::{tokio_par_for_each, ParallelError};
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

/// The maximum git filter protocol packet size
pub const MAX_CONCURRENT_UPLOADS: usize = 8; // TODO
pub const MAX_CONCURRENT_DOWNLOADS: usize = 8; // TODO

const DEFAULT_CAS_ENDPOINT: &str = "http://localhost:8080";
const READ_BLOCK_SIZE: usize = 1024 * 1024;

pub async fn upload_async(
    file_paths: Vec<String>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
) -> errors::Result<Vec<PointerFile>> {
    // chunk files
    // produce Xorbs + Shards
    // upload shards and xorbs
    // for each file, return the filehash
    let config = default_config(
        endpoint.unwrap_or(DEFAULT_CAS_ENDPOINT.to_string()),
        token_info,
        token_refresher,
    )?;

    let processor = Arc::new(PointerFileTranslator::new(config).await?);
    let processor = &processor;
    // for all files, clean them, producing pointer files.
    let pointers = tokio_par_for_each(file_paths, MAX_CONCURRENT_UPLOADS, |f, _| async {
        let proc = processor.clone();
        clean_file(&proc, f).await
    })
    .await
    .map_err(|e| match e {
        ParallelError::JoinError => DataProcessingError::InternalError("Join error".to_string()),
        ParallelError::TaskError(e) => e,
    })?;

    // Push the CAS blocks and flush the mdb to disk
    processor.finalize_cleaning().await?;

    Ok(pointers)
}

pub async fn download_async(
    pointer_files: Vec<PointerFile>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
) -> errors::Result<Vec<PointerFile>> {
    let config = default_config(
        endpoint.unwrap_or(DEFAULT_CAS_ENDPOINT.to_string()),
        token_info,
        token_refresher,
    )?;
    let processor = Arc::new(PointerFileTranslator::new(config).await?);
    let processor = &processor;
    let pfs = tokio_par_for_each(
        pointer_files,
        MAX_CONCURRENT_DOWNLOADS,
        |pointer_file, _| async move {
            let proc = processor.clone();
            smudge_file(&proc, &pointer_file).await
        },
    )
    .await
    .map_err(|e| match e {
        ParallelError::JoinError => DataProcessingError::InternalError("Join error".to_string()),
        ParallelError::TaskError(e) => e,
    })?;

    Ok(pfs)
}

async fn clean_file(processor: &PointerFileTranslator, f: String) -> errors::Result<PointerFile> {
    let mut read_buf = vec![0u8; READ_BLOCK_SIZE];
    let path = PathBuf::from(f);
    let mut reader = BufReader::new(File::open(path.clone())?);

    let handle = processor.start_clean(1024, None).await?;

    loop {
        let bytes = reader.read(&mut read_buf)?;
        if bytes == 0 {
            break;
        }

        handle.add_bytes(read_buf[0..bytes].to_vec()).await?;
    }
    let pf_str = handle.result().await?;
    let pf = PointerFile::init_from_string(&pf_str, path.to_str().unwrap());
    Ok(pf)
}

async fn smudge_file(
    proc: &PointerFileTranslator,
    pointer_file: &PointerFile,
) -> errors::Result<PointerFile> {
    let path = PathBuf::from(pointer_file.path());
    if let Some(parent_dir) = path.parent() {
        fs::create_dir_all(parent_dir)?;
    }
    let mut f: Box<dyn Write + Send> = Box::new(File::create(&path)?);
    proc.smudge_file_from_pointer(pointer_file, &mut f, None)
        .await?;
    let mut pointer_file_clone = pointer_file.clone();
    match pointer_file_clone.telemetry
    {
        None => pointer_file_clone.telemetry =  Some(PointerFileTelemetry {
            latency: Some(proc.start.elapsed()),
            compressed_size: None
        }),
        Some(ref mut telemetry) => telemetry.latency = Some(proc.start.elapsed()),
    }

    Ok(pointer_file_clone)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::current_dir;
    use std::fs::canonicalize;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn upload_files() {
        let cwd = current_dir().unwrap();
        let path = cwd.join("src").join("data_client.rs");

        let abs_path = canonicalize(path).unwrap();
        let s = abs_path.to_string_lossy();
        let files = vec![s.to_string()];
        let pointers = upload_async(files, None, None, None).await.unwrap();
        println!("files: {pointers:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn download_files() {
        let pointers = vec![PointerFile::init_from_info(
            "/tmp/foo.rs",
            "6999733a46030e67f6f020651c91442ace735572458573df599106e54646867c",
            4203,
        )];
        let paths = download_async(pointers, None, None, None).await.unwrap();
        println!("paths: {paths:?}");
    }
}
