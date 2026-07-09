//! Standalone benchmark harness: download one Xet file to disk via
//! `FileDownloadSession::download_file` (the reconstruct-to-file path, the only
//! path that exercises the `ParallelWriter`).
//!
//! The writer implementation is selected by xet-core from the environment
//! variable `HF_XET_RECONSTRUCT_WRITE_SEQUENTIALLY` (read fresh when the
//! `XetContext` below is constructed), so the caller toggles the writer by
//! setting that env var before launching this process.
//!
//! CAS auth (endpoint URL + JWT + expiration) is passed in on the command line;
//! the driver script mints a fresh read token per run and hands it here.
//!
//! Output: a single machine-parseable line on success, e.g.
//!   RESULT writer=parallel bytes=3502755717 secs=12.3456 mib_per_s=270.55
//! Exit code is non-zero on any failure.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use xet_data::processing::data_client::default_config;
use xet_data::processing::{FileDownloadSession, XetFileInfo};
use xet_runtime::core::XetContext;

const SEQ_ENV: &str = "HF_XET_RECONSTRUCTION_WRITE_SEQUENTIALLY";
const SEQ_ENV_ALIAS: &str = "HF_XET_RECONSTRUCT_WRITE_SEQUENTIALLY";
const MULTI_FD_ENV: &str = "HF_XET_RECONSTRUCTION_WRITE_MULTI_FD";
const MULTI_FD_ENV_ALIAS: &str = "HF_XET_RECONSTRUCT_WRITE_MULTI_FD";

fn arg(args: &[String], key: &str) -> Option<String> {
    args.iter().position(|a| a == key).and_then(|i| args.get(i + 1).cloned())
}

fn required(args: &[String], key: &str) -> Result<String> {
    arg(args, key).with_context(|| format!("missing required argument {key}"))
}

/// True when either spelling of `env`/`alias` is set to a truthy value. Mirrors
/// xet-core's bool parsing loosely, only for the human-facing label.
fn env_flag(env: &str, alias: &str) -> bool {
    let raw = std::env::var(env).ok().or_else(|| std::env::var(alias).ok());
    matches!(
        raw.as_deref().map(str::trim).map(str::to_ascii_lowercase).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

/// Human-facing label for the selected writer. Mirrors the reconstructor's
/// precedence: sequential > multi-fd > parallel (shared-handle positioned writes).
fn writer_label() -> &'static str {
    if env_flag(SEQ_ENV, SEQ_ENV_ALIAS) {
        "sequential"
    } else if env_flag(MULTI_FD_ENV, MULTI_FD_ENV_ALIAS) {
        "multi_fd"
    } else {
        "parallel"
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let cas_url = required(&args, "--cas-url")?;
    let token = required(&args, "--token")?;
    let token_exp: u64 = required(&args, "--token-exp")?
        .parse()
        .context("--token-exp must be a unix-seconds integer")?;
    let hash = required(&args, "--hash")?;
    let size: Option<u64> = arg(&args, "--size")
        .map(|s| s.parse())
        .transpose()
        .context("--size must be an integer")?;
    let out = PathBuf::from(required(&args, "--out")?);

    let writer_label = writer_label();

    let file_info = match size {
        Some(sz) => XetFileInfo::new(hash, sz),
        None => XetFileInfo::new_hash_only(hash),
    };

    // Reads HF_XET_RECONSTRUCT[ION]_WRITE_SEQUENTIALLY from the environment at
    // construction time; no process-global caching, so the value set for this
    // process governs the writer choice for the download below.
    let ctx = XetContext::default().context("create XetContext")?;
    let runtime = ctx.runtime.clone();

    let out_for_task = out.clone();
    let (bytes, secs, write_nanos, write_bytes, write_calls) = runtime
        .bridge_sync(async move {
            let config = default_config(&ctx, cas_url, Some((token, token_exp)), None, None)
                .map_err(|e| anyhow!("default_config failed: {e}"))?;
            let session = FileDownloadSession::new(Arc::new(config), None)
                .await
                .map_err(|e| anyhow!("FileDownloadSession::new failed: {e}"))?;

            // Isolate the write-syscall counters to this single download.
            xet_data::file_reconstruction::write_timing::reset();
            let start = Instant::now();
            let (_id, n_bytes) = session
                .download_file(&file_info, &out_for_task)
                .await
                .map_err(|e| anyhow!("download_file failed: {e}"))?;
            let secs = start.elapsed().as_secs_f64();
            let (write_nanos, write_bytes, write_calls) = xet_data::file_reconstruction::write_timing::snapshot();

            anyhow::Ok((n_bytes, secs, write_nanos, write_bytes, write_calls))
        })
        .context("runtime bridge_sync failed")??;

    // Free disk immediately; the driver runs this many times.
    let _ = std::fs::remove_file(&out);

    if let Some(expected) = size
        && bytes != expected
    {
        bail!("size mismatch: expected {expected}, got {bytes}");
    }

    let mib = bytes as f64 / (1024.0 * 1024.0);
    let mib_per_s = if secs > 0.0 { mib / secs } else { 0.0 };
    // write_secs is the total time in write syscalls; for the parallel writer it
    // is summed across concurrent blocking-pool threads and may exceed `secs`.
    let write_secs = write_nanos as f64 / 1e9;
    let write_mib = write_bytes as f64 / (1024.0 * 1024.0);
    let write_frac = if secs > 0.0 { write_secs / secs } else { 0.0 };
    println!(
        "RESULT writer={writer_label} bytes={bytes} secs={secs:.4} mib_per_s={mib_per_s:.2} \
         write_secs={write_secs:.4} write_frac={write_frac:.4} write_calls={write_calls} write_mib={write_mib:.1}"
    );
    Ok(())
}
