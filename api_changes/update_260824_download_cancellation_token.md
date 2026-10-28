# API Update: `download_file_background` takes a cancellation token

## Background

`XetFileDownloadGroup::abort()` returned promptly but the transfer it was meant to stop kept
running to completion, holding bandwidth and then discarding the data (issue #942).

`FileDownloadSession::setup_reconstructor` never called
`FileReconstructor::with_cancellation_token`, so every reconstruction ran with a token the
reconstructor had built for itself and nobody held a handle to. Its cancellation checks could
not fire. The group's `abort()` aborted the wrapper join handle, which drops that one task but
not the work the reconstruction had already spawned, since those are not its children.

## Change

`FileDownloadSession::download_file_background` takes a fourth argument:

```rust
pub async fn download_file_background(
    self: &Arc<Self>,
    file_info: XetFileInfo,
    write_path: PathBuf,
    cancellation_token: Option<CancellationToken>,
) -> Result<(UniqueId, JoinHandle<Result<u64>>)>
```

The token is passed to the reconstructor, so cancelling it stops the term loop from
scheduling further ranges.

## Applying this downstream

Pass `None` to keep the previous behaviour:

```rust
session.download_file_background(file_info, path, None).await?
```

Pass a token if the caller can be aborted. `XetFileDownloadGroup` now creates its per-file
child task runtime before starting the transfer so it can hand over
`task_runtime.cancellation_token()`.

## Behaviour note

A download stopped by its token returns the bytes written so far rather than
`DataError::SizeMismatch`. The size check in `download_file_with_id` is skipped when the token
is cancelled, because a short read is expected in that case and reporting a user abort as a
corrupt download would be misleading.

Private signatures changed alongside it: `setup_reconstructor` and `download_file_with_id`
both take the same `Option<CancellationToken>`. Neither is public.
