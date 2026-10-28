# Stream aborts and shared upload failures

`XetUploadCommit::commit()` now excludes streams explicitly aborted before a successful
`finish()`, allowing a failed source to be retried with a new stream in the same commit.
Dropping an unfinished handle still fails the commit. `abort()` after a successful
`finish()` is a no-op: the completed file remains included and its status stays completed.
Python context managers and the WASM wrapper follow the same rules.

Aborting a source does not clear failures in shared deduplication or xorb storage work.
Those failures remain owned by `FileUploadSession` and fail finalization before shard
publication, even if the stream that observed them was subsequently aborted. Callers
recovering from an internal storage failure must start a new commit. Finalization drains
dedup and xorb tasks before returning; it also validates completion in release builds.

`DataError` gains `SharedUploadError(Arc<DataError>)`, a transparent wrapper retaining the
original typed cause for both the cleaner and the session. Downstream exhaustive matches
must handle this variant and recurse into its cause when classifying errors. `XetError`
and telemetry classification already do this, preserving Python exception categories.

Direct `FileUploadSession` users can call the new `abandon_file(id)` after dropping an
unfinished cleaner. Abandonment changes progress bookkeeping only; it does not cancel
shared storage work or clear a session failure. The abandoned attempt stays listed,
complete at its frozen completed-byte count, and remains included in telemetry file
counts. Group progress can therefore exceed the sizes of files in the commit report.
