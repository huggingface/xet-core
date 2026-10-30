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
dedup work, then cancels and joins remaining xorb uploads on a shared failure, preserving
the original cause rather than waiting for siblings' retries. Successful finalization
checks that every registered xorb finished uploading before publishing shards, in every
build. Logical-byte totals and dependency bookkeeping remain debug assertions; a progress
undercount such as #1053 does not become a release-mode upload failure.

Rust cancellation is cooperative. After `abort()`, callers must drop or drive pending
stream `write()` and `finish()` futures before awaiting `commit()`. Retaining an unpolled
future can keep the pipeline mutex locked, especially with an external runtime. Finish
and abort settle under the same result lock: a finish that already stored metadata stays
in the commit; abort during ingestion prevents a later finish from storing metadata and
that finish returns `UserCancelled`.

`DataError` gains `SharedUploadError(Arc<DataError>)`, a transparent wrapper retaining the
original typed cause for both the cleaner and the session. Downstream exhaustive matches
must handle this variant and recurse into its cause when classifying errors. `XetError`
and telemetry classification already do this, preserving Python exception categories.

Direct `FileUploadSession` users can call the new `abandon_file(id)` after dropping an
unfinished cleaner. Abandonment changes progress bookkeeping only; it does not cancel
shared storage work or clear a session failure. The abandoned attempt stays listed,
complete at its frozen completed-byte count, and remains included in telemetry file
counts. Group progress can therefore exceed the sizes of files in the commit report.
