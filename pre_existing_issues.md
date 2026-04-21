# Pre-existing Issues Found During PR Review

These issues exist on `origin/main` and were not introduced by the XetContext/XetRuntime split PR.
They should be fixed in a separate PR.

---

## 1. `test_multiple_resume` is flaky — redb database lock conflict

**Location:** `xet_data/tests/test_session_resume.rs:115`

**Symptom:** The test panics intermittently with:
```
called `Result::unwrap()` on an `Err` value: ClientError(Other("Error opening redb database: Database already open. Cannot acquire lock."))
```

**Root cause:** The test creates `FileUploadSession` objects in a `for` loop (lines 114–135).
Each session opens a redb database.  The session from the previous iteration is dropped at the
end of the loop body, but the drop may not fully release the database lock before the next
iteration calls `FileUploadSession::new` and tries to open the same database.  This is a
race between the async runtime tearing down the previous session's resources and the new
session attempting to acquire the lock.

**Suggested fix:** Either:
- Explicitly await or drop the previous session and add a small yield/barrier before creating the next one, or
- Wrap the session in a block scope and ensure the database is fully closed before proceeding, or
- Use `#[tokio::test]` with `serial_test` to avoid parallel execution with other tests that may share the same temp directory.

**Severity:** Medium — causes intermittent CI failures.

---

## 2. `Display` for `XetRuntime` references stale method name

**Location:** `xet_runtime/src/core/runtime.rs:701`

**Current code:**
```rust
return write!(f, "Terminated Tokio Runtime Handle; cancel_all_and_shutdown called.");
```

**Issue:** The method `cancel_all_and_shutdown` no longer exists.  The shutdown method is
`perform_sigint_shutdown`.  The display string should be updated to match.

**Suggested fix:** Change the message to reference the current method name, e.g.:
```rust
return write!(f, "Terminated Tokio Runtime Handle; runtime has been shut down.");
```

**Severity:** Low — cosmetic, only affects debug display output.

---

## 3. `ShardFileManager` doc comment has stale API names

**Location:** `xet_core_structures/src/metadata_shard/shard_file_manager.rs:91–107`

**Current code:**
```rust
/// Usage:
///
/// // Session directory is where it stores shard and shard state.
/// let mut mng = ShardFileManager::new("<session_directory>")
///
/// // Add other known shards with register_shards.
/// mng.register_shards(&[other shard, directories, etc.])?;
///
/// // Run queries, add data, etc. with get_file_reconstruction_info, chunk_hash_dedup_query,
/// add_xorb_block, add_file_reconstruction_info.
///
/// // Finalize by calling process_session_directory
/// let new_shards = mdb.process_session_directory()?;
```

**Issue:** The example uses `ShardFileManager::new` (actual constructor is `new_in_session_directory`
or `new_in_cache_directory`) and calls `process_session_directory` (not a current method name).
The variable name also changes from `mng` to `mdb` mid-example.

**Suggested fix:** Update the usage example to use the actual constructor and finalization method
names (`new_in_session_directory`, `flush`, `consolidate_shards_in_directory`, etc.).

**Severity:** Low — documentation only.

---

## 4. `MDBShardFile` has a truncated doc comment

**Location:** `xet_core_structures/src/metadata_shard/shard_file_handle.rs:22`

**Current code:**
```rust
/// When a specific implementation of the  
#[derive(Debug)]
pub struct MDBShardFile {
```

**Issue:** The doc comment is incomplete — it ends mid-sentence with trailing whitespace.

**Suggested fix:** Either complete the sentence or remove the partial comment.

**Severity:** Low — documentation only.

---

## 5. `git_xet` function `xet_runtime()` has misleading name

**Location:** `git_xet/src/app/xet_agent.rs:19–22`

**Current code:**
```rust
fn xet_runtime() -> &'static XetContext {
    static RUNTIME: OnceLock<XetContext> = OnceLock::new();
    RUNTIME.get_or_init(|| XetContext::default().expect("xet context"))
}
```

**Issue:** The function returns `&XetContext` but is named `xet_runtime()`, and the static
is named `RUNTIME`.  This is misleading now that `XetRuntime` and `XetContext` are distinct types.

**Suggested fix:** Rename to `xet_context()` with `static CONTEXT: OnceLock<XetContext>`.

**Severity:** Low — naming clarity only, no functional issue.

---

## 6. Typos in `shard_file_handle.rs` comments

**Location:** `xet_core_structures/src/metadata_shard/shard_file_handle.rs`

- ~line 30: "On **occation**" → "On **occasion**"
- ~line 310: "**Registerd**" → "**Registered**"
- ~line 375: "In the **xorbe** of" → "In the **xorb** of" (if that's the intended word)

**Severity:** Low — typos in comments.
