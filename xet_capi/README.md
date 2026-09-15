# xet_capi — C API for hf-xet

C ABI bindings for the Hugging Face Xet client (`hf-xet`): upload and download
files stored in Xet-backed Hub repos from C, or any language with a C FFI.

> **Experimental:** the API surface (function signatures, ownership rules,
> struct layouts) may change without notice until it stabilizes.

## Building

```bash
cargo build -p xet_capi --release
```

This produces a self-contained static and shared library
(`target/release/libxet_capi.{a,dylib/so}` or `xet_capi.{lib,dll}` on Windows)
and regenerates the header [`include/hf_xet.h`](include/hf_xet.h) via cbindgen.
CMake consumers can `add_subdirectory` this directory and link the imported
`xet_capi` target (see [`CMakeLists.txt`](CMakeLists.txt)).

## API model

The header is the reference; the conventions in brief:

- **Handles**: every type is an opaque handle, freed with its `xet_*_free`
  function (all accept NULL). A `XetSession` produces `XetUploadCommit` /
  `XetFileDownloadGroup` / `XetDownloadStreamGroup` handles, each carrying its
  own auth from a `XetAuthConfig`.
- **Errors**: fallible functions return `XetStatus` and deliver results via
  `out` parameters. The trailing `XetError **err` may be NULL; on failure it
  receives an error inspected with `xet_error_code` / `xet_error_message`.
- **Blocking**: transfer functions (`xet_file_upload_finalize`,
  `xet_upload_commit_commit`, `xet_file_download_group_finish`,
  `xet_stream_upload_write/finish`, `xet_download_stream_next`) block the
  calling thread until the operation completes. Poll progress from another
  thread via the `xet_*_progress` functions; `xet_*_abort` /
  `xet_download_stream_cancel` may also be called from another thread. No
  callbacks cross the ABI in either direction.

Typical flow: session → commit/group → queue uploads or downloads → blocking
finalize/finish → read report → free everything.

## Examples and docs

- [`examples/`](examples/README.md) — the same upload → commit → download
  round-trip in C, C++, Go, Swift, and Zig, including the progress-thread
  pattern (C) and a header-only C++ RAII wrapper
  ([`examples/cpp/xet_session.hpp`](examples/cpp/xet_session.hpp)).
- `doxygen Doxyfile` renders the header docs to `docs/html/` (not committed).
