# hf_xet_wasm_upload

Thin wasm-bindgen wrapper around `xet::xet_session::XetUploadCommit` for
in-browser smoke testing of the Xet upload pipeline.

This crate is **not** the supported wasm interface for production use.
Real consumers should depend on `hf-xet` directly and use
`xet::xet_session::XetSession::new_upload_commit()`.

## Build

```sh
./build_wasm.sh
```

Produces a `pkg/` directory with the ES-module-targeted JS glue and
the wasm binary.

## Manual smoke test

After building, serve `examples/upload.html` from a static server:

```sh
python3 -m http.server 8080
```

Then open `http://localhost:8080/examples/upload.html`.
