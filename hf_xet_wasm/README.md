# hf_xet_wasm: xet-core for WebAssembly

This crate enables functionality to use the xet upload protocol from the browser with the use of a wasm based binary replicating the functionality of the `hf_xet` python library.
Functionality included but not limited to chunking, global deduplication, xorb formation, xorb upload, shard formation, shard upload.

Download functionality is not currently supported.

hf_xet_wasm has: chunking, global deduplication, xorb formation, xorb upload, shard formation, shard upload

hf_xet_wasm is missing: complete download support (xorbs, shards, chunk caching)

## Critical Differences and Changes

In order to compile xet-core to wasm there are numerous changes:

- A version of the data crate that does not assume the presence of any tokio threads
  - there is not yet such a thing as "multiple threads" in WebAssembly (at the time of writing)
  - Additionally only a specific feature set of tokio is supported in WASM, we only use those traits: ["sync", "rt", "macros", "time", "io-util"]
- To support multithreading we use web workers (wasm_thread dependency)
- Any components that use `async_trait` are required to change the `async_trait` proc_macro usage to not dictate `Send`'ness
  - any use of `#[async_trait::async_trait]` becomes:
  - ```rust
    #[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
    #[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
    pub trait Blah {}
    ```
  - this is required as the output from the `async_trait` macro is not compatible to be `Send` when compiled to WASM
  - (pattern adopted from from reqwest_middleware)
- Moves any operations that utilise or rely on the file system to in memory, primarily shard formation and storage
  - We choose not to use on the file system interface provided to browser based applications
- Remove custom dns resolver to HTTP requests
  - HTTP requests in the browser are limited fetch calls made by reqwest.
  - custom dns is not allowed, only HTTP

## Build Instructions

- Install nightly toolchain and dependencies:
```bash
rustup toolchain install nightly
rustup component add rust-src --toolchain nightly
cargo install --version 0.2.100 wasm-bindgen-cli
```
- Build with `./build_wasm.sh` (bash) 

## Run Instructions

The runnable example is composed of a set of files in the examples directory.

First fill up the four `[FILL_ME]` fields in examples/index.html with a desired testing target.

Then serve the web directory using a local http server, for example, https://crates.io/crates/sfz.

- Install sfz:
```bash
cargo install sfz
```

- Serve the web
```bash
sfz --coi -r examples
```

- Observe in browser
In browser, go to URL http://127.0.0.1:5000, hit F12 and check the output
under the "Console" tab.

## Authentication in hf_xet_wasm

Like hf_xet it is the caller's responsibility to set up authentication with the CAS server by getting a token from the huggingface hub.
The caller is also required to provide a method to get a fresh/refreshed token from the hub in the event of token expiration.

In hf_xet_wasm it must be supplied to the XetSession using a user-defined set of interfaces.

```typescript
    class TokenInfo {
        token(): string {
        }
        exp(): bigint {
            return this.exp;
        }
    }

    class TokenRefresher {
        async refreshToken(): TokenInfo {
        }
    }

    const xetSession = new XetSession(<cas-enpdoint>, tokenInfo, tokenRefresher);
```