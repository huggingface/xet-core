#### Build Instructions

- Install nightly toolchain and dependencies:
```bash
rustup toolchain install nightly
rustup component add rust-src --toolchain nightly
cargo install wasm-bindgen-cli
```
- Build with `./build_wasm.sh` (bash) 

#### Run Instructions
Serve the web directory using a local http server, for example, https://crates.io/crates/sfz.

- Install sfz:
```bash
cargo install sfz
```

- Serve the web
```bash
sfz --coi -r web
```

- Observe in browser
In browser, go to URL http://127.0.0.1:5000, hit F12 and check the output
under the "Console" tab.