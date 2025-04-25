#### Build Instructions

- Install nightly toolchain and dependencies:
```bash
rustup toolchain install nightly
rustup component add rust-src --toolchain nightly
cargo install wasm-bindgen-cli
```
- Build with `./build_wasm.sh` (bash) 