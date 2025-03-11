# xet-core-wasm

build command:

```bash
RUSTFLAGS='--cfg getrandom_backend="wasm_js"' wasm-pack build --target web
```

(will need to have previously `cargo install wasm-pack`)