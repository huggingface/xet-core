# Development Notes

* `pip install maturin`
* from this directory: `maturin develop`
* for release build `maturin develop -r`

## Developing with tokio console

> Prerequisite is installing tokio-console (`cargo instal tokio-console`). See [https://github.com/tokio-rs/console](https://github.com/tokio-rs/console)

To use tokio-console with hf-xet there are compile hf_xet with the following command:
```sh
RUSTFLAGS="--cfg tokio_unstable" maturin develop -r --features tokio-console
```

Then while hf_xet is running (via a `hf` cli command or `huggingface_hub` python code), `tokio-console` will be able to connect.

### Ex.

```bash
# In one terminal:
pip install huggingface_hub
RUSTFLAGS="--cfg tokio_unstable" maturin develop -r --features tokio-console
hf download openai/gpt-oss-20b

# In another terminal
cargo install tokio-console
tokio-console
```
