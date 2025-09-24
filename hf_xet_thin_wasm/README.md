# hf_xet_thin_wasm

Exports limited functionality from xet-core in a WebAssembly compile-able/compatible way for use primarily by [huggingface.js](https://github.com/huggingface/huggingface.js).

Exports:

- Xorb hash computation
- File hash computation
- Verification range hash computation
- Chunker struct/class
  - Generate chunk boundaries
  - Compute chunk hashes
