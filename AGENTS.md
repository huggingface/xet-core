xet-core is a repo that produces hf-xet Python package and the git-xet CLI tool. It is expected to be used by the huggingface_hub Python package and in conjuction with git-lfs. The code needs to be easy to understand and maintain while also being high-performance and efficient.

* NO TRIVIAL COMMENTS
* Follow Rust idioms and best practices
* Latest Rust features can be used
* Descriptive variable and function names
* No wildcard imports
* Explicit error handling with Result<T, E> over panics
* Use custom error types using thiserror for domain-specific errors
* Format: cargo +nightly fmt
* Lint: cargo clippy --all-features --all-targets -- -D warnings --allow deprecated
* Place unit tests in the same file using #[cfg(test)] modules
* Integration tests go in the tests/ directory
* Add dependencies to Cargo.toml
* Prefer well-maintained crates from crates.io
* Be mindful of allocations in hot paths
* Prefer structured logging
* Provide helpful error messages
* Make sure to add a step to verify that changes made are minimal and required.