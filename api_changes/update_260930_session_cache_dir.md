This update makes the xet cache root configurable per session instead of only through environment variables.

What changed
- Added `data.cache_root: Option<TemplatedPathBuf>` to `XetConfig` (env: `HF_XET_DATA_CACHE_ROOT`). Default `None`.
- Added `XetSessionBuilder::with_cache_dir(dir: impl Into<PathBuf>)` (native only), which sets `data.cache_root`.
- `TranslatorConfig::new` uses `data.cache_root` when set for the shard cache, staging, and memory-session directories; otherwise it falls back to `xet_runtime::core::xet_cache_root()` (`HF_XET_CACHE`, `HF_HOME`, `XDG_CACHE_HOME`, then `~/.cache/huggingface/xet`), unchanged.

Why this matters
- Libraries embedding `hf-xet` (e.g. `hf-hub`) can place the xet cache next to their own configured cache directory without mutating process environment variables.

Usage notes
- `xet_cache_root()` is unchanged and still used for the log directory in `init_logging`, which runs before any `XetContext` exists.
