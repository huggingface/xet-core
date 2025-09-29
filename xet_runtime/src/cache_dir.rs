use std::env;
use std::env::current_dir;
use std::path::PathBuf;

use dirs::home_dir;
use utils::normalized_path_from_user_string;

// Calculates the cache root path once.
pub fn xet_cache_root() -> PathBuf {
    // if HF_HOME is set use that instead of ~/.cache/huggingface
    // if HF_XET_CACHE is set use that instead of ~/.cache/huggingface/xet
    // HF_XET_CACHE takes precedence over HF_HOME

    // If HF_XET_CACHE is set, use that directly.
    if let Ok(cache) = env::var("HF_XET_CACHE") {
        normalized_path_from_user_string(cache)

    // If HF_HOME is set, use the $HF_HOME/xet
    } else if let Ok(hf_home) = env::var("HF_HOME") {
        normalized_path_from_user_string(hf_home).join("xet")

    // If XDG_CACHE_HOME is set, use the $XDG_CACHE_HOME/huggingface/xet, otherwise
    // use $HOME/.cache/huggingface/xet
    } else if let Ok(xdg_cache_home) = env::var("XDG_CACHE_HOME") {
        normalized_path_from_user_string(xdg_cache_home).join("huggingface").join("xet")

    // Use the same default as huggingface_hub, ~/.cache/huggingface/xet (slightly nonstandard, but won't
    // mess with it).
    } else {
        home_dir()
            .unwrap_or(current_dir().unwrap_or_else(|_| ".".into()))
            .join(".cache")
            .join("huggingface")
            .join("xet")
    }
}
