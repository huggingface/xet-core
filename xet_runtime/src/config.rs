use std::cell::RefCell;
use std::sync::Arc;

use xet_config::XetConfig;

use crate::runtime::XetRuntime;

// Use thread-local references to the config that caches access.  This way, xet_config() will
// can be called outside of an existing runtime.
thread_local! {
    static THREAD_CONFIG_REF: RefCell<Option<Arc<XetConfig>>> = const { RefCell::new(None) };
}

pub fn xet_config() -> Arc<XetConfig> {
    if let Some(config) = THREAD_CONFIG_REF.with_borrow(|config| config.clone()) {
        return config;
    }

    let config = {
        if let Some(runtime) = XetRuntime::current_if_exists() {
            runtime.config().clone()
        } else {
            Arc::new(XetConfig::new())
        }
    };

    THREAD_CONFIG_REF.set(Some(config.clone()));

    config
}

/// Reset the thread-local config cache for testing purposes.
/// This forces the next call to `xet_config()` to reload from environment variables.
pub fn reset_xet_config_for_test() {
    THREAD_CONFIG_REF.set(None);
}
