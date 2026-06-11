//! Runtime-scoped cache of adaptive concurrency controllers, keyed by endpoint.
//!
//! All `RemoteClient`s created from the same `XetContext` (i.e. the same
//! `XetSession`) and pointing at the same endpoint share one upload and one
//! download controller, so the adaptive model state and the concurrency limit
//! are session-wide rather than per-client.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use xet_runtime::core::XetContext;

use super::AdaptiveConcurrencyController;

const CACHE_KEY: &str = "cas_client::concurrency_controllers";

#[derive(Default)]
struct ConcurrencyControllerCache {
    upload: Mutex<HashMap<String, Arc<AdaptiveConcurrencyController>>>,
    download: Mutex<HashMap<String, Arc<AdaptiveConcurrencyController>>>,
}

/// Returns the shared upload concurrency controller for this (ctx, endpoint) pair,
/// creating it on first use.
pub fn upload_controller(ctx: &XetContext, endpoint: &str) -> Arc<AdaptiveConcurrencyController> {
    let cache: Arc<ConcurrencyControllerCache> = ctx.common.cache_get_or_create(CACHE_KEY, Default::default);
    let mut map = cache.upload.lock().unwrap();
    map.entry(endpoint.to_string())
        .or_insert_with(|| AdaptiveConcurrencyController::new_upload(ctx.clone(), "upload"))
        .clone()
}

/// Returns the shared download concurrency controller for this (ctx, endpoint) pair,
/// creating it on first use.
pub fn download_controller(ctx: &XetContext, endpoint: &str) -> Arc<AdaptiveConcurrencyController> {
    let cache: Arc<ConcurrencyControllerCache> = ctx.common.cache_get_or_create(CACHE_KEY, Default::default);
    let mut map = cache.download.lock().unwrap();
    map.entry(endpoint.to_string())
        .or_insert_with(|| AdaptiveConcurrencyController::new_download(ctx.clone(), "download"))
        .clone()
}

#[cfg(test)]
#[cfg(not(target_family = "wasm"))]
mod tests {
    use super::*;

    const EP_A: &str = "https://cas-a.example.com";
    const EP_B: &str = "https://cas-b.example.com";

    #[test]
    fn test_same_ctx_same_endpoint_shares_controller() {
        let ctx = XetContext::default().unwrap();
        assert!(Arc::ptr_eq(&upload_controller(&ctx, EP_A), &upload_controller(&ctx, EP_A)));
        assert!(Arc::ptr_eq(&download_controller(&ctx, EP_A), &download_controller(&ctx, EP_A)));
    }

    #[test]
    fn test_same_ctx_different_endpoint_distinct_controllers() {
        let ctx = XetContext::default().unwrap();
        assert!(!Arc::ptr_eq(&upload_controller(&ctx, EP_A), &upload_controller(&ctx, EP_B)));
        assert!(!Arc::ptr_eq(&download_controller(&ctx, EP_A), &download_controller(&ctx, EP_B)));
    }

    #[test]
    fn test_different_ctx_distinct_controllers() {
        let ctx1 = XetContext::default().unwrap();
        let ctx2 = XetContext::default().unwrap();
        assert!(!Arc::ptr_eq(&upload_controller(&ctx1, EP_A), &upload_controller(&ctx2, EP_A)));
    }

    #[test]
    fn test_upload_and_download_controllers_distinct() {
        let ctx = XetContext::default().unwrap();
        assert!(!Arc::ptr_eq(&upload_controller(&ctx, EP_A), &download_controller(&ctx, EP_A)));
    }

    #[test]
    // The cache lives inside XetCommon, so cached controllers must not hold anything that
    // points back at XetCommon — that Arc cycle would keep the runtime state alive forever.
    fn test_cached_controllers_do_not_keep_runtime_common_alive() {
        let ctx = XetContext::default().unwrap();
        let weak_common = Arc::downgrade(&ctx.common);
        let _upload = upload_controller(&ctx, EP_A);
        let _download = download_controller(&ctx, EP_A);
        drop(ctx);
        assert!(weak_common.upgrade().is_none());
    }
}
