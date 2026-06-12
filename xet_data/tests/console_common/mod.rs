#![cfg(feature = "console")]

use std::path::Path;
use std::sync::Arc;

use rand::Rng;
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::{FileUploadSession, Sha256Policy, XetFileInfo};
use xet_runtime::console::registry::registry;
use xet_runtime::console::state::SessionConsole;

/// Simulate the session scope that xet_pkg normally installs (Task 8).
pub fn install_scope(config: &TranslatorConfig) -> Arc<SessionConsole> {
    let id = format!("itest-{}", xet_runtime::utils::UniqueId::new().0);
    let handle = registry().register_session(id, vec![]);
    let scope = handle.scope.clone();
    config.ctx.common.console_session.set(handle).ok();
    scope
}

/// Write `size` random bytes to a file and ingest it; returns its XetFileInfo.
pub async fn upload_random_file(session: &Arc<FileUploadSession>, dir: &Path, size: usize) -> XetFileInfo {
    let path = dir.join(format!("file_{size}.bin"));
    let mut data = vec![0u8; size];
    rand::rng().fill_bytes(&mut data);
    std::fs::write(&path, &data).unwrap();
    let (_id, handle) = session.spawn_upload_from_path(path, Sha256Policy::Compute).await.unwrap();
    handle.await.unwrap().unwrap().0
}
