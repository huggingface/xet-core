use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::task::JoinHandle;

use super::XetFileInfo;
use super::configurations::TranslatorConfig;
use super::file_cleaner::{Sha256Policy, SingleFileCleaner};
use super::file_download_session::FileDownloadSession;
use super::file_upload_session::FileUploadSession;
use crate::deduplication::DeduplicationMetrics;
use crate::error::Result;
use crate::progress_tracking::{GroupProgressReport, ItemProgressReport, UniqueID};

pub struct FileUploadCoordinator {
    session: Arc<FileUploadSession>,
}

impl FileUploadCoordinator {
    pub async fn new(config: Arc<TranslatorConfig>) -> Result<Arc<Self>> {
        let session = FileUploadSession::new(config).await?;
        Ok(Arc::new(Self { session }))
    }

    pub fn report(&self) -> GroupProgressReport {
        self.session.report()
    }

    pub fn item_report(&self, id: UniqueID) -> Option<ItemProgressReport> {
        self.session.item_report(id)
    }

    pub fn item_reports(&self) -> HashMap<UniqueID, ItemProgressReport> {
        self.session.item_reports()
    }

    pub fn start_clean(
        self: &Arc<Self>,
        tracking_name: Option<Arc<str>>,
        size: Option<u64>,
        sha256: Sha256Policy,
    ) -> Result<(UniqueID, SingleFileCleaner)> {
        self.session.start_clean(tracking_name, size, sha256)
    }

    pub async fn spawn_upload_from_path(
        self: &Arc<Self>,
        file_path: PathBuf,
        sha256: Sha256Policy,
    ) -> Result<(UniqueID, JoinHandle<Result<(XetFileInfo, DeduplicationMetrics)>>)> {
        self.session.spawn_upload_from_path(file_path, sha256).await
    }

    pub async fn spawn_upload_bytes(
        self: &Arc<Self>,
        bytes: Vec<u8>,
        sha256: Sha256Policy,
        tracking_name: Option<Arc<str>>,
    ) -> Result<(UniqueID, JoinHandle<Result<(XetFileInfo, DeduplicationMetrics)>>)> {
        self.session.spawn_upload_bytes(bytes, sha256, tracking_name).await
    }

    pub async fn finalize_with_report(self: Arc<Self>) -> Result<(DeduplicationMetrics, GroupProgressReport)> {
        self.session.clone().finalize_with_report().await
    }
}

pub struct FileDownloadCoordinator {
    session: Arc<FileDownloadSession>,
}

impl FileDownloadCoordinator {
    pub async fn new(config: Arc<TranslatorConfig>) -> Result<Arc<Self>> {
        let session = FileDownloadSession::new(config).await?;
        Ok(Arc::new(Self { session }))
    }

    pub fn report(&self) -> GroupProgressReport {
        self.session.report()
    }

    pub fn item_report(&self, id: UniqueID) -> Option<ItemProgressReport> {
        self.session.item_report(id)
    }

    pub fn item_reports(&self) -> HashMap<UniqueID, ItemProgressReport> {
        self.session.item_reports()
    }

    pub async fn download_file_background(
        self: &Arc<Self>,
        file_info: XetFileInfo,
        write_path: PathBuf,
    ) -> Result<(UniqueID, JoinHandle<Result<u64>>)> {
        self.session.download_file_background(file_info, write_path).await
    }

    pub fn session(&self) -> Arc<FileDownloadSession> {
        self.session.clone()
    }

    pub fn abort_active_streams(&self) {
        self.session.abort_active_streams()
    }
}
