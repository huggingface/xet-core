use crate::runtime::init_threadpool;
use crate::token_refresh::WrappedTokenRefresher;
use crate::{
    convert_data_processing_error, try_parse_progress_updater, try_parse_progress_updaters, DestinationPath,
    PyXetDownloadInfo, PyXetUploadInfo,
};
use data::configurations::TranslatorConfig;
use data::data_client::default_config;
use data::{FileDownloader, FileUploadSession, XetFileInfo};
use error_printer::ErrorPrinter;
use pyo3::{pyclass, pymethods, Py, PyAny, PyResult};
use std::collections::HashMap;
use std::sync::Arc;
use utils::auth::{TokenInfo, TokenRefresher};

#[pyclass]
#[derive()]
pub struct XetSession {
    repo_id: String,
    file_upload_session: async_once_cell::OnceCell<Arc<FileUploadSession>>,
    file_downloader: async_once_cell::OnceCell<Arc<FileDownloader>>,
    config: Arc<TranslatorConfig>,
    threadpool: Arc<xet_threadpool::ThreadPool>,
}

#[pymethods]
impl XetSession {
    #[new]
    #[pyo3(signature = (repo_id: str, endpoint="https://huggingface.co", token_info=None, token_refresher=None)
    )]
    pub fn new(
        py: Python,
        repo_id: String,
        endpoint: Option<String>,
        token_info: Option<(String, u64)>,
        token_refresher: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let threadpool = init_threadpool(py)?;
        let token_refresher: Option<Arc<dyn TokenRefresher>> = token_refresher
            .map(WrappedTokenRefresher::from_func)
            .transpose()?
            .map(|tr| Arc::new(tr) as Arc<dyn TokenRefresher>);
        let config = default_config(endpoint, None, token_info, token_refresher);
        Self { repo_id, config }
    }

    pub async fn upload_file(
        &self,
        tracker_id: String,
        file_path: String,
        progress_updater: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        Ok(())
    }

    pub async fn upload_bytes(
        &self,
        tracker_id: String,
        file_bytes: Vec<u8>,
        progress_updater: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        Ok(())
    }

    pub async fn flush(&self) -> PyResult<HashMap<String, PyXetUploadInfo>> {
        Ok(HashMap::new())
    }

    pub async fn close(&self) -> PyResult<HashMap<String, PyXetUploadInfo>> {
        Ok(HashMap::new())
    }

    pub async fn download_file(
        &self,
        download_info: PyXetDownloadInfo,
        progress_updater: Option<Py<PyAny>>,
    ) -> PyResult<String> {
        let (file_info, destination_path): (XetFileInfo, DestinationPath) = download_info.into();
        let file_downloader = self.file_downloader().await;
        let progress = progress_updater
            .map(try_parse_progress_updater)
            .transpose()
            .log_error("failed to parse progress_updater, ignoring error, progress bar will be incorrect")
            .ok()
            .flatten();
        data::data_client::smudge_file(&file_downloader, &file_info, destination_path.as_str(), progress)
            .await
            .map_err(convert_data_processing_error)
    }
}

impl XetSession {
    async fn file_downloader(&self) -> Arc<FileDownloader> {
        self.file_downloader
            .get_or_init(async || {
                Arc::new(FileDownloader::new(self.config.clone(), self.threadpool.clone()).await.unwrap())
            })
            .await
            .clone()
    }

    async fn file_upload_session(&self) -> Arc<FileUploadSession> {
        self.file_upload_session
            .get_or_init(async || {
                Arc::new(
                    FileUploadSession::new(self.config.clone(), self.threadpool.clone(), None)
                        .await
                        .unwrap(),
                )
            })
            .await
            .clone()
    }
}
