use crate::log_buffer::HF_DEFAULT_ENDPOINT;
use crate::runtime::init_threadpool;
use crate::token_refresh::WrappedTokenRefresher;
use crate::{
    convert_data_processing_error, try_parse_progress_updater, DestinationPath, PyXetDownloadInfo, PyXetUploadInfo,
};
use base64::engine::general_purpose::STANDARD;
use base64::engine::GeneralPurpose;
use base64::Engine;
use data::configurations::TranslatorConfig;
use data::data_client::default_config;
use data::{FileDownloader, FileUploadSession, XetFileInfo, INGESTION_BLOCK_SIZE};
use error_printer::ErrorPrinter;
use pyo3::Python;
use pyo3::{pyclass, pymethods, Py, PyAny, PyResult};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use utils::auth::TokenRefresher;

/// XetSession is per-repo specific
#[pyclass]
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
        let config =
            default_config(endpoint.unwrap_or(HF_DEFAULT_ENDPOINT.to_string()), None, token_info, token_refresher)
                .map_err(convert_data_processing_error)?;
        Ok(Self {
            repo_id,
            config,
            file_upload_session: async_once_cell::OnceCell::new(),
            file_downloader: async_once_cell::OnceCell::new(),
            threadpool,
        })
    }

    pub async fn upload_file(
        &self,
        tracker_id: String,
        file_path: String,
        progress_updater: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        let file_upload_session = self.file_upload_session().await;

        let mut reader = File::open(&file_path)?;

        let n = reader.metadata()?.len() as usize;
        let mut buffer = vec![0u8; usize::min(n, *INGESTION_BLOCK_SIZE)];

        let mut handle = file_upload_session.start_clean(Some(STANDARD.encode(tracker_id.as_bytes())));

        loop {
            let bytes = reader.read(&mut buffer)?;
            if bytes == 0 {
                break;
            }

            handle
                .add_data(&buffer[0..bytes])
                .await
                .map_err(convert_data_processing_error)?;
        }

        Ok(())
    }

    pub async fn upload_bytes(
        &self,
        tracker_id: String,
        file_bytes: Vec<u8>,
        progress_updater: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        let file_upload_session = self.file_upload_session().await;

        let mut handle = file_upload_session.start_clean(Some(STANDARD.encode(tracker_id.as_bytes())));
        handle.add_data(&file_bytes).await.map_err(convert_data_processing_error)?;

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
            .get_or_init(async {
                Arc::new(FileDownloader::new(self.config.clone(), self.threadpool.clone()).await.unwrap())
            })
            .await
            .clone()
    }

    async fn file_upload_session(&self) -> Arc<FileUploadSession> {
        self.file_upload_session
            .get_or_init(async {
                FileUploadSession::new(self.config.clone(), self.threadpool.clone(), None)
                    .await
                    .unwrap()
            })
            .await
            .clone()
    }
}
