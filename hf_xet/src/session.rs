use crate::{
    convert_data_processing_error, token_refresh::WrappedTokenRefresher,
    try_parse_progress_updater, PyPointerFile,
};
use data::configurations::TranslatorConfig;
use data::data_client::{clean_file, default_config, DEFAULT_CAS_ENDPOINT};
use data::errors::DataProcessingError;
use data::{errors, FileDownloader, FileProvider, FileUploadSession, OutputProvider, PointerFile};
use pyo3::{pyclass, pymethods, Py, PyAny, PyResult};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use utils::auth::TokenRefresher;
use xet_threadpool::ThreadPool;

#[pyclass]
#[derive(Clone)]
pub struct PyXetSession {
    config: Arc<TranslatorConfig>,
    upload: Arc<async_once_cell::OnceCell<Arc<FileUploadSession>>>,
    download: Arc<async_once_cell::OnceCell<Arc<FileDownloader>>>,
    threadpool: Arc<ThreadPool>,
    upload_task_queue: Arc<Mutex<VecDeque<JoinHandle<errors::Result<PointerFile>>>>>,
}

#[pymethods]
impl PyXetSession {
    #[new]
    #[pyo3(signature = (endpoint=None, token_info=None, token_refresher=None))]
    pub fn new(
        py: pyo3::Python,
        endpoint: Option<String>,
        token_info: Option<(String, u64)>,
        token_refresher: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let threadpool = crate::runtime::get_threadpool(py)?;
        let refresher = token_refresher
            .map(WrappedTokenRefresher::from_func)
            .transpose()?
            .map(Arc::new)
            .map(|v| v as Arc<dyn TokenRefresher>);
        let config = default_config(endpoint.unwrap_or(DEFAULT_CAS_ENDPOINT.clone()), None, token_info, refresher)
            .map_err(convert_data_processing_error)?;

        Ok(PyXetSession {
            config: config.clone(),
            upload: Arc::new(Default::default()),
            download: Arc::new(Default::default()),
            threadpool,
            upload_task_queue: Default::default(),
        })
    }

    #[pyo3(signature = (file, progress_updater=None))]
    pub async fn download_file(&self, file: PyPointerFile, progress_updater: Option<Py<PyAny>>) -> PyResult<String> {
        let pointer_file = PointerFile::from(file);

        let progress_updater = progress_updater.map(try_parse_progress_updater).transpose()?;

        let path = PathBuf::from(pointer_file.path());
        if let Some(parent_dir) = path.parent() {
            std::fs::create_dir_all(parent_dir)?;
        }
        let output = OutputProvider::File(FileProvider::new(path));
        self.download()
            .await
            .smudge_file_from_pointer(&pointer_file, &output, None, progress_updater)
            .await
            .map_err(convert_data_processing_error)?;
        Ok(pointer_file.path().to_string())
    }

    #[pyo3(signature = (file_path, progress_updater=None))]
    pub async fn upload_file(&self, file_path: String, progress_updater: Option<Py<PyAny>>) -> PyResult<()> {
        let progress_updater = progress_updater.map(try_parse_progress_updater).transpose()?;

        let upload = self.upload().await.clone();
        let pointer_file_jh = self
            .threadpool
            .spawn(async move { clean_file(upload, file_path, progress_updater).await.map(|(pf, _metrics)| pf) });
        self.upload_task_queue.lock().await.push_back(pointer_file_jh);
        Ok(())
    }

    pub async fn flush(&self) -> PyResult<Vec<PyPointerFile>> {
        let handles = { self.upload_task_queue.lock().await.split_off(0) };
        let mut result = Vec::with_capacity(handles.len());
        for handle in handles {
            let pointer_file = handle
                .await
                .map_err(|e| convert_data_processing_error(DataProcessingError::InternalError(e.to_string())))?
                .map_err(convert_data_processing_error)?;
            result.push(pointer_file.into());
        }
        self.upload().await.finalize().await.map_err(convert_data_processing_error)?;
        Ok(result)
    }
}

impl PyXetSession {
    async fn upload(&self) -> Arc<FileUploadSession> {
        self.upload
            .get_or_init(async {
                // fix unwrap?
                let upload = FileUploadSession::new(self.config.clone(), self.threadpool.clone(), None)
                    .await
                    .unwrap();
                upload
            })
            .await
            .clone()
    }

    async fn download(&self) -> Arc<FileDownloader> {
        self.download
            .get_or_init(async {
                // fix unwrap?
                let download = FileDownloader::new(self.config.clone(), self.threadpool.clone()).await.unwrap();
                Arc::new(download)
            })
            .await
            .clone()
    }
}
