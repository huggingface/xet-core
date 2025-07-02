use crate::log_buffer::HF_DEFAULT_ENDPOINT;
use crate::token_refresh::WrappedTokenRefresher;
use crate::{
    convert_data_processing_error, try_parse_progress_updater, DestinationPath, PyXetDownloadInfo, PyXetUploadInfo,
};
use data::configurations::TranslatorConfig;
use data::constants::INGESTION_BLOCK_SIZE;
use data::data_client::default_config;
use data::errors::DataProcessingError;
use data::{FileDownloader, FileUploadSession, SingleFileCleaner, XetFileInfo};
use merklehash::MerkleHash;
use progress_tracking::item_tracking::ItemProgressUpdater;
use pyo3::{pyclass, pymethods, Py, PyAny, PyResult};
use std::fs::File;
use std::io::Cursor;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use utils::auth::TokenRefresher;

#[pyclass]
pub struct XetSession {
    config: Arc<TranslatorConfig>,
    file_upload_session: async_once_cell::OnceCell<Result<Arc<FileUploadSession>, Arc<DataProcessingError>>>,
    file_downloader: async_once_cell::OnceCell<Result<Arc<FileDownloader>, Arc<DataProcessingError>>>,
}

#[pymethods]
impl XetSession {
    #[new]
    #[pyo3(signature = (endpoint=None, token_info=None, token_refresher=None))]
    pub fn new(
        endpoint: Option<String>,
        token_info: Option<(String, u64)>,
        token_refresher: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let token_refresher: Option<Arc<dyn TokenRefresher>> = token_refresher
            .map(WrappedTokenRefresher::from_func)
            .transpose()?
            .map(|tr| Arc::new(tr) as Arc<dyn TokenRefresher>);
        let config =
            default_config(endpoint.unwrap_or(HF_DEFAULT_ENDPOINT.to_string()), None, token_info, token_refresher)
                .map_err(convert_data_processing_error)?;
        Ok(Self {
            config,
            file_upload_session: async_once_cell::OnceCell::new(),
            file_downloader: async_once_cell::OnceCell::new(),
        })
    }

    #[pyo3(signature = (file_path))]
    pub async fn upload_file(&self, file_path: String) -> PyResult<PyXetUploadInfo> {
        let file_upload_session = self.file_upload_session().await.map_err(convert_data_processing_error)?;

        let mut reader = File::open(&file_path)?;
        let file_size = reader.metadata()?.len();
        let handle = file_upload_session.start_clean(Some(file_path.into()), file_size).await;

        consume_reader_to_cleaner(&mut reader, file_size.min(*INGESTION_BLOCK_SIZE), handle).await
    }

    #[pyo3(signature = (file_contents))]
    pub async fn upload_bytes(&self, file_contents: Vec<u8>) -> PyResult<PyXetUploadInfo> {
        let file_upload_session = self.file_upload_session().await.map_err(convert_data_processing_error)?;

        let contents_size = file_contents.len();
        let handle = file_upload_session.start_clean(None, contents_size as u64).await;
        let mut reader = Cursor::new(file_contents);

        consume_reader_to_cleaner(&mut reader, contents_size.min(*INGESTION_BLOCK_SIZE), handle).await
    }

    #[pyo3(signature = (file, progress_updater = None))]
    pub async fn download_file_to_path(
        &self,
        file: PyXetDownloadInfo,
        progress_updater: Option<Py<PyAny>>,
    ) -> PyResult<String> {
        let downloader = self.file_downloader().await.map_err(convert_data_processing_error)?;

        let progress_updater = progress_updater
            .map(try_parse_progress_updater)
            .transpose()?
            .map(ItemProgressUpdater::new);

        let (file_info, dest_path): (XetFileInfo, DestinationPath) = file.into();
        let hash = MerkleHash::from_hex(file_info.hash())
            .map_err(data::errors::DataProcessingError::from)
            .map_err(convert_data_processing_error)?;
        let dest_path_buf: PathBuf = dest_path.as_str().into();
        let output_provider = cas_client::OutputProvider::new_file_provider(dest_path_buf);
        downloader
            .smudge_file_from_hash(&hash, dest_path.as_str().into(), &output_provider, None, progress_updater)
            .await
            .map_err(convert_data_processing_error)?;
        Ok(dest_path)
    }
}

async fn consume_reader_to_cleaner(
    reader: &mut impl Read,
    ingestion_block_size: usize,
    mut handle: SingleFileCleaner,
) -> PyResult<PyXetUploadInfo> {
    let mut buffer = vec![0u8; ingestion_block_size];

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

    let (info, _) = handle.finish().await.map_err(convert_data_processing_error)?;

    Ok(info.into())
}

impl XetSession {
    async fn file_downloader(&self) -> Result<Arc<FileDownloader>, Arc<DataProcessingError>> {
        self.file_downloader
            .get_or_init(async { FileDownloader::new(self.config.clone()).await.map(Arc::new).map_err(Arc::new) })
            .await
            .clone()
    }

    async fn file_upload_session(&self) -> Result<Arc<FileUploadSession>, Arc<DataProcessingError>> {
        self.file_upload_session
            .get_or_init(async { FileUploadSession::new(self.config.clone(), None).await.map_err(Arc::new) })
            .await
            .clone()
    }
}
