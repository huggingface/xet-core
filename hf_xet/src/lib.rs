mod log;
mod log_buffer;
mod progress_update;
mod runtime;
mod token_refresh;

use std::fmt::Debug;
use std::iter::IntoIterator;
use std::sync::Arc;

use data::errors::DataProcessingError;
use data::{data_client, PointerFile};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::pyfunction;
use runtime::async_run;
use token_refresh::WrappedTokenRefresher;
use utils::auth::TokenRefresher;
use utils::progress::ProgressUpdater;
use cas_types::HttpRange;
use cas_object::parse_chunk_header;

use crate::progress_update::WrappedProgressUpdater;
use reqwest::header::{AUTHORIZATION, RANGE};
use lz4_flex;
use std::io::prelude::*;
use std::fs::File;
use std::time::{SystemTime, UNIX_EPOCH};
use reqwest::Url;

fn convert_data_processing_error(e: DataProcessingError) -> PyErr {
    if cfg!(debug_assertions) {
        PyRuntimeError::new_err(format!("Data processing error: {e:?}"))
    } else {
        PyRuntimeError::new_err(format!("Data processing error: {e}"))
    }
}

#[pyfunction]
#[pyo3(signature = (file_paths, endpoint, token_info, token_refresher, progress_updater), text_signature = "(file_paths: List[str], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresher: Optional[Callable[[], (str, int)]], progress_updater: Optional[Callable[[int, None]]) -> List[PyPointerFile]")]
pub fn upload_files(
    py: Python,
    file_paths: Vec<String>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Py<PyAny>>,
    progress_updater: Option<Py<PyAny>>,
) -> PyResult<Vec<PyPointerFile>> {
    let refresher = token_refresher.map(WrappedTokenRefresher::from_func).transpose()?.map(Arc::new);
    let updater = progress_updater
        .map(WrappedProgressUpdater::from_func)
        .transpose()?
        .map(Arc::new);

    async_run(py, move |threadpool| async move {
        let out: Vec<PyPointerFile> = data_client::upload_async(
            threadpool,
            file_paths,
            endpoint,
            token_info,
            refresher.map(|v| v as Arc<_>),
            updater.map(|v| v as Arc<_>),
        )
        .await
        .map_err(convert_data_processing_error)?
        .into_iter()
        .map(PyPointerFile::from)
        .collect();
        PyResult::Ok(out)
    })
}

#[pyfunction]
#[pyo3(signature = (files, endpoint, token_info, token_refresher, progress_updater), text_signature = "(files: List[PyPointerFile], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresher: Optional[Callable[[], (str, int)]], progress_updater: Optional[List[Callable[[int], None]]]) -> List[str]")]
pub fn download_files(
    py: Python,
    files: Vec<PyPointerFile>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Py<PyAny>>,
    progress_updater: Option<Vec<Py<PyAny>>>,
) -> PyResult<Vec<String>> {
    let pfs = files.into_iter().map(PointerFile::from).collect();

    let refresher = token_refresher.map(WrappedTokenRefresher::from_func).transpose()?.map(Arc::new);
    let updaters = progress_updater.map(try_parse_progress_updaters).transpose()?;

    async_run(py, move |threadpool| async move {
        let out: Vec<String> = data_client::download_async(
            threadpool,
            pfs,
            endpoint,
            token_info,
            refresher.map(|v| v as Arc<_>),
            updaters,
        )
        .await
        .map_err(convert_data_processing_error)?;

        PyResult::Ok(out)
    })
}

#[pyfunction]
#[pyo3(signature = (files, endpoint, token_info, token_refresher, progress_updater), text_signature = "(files: List[PyPointerFile], endpoint: Optional[str], token_info: Optional[(str, int)], token_refresher: Optional[Callable[[], (str, int)]], progress_updater: Optional[List[Callable[[int], None]]]) -> List[str]")]
pub fn naive_download_files(
    py: Python,
    files: Vec<PyPointerFile>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Py<PyAny>>,
    progress_updater: Option<Vec<Py<PyAny>>>,
) -> PyResult<Vec<String>> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            naive_download_impl(
                files,
                endpoint,
                token_info,
                token_refresher,
                progress_updater,
            ).await
        }).map_err(|err| {
            PyRuntimeError::new_err(format!("Error: {err:?}"))
        })
}

async fn fetch_block(block_url: &str, url_range: &HttpRange) -> Vec<u8> {
    let res = reqwest::Client::new().get(block_url)
        .header(RANGE, format!("bytes={}-{}", url_range.start, url_range.end))
        .send()
        .await.unwrap();

    res.bytes().await.unwrap().to_vec()
}

async fn naive_download_impl(
    files: Vec<PyPointerFile>,
    endpoint: Option<String>,
    token_info: Option<(String, u64)>,
    token_refresher: Option<Py<PyAny>>,
    progress_updater: Option<Vec<Py<PyAny>>>,
) -> PyResult<Vec<String>> {
    let mut token = token_info.as_ref().unwrap().0.clone();
    let mut expiration = token_info.as_ref().unwrap().1;

    let refresher = WrappedTokenRefresher::from_func(token_refresher.unwrap()).unwrap();

    for file in files.iter() {
        let cur_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(u64::MAX);
        if expiration <= cur_time{
            let (new_token, new_expiry) = refresher.refresh().unwrap();
            token = new_token; 
            expiration = new_expiry; 
        }

        let url_val = format!("{}/reconstruction/{}", endpoint.as_deref().unwrap_or("http://localhost:5564"), file.hash);
        let url = Url::parse(&format!("{}/reconstruction/{}", endpoint.as_deref().unwrap_or("http://localhost:5564"), file.hash)).unwrap();

        let res = reqwest::Client::new().get(url)
            .header(AUTHORIZATION, format!("Bearer {}", token)) 
            .send()
            .await.unwrap()
            .json::<cas_types::QueryReconstructionResponse>()
            .await.unwrap();


        let fetch_info = res.fetch_info;

        let mut buffer = File::create(file.path.clone()).unwrap();

        for term in res.terms.iter() {
            let hash = term.hash;
            let ranges = fetch_info.get(&hash).unwrap();
            
            for r in ranges.iter() {
                
                let mut response: Vec<u8> = [].to_vec();
                if r.range == term.range {
                    response = fetch_block(&r.url, &r.url_range).await;
                }

                let mut chunk_start = 0;
                loop {
                    if chunk_start >= response.len() {
                        break;
                    }

                    let chunk_header = parse_chunk_header(response[chunk_start..chunk_start + 8].try_into().unwrap()).unwrap();
                    let compressed_length = chunk_header.get_compressed_length();

                    let mut read_buffer = Vec::new();

                    let mut rdr = lz4_flex::frame::FrameDecoder::new(&response[chunk_start + 8..chunk_start + 8 + compressed_length as usize]);
                    
                    let written = rdr.read(&mut read_buffer).unwrap();

                    buffer.write(&read_buffer).unwrap();

                    chunk_start = chunk_start + compressed_length as usize + 8;
                }
            }
        }
        buffer.flush().unwrap();
        
    }

    PyResult::Ok(vec![])
}

fn try_parse_progress_updaters(funcs: Vec<Py<PyAny>>) -> PyResult<Vec<Arc<dyn ProgressUpdater>>> {
    let mut updaters = Vec::with_capacity(funcs.len());
    for updater_func in funcs {
        let wrapped = Arc::new(WrappedProgressUpdater::from_func(updater_func)?);
        updaters.push(wrapped as Arc<dyn ProgressUpdater>);
    }
    Ok(updaters)
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct PyPointerFile {
    #[pyo3(get, set)]
    path: String,
    #[pyo3(get)]
    hash: String,
    #[pyo3(get)]
    filesize: u64,
}

impl From<PointerFile> for PyPointerFile {
    fn from(pf: PointerFile) -> Self {
        Self {
            path: pf.path().to_string(),
            hash: pf.hash_string().to_string(),
            filesize: pf.filesize(),
        }
    }
}

impl From<PyPointerFile> for PointerFile {
    fn from(pf: PyPointerFile) -> Self {
        PointerFile::init_from_info(&pf.path, &pf.hash, pf.filesize)
    }
}

#[pymethods]
impl PyPointerFile {
    #[new]
    pub fn new(path: String, hash: String, filesize: u64) -> Self {
        Self { path, hash, filesize }
    }

    fn __str__(&self) -> String {
        format!("{self:?}")
    }

    fn __repr__(&self) -> String {
        format!("PyPointerFile({}, {}, {})", self.path, self.hash, self.filesize)
    }
}

#[pymodule]
pub fn hf_xet(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(upload_files, m)?)?;
    m.add_function(wrap_pyfunction!(download_files, m)?)?;
    m.add_function(wrap_pyfunction!(naive_download_files, m)?)?;
    m.add_class::<PyPointerFile>()?;
    Ok(())
}
