//! Legacy types and functions kept for backward compatibility with `huggingface_hub`.
//!
//! All items in this module emit a ``DeprecationWarning`` when called from Python.
//! New code should use the ``XetSession`` object-oriented API.

pub mod functions;
pub(super) mod progress_update;
pub(super) mod runtime;
pub(super) mod token_refresh;
mod types;

pub use functions::{download_files, force_sigint_shutdown, hash_files, upload_bytes, upload_files};
pub use progress_update::{PyItemProgressUpdate, PyTotalProgressUpdate};
pub use types::{PyPointerFile, PyXetDownloadInfo, PyXetUploadInfo};
