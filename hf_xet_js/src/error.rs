use thiserror::Error;
use wasm_bindgen::JsValue;
use merklehash::DataHashHexParseError;

#[derive(Debug, Error)]
pub enum HFXetJSError {
    #[error("{0}")]
    Generic(String),
    #[error("InvalidArguments: {0}")]
    InvalidArguments(String),
    #[error("serde_wasm_bindgen: {0}")]
    SerdeWasmBindgen(#[from] serde_wasm_bindgen::Error),
    #[error("DataHashHexParseError {0}")]
    DataHashHexParse(#[from] DataHashHexParseError),
}

impl From<HFXetJSError> for JsValue {
    fn from(value: HFXetJSError) -> Self {
        JsValue::from_str(&value.to_string())
    }
}

impl HFXetJSError {
    pub fn invalid_arguments<T: ToString>(inner: T) -> Self {
        HFXetJSError::InvalidArguments(inner.to_string())
    }
}
