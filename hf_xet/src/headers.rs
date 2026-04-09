use std::collections::HashMap;

use http::header::{HeaderMap, HeaderName, HeaderValue};
use pyo3::exceptions::PyValueError;
use pyo3::PyResult;

/// Convert a Python ``Dict[str, str]`` into a Rust [`HeaderMap`].
pub(crate) fn hashmap_to_headermap(map: HashMap<String, String>) -> PyResult<HeaderMap> {
    let mut header_map = HeaderMap::with_capacity(map.len());
    for (key, value) in map {
        let name = HeaderName::from_bytes(key.as_bytes())
            .map_err(|e| PyValueError::new_err(format!("Invalid header name '{key}': {e}")))?;
        let val = HeaderValue::from_str(&value)
            .map_err(|e| PyValueError::new_err(format!("Invalid header value for '{key}': {e}")))?;
        header_map.insert(name, val);
    }
    Ok(header_map)
}
