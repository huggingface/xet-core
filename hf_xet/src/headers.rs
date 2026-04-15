use std::collections::HashMap;

use http::header::{self, HeaderMap, HeaderName, HeaderValue};
use pyo3::PyResult;
use pyo3::exceptions::PyValueError;

const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

/// Build a HeaderMap containing only the USER_AGENT header.
pub(crate) fn default_headers() -> HeaderMap {
    let mut map = HeaderMap::new();
    map.insert(header::USER_AGENT, HeaderValue::from_static(USER_AGENT));
    map
}

/// Build a HeaderMap from a Python dict and merge in the USER_AGENT.
pub(crate) fn build_headers_with_user_agent(request_headers: Option<HashMap<String, String>>) -> PyResult<HeaderMap> {
    let mut map = request_headers.map(build_header_map).transpose()?.unwrap_or_default();

    let combined_user_agent = if let Some(existing_ua) = map.get(header::USER_AGENT) {
        let existing_str = existing_ua.to_str().unwrap_or("");
        format!("{}; {}", existing_str, USER_AGENT)
    } else {
        USER_AGENT.to_string()
    };

    let user_agent_value =
        HeaderValue::from_str(&combined_user_agent).unwrap_or_else(|_| HeaderValue::from_static(USER_AGENT));
    map.insert(header::USER_AGENT, user_agent_value);

    Ok(map)
}

/// Build a HeaderMap from a Python dict.
pub(crate) fn build_header_map(headers: HashMap<String, String>) -> PyResult<HeaderMap> {
    let mut map = HeaderMap::with_capacity(headers.len());
    for (key, value) in headers {
        let name = HeaderName::from_bytes(key.as_bytes())
            .map_err(|e| PyValueError::new_err(format!("Invalid header name '{}': {}", key, e)))?;
        let value = HeaderValue::from_str(&value)
            .map_err(|e| PyValueError::new_err(format!("Invalid header value for '{}': {}", key, e)))?;
        map.insert(name, value);
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_headers_contains_user_agent() {
        let headers = default_headers();
        assert_eq!(headers.len(), 1);
        let ua = headers.get(header::USER_AGENT).unwrap().to_str().unwrap();
        assert!(ua.starts_with("hf_xet/"), "expected 'hf_xet/<version>', got '{ua}'");
    }

    #[test]
    fn test_build_headers_with_none_empty_hashmap() {
        let empty_map: HashMap<String, String> = HashMap::new();
        let headers = build_headers_with_user_agent(Some(empty_map)).unwrap();

        // Should have exactly one header: USER_AGENT
        assert_eq!(headers.len(), 1);
        assert!(headers.contains_key(header::USER_AGENT));

        let user_agent = headers.get(header::USER_AGENT).unwrap().to_str().unwrap();
        assert_eq!(user_agent, USER_AGENT);

        let headers = build_headers_with_user_agent(None).unwrap();

        // Should have exactly one header: USER_AGENT
        assert_eq!(headers.len(), 1);
        assert!(headers.contains_key(header::USER_AGENT));

        let user_agent = headers.get(header::USER_AGENT).unwrap().to_str().unwrap();
        assert_eq!(user_agent, USER_AGENT);
    }

    #[test]
    fn test_build_headers_with_valid_headers() {
        let mut headers_map = HashMap::new();
        headers_map.insert("Content-Type".to_string(), "application/json".to_string());
        headers_map.insert("Authorization".to_string(), "Bearer token123".to_string());

        let headers = build_headers_with_user_agent(Some(headers_map)).unwrap();

        // Should have 3 headers: Content-Type, Authorization, and USER_AGENT
        assert_eq!(headers.len(), 3);

        // Verify each header was converted correctly
        assert_eq!(headers.get(header::CONTENT_TYPE).unwrap().to_str().unwrap(), "application/json");
        assert_eq!(headers.get(header::AUTHORIZATION).unwrap().to_str().unwrap(), "Bearer token123");

        // Verify USER_AGENT was added
        let user_agent = headers.get(header::USER_AGENT).unwrap().to_str().unwrap();
        assert_eq!(user_agent, USER_AGENT);
    }

    #[test]
    fn test_build_headers_appends_to_existing_user_agent() {
        let mut headers_map = HashMap::new();
        headers_map.insert("User-Agent".to_string(), "CustomClient/1.0".to_string());

        let headers = build_headers_with_user_agent(Some(headers_map)).unwrap();

        // Should have exactly one header: USER_AGENT
        assert_eq!(headers.len(), 1);

        // Verify USER_AGENT was appended to existing one
        let user_agent = headers.get(header::USER_AGENT).unwrap().to_str().unwrap();
        assert_eq!(user_agent, format!("CustomClient/1.0; {}", USER_AGENT));
    }

    #[test]
    fn test_build_headers_with_invalid_header_name_or_value() {
        let mut headers_map = HashMap::new();
        headers_map.insert("Invalid Header!".to_string(), "value".to_string());

        let result = build_headers_with_user_agent(Some(headers_map));

        // Should return an error for invalid header name
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid header name"));

        let mut headers_map = HashMap::new();
        // Header values cannot contain newlines
        headers_map.insert("X-Custom".to_string(), "value\nwith\nnewlines".to_string());

        let result = build_headers_with_user_agent(Some(headers_map));

        // Should return an error for invalid header value
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid header value"));
    }
}
