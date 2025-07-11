use std::fmt::{Debug, Formatter};

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::PyAnyMethods;
use pyo3::{Py, PyAny, PyErr, PyResult, Python};
use tracing::error;
use utils::auth::{TokenInfo, TokenRefresher};
use utils::errors::AuthError;

/// A wrapper struct of a python function to refresh the CAS auth token.
/// Since tokens are generated by hub, we want to be able to refresh the
/// token using the hub client, which is only available in python.
pub struct WrappedTokenRefresher {
    /// The function responsible for refreshing a token.
    /// Expects no inputs and returns a (str, u64) representing the new token
    /// and the unixtime (in seconds) of expiration, raising an exception
    /// if there is an issue.
    py_func: Py<PyAny>,
    name: String,
}

impl Debug for WrappedTokenRefresher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WrappedTokenRefresher({})", self.name)
    }
}

impl WrappedTokenRefresher {
    pub fn from_func(py_func: Py<PyAny>) -> PyResult<Self> {
        let name = Self::validate_callable(&py_func)?;
        Ok(Self { py_func, name })
    }

    /// Validate that the inputted python object is callable
    fn validate_callable(py_func: &Py<PyAny>) -> Result<String, PyErr> {
        Python::with_gil(|py| {
            let f = py_func.bind(py);
            let name = f
                .repr()
                .and_then(|repr| repr.extract::<String>())
                .unwrap_or("unknown".to_string());
            if !f.is_callable() {
                error!("TokenRefresher func: {name} is not callable");
                return Err(PyTypeError::new_err(format!("refresh func: {name} is not callable")));
            }
            Ok(name)
        })
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
impl TokenRefresher for WrappedTokenRefresher {
    async fn refresh(&self) -> Result<TokenInfo, AuthError> {
        Python::with_gil(|py| {
            let f = self.py_func.bind(py);
            if !f.is_callable() {
                return Err(AuthError::RefreshFunctionNotCallable(self.name.clone()));
            }
            let result = f
                .call0()
                .map_err(|e| AuthError::TokenRefreshFailure(format!("Error refreshing token: {e:?}")))?;
            result.extract::<(String, u64)>().map_err(|e| {
                AuthError::TokenRefreshFailure(format!("refresh function didn't return a (String, u64) tuple: {e:?}"))
            })
        })
    }
}
