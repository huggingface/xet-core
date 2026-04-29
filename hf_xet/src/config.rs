use pyo3::prelude::*;
use pyo3::types::PyDict;
use xet_runtime::config::XetConfig;

#[pyclass(name = "XetConfig")]
#[derive(Clone)]
pub struct PyXetConfig {
    inner: XetConfig,
}

impl From<XetConfig> for PyXetConfig {
    fn from(inner: XetConfig) -> Self {
        Self { inner }
    }
}

impl PyXetConfig {
    pub fn inner(&self) -> &XetConfig {
        &self.inner
    }

    pub fn into_inner(self) -> XetConfig {
        self.inner
    }
}

#[pymethods]
impl PyXetConfig {
    #[new]
    fn py_new() -> Self {
        Self {
            inner: XetConfig::new(),
        }
    }

    /// Return a new XetConfig with one or more values updated.
    ///
    /// Can be called in two ways:
    ///   config.with_config("group.field", value)          -- single update
    ///   config.with_config({"group.field": value, ...})   -- batch update
    #[pyo3(name = "with_config")]
    #[pyo3(signature = (name_or_dict, value=None))]
    fn py_with_config(&self, name_or_dict: &Bound<'_, PyAny>, value: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let mut new_inner = self.inner.clone();

        if let Ok(dict) = name_or_dict.downcast::<PyDict>() {
            if value.is_some() {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "with_config(dict) does not accept a second argument",
                ));
            }
            for (key, val) in dict.iter() {
                let key_str: String = key.extract()?;
                new_inner.update_field_from_python(&key_str, &val)?;
            }
        } else {
            let name: String = name_or_dict.extract()?;
            let val = value.ok_or_else(|| {
                pyo3::exceptions::PyTypeError::new_err("with_config(name, value) requires a value argument")
            })?;
            new_inner.update_field_from_python(&name, val)?;
        }

        Ok(Self { inner: new_inner })
    }

    /// Get a configuration value as its native Python type by dotted path
    /// (e.g. "data.max_concurrent_file_ingestion").
    #[pyo3(name = "get")]
    fn py_get(&self, py: Python<'_>, path: &str) -> PyResult<Py<PyAny>> {
        self.inner.get_field_to_python(path, py)
    }

    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        self.inner
            .get_field_to_python(key, py)
            .map_err(|_| pyo3::exceptions::PyKeyError::new_err(key.to_owned()))
    }

    /// Return all (key, value) pairs as a list of tuples.
    /// Keys are dotted paths like "data.max_concurrent_file_ingestion".
    fn items(&self, py: Python<'_>) -> PyResult<Vec<(String, Py<PyAny>)>> {
        self.inner.all_items_to_python(py)
    }

    /// Return all dotted-path keys.
    fn keys(&self) -> Vec<String> {
        self.inner.all_keys()
    }

    fn __len__(&self) -> usize {
        self.inner.all_keys().len()
    }

    fn __iter__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyXetConfigIter>> {
        let items = slf.inner.all_items_to_python(py)?;
        Py::new(py, PyXetConfigIter { items, index: 0 })
    }

    fn __repr__(&self) -> String {
        format!("XetConfig({:?})", self.inner)
    }

    fn __str__(&self) -> String {
        format!("{:?}", self.inner)
    }
}

#[pyclass]
pub(crate) struct PyXetConfigIter {
    items: Vec<(String, Py<PyAny>)>,
    index: usize,
}

#[pymethods]
impl PyXetConfigIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> Option<(String, Py<PyAny>)> {
        if self.index < self.items.len() {
            let (key, value) = &self.items[self.index];
            self.index += 1;
            Some((key.clone(), value.clone_ref(py)))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use pyo3::exceptions::{PyTypeError, PyValueError};
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyString};
    use xet_runtime::config::XetConfig;

    use super::*;

    #[test]
    fn from_into_roundtrip_preserves_xet_config() {
        let cfg = XetConfig::default();
        let py_cfg = PyXetConfig::from(cfg.clone());
        assert_eq!(format!("{:?}", py_cfg.into_inner()), format!("{:?}", cfg));
    }

    #[test]
    fn py_with_config_single_updates_inner() {
        Python::attach(|py| {
            let base = PyXetConfig::from(XetConfig::new());
            let original = base.inner().data.max_concurrent_file_ingestion;
            let key = PyString::new(py, "data.max_concurrent_file_ingestion");
            let val = 17usize.into_pyobject(py).unwrap();
            let updated = base
                .py_with_config(key.as_any(), Some(&val))
                .expect("with_config single-arg form");
            assert_eq!(updated.inner().data.max_concurrent_file_ingestion, 17);
            assert_eq!(base.inner().data.max_concurrent_file_ingestion, original);
        });
    }

    #[test]
    fn py_with_config_dict_updates_inner() {
        Python::attach(|py| {
            let base = PyXetConfig::from(XetConfig::new());
            let dict = PyDict::new(py);
            dict.set_item("data.max_concurrent_file_ingestion", 21usize)
                .expect("dict set_item");
            let updated = base.py_with_config(dict.as_any(), None).expect("with_config dict form");
            assert_eq!(updated.inner().data.max_concurrent_file_ingestion, 21);
        });
    }

    #[test]
    fn py_with_config_dict_rejects_second_positional_argument() {
        Python::attach(|py| {
            let base = PyXetConfig::from(XetConfig::new());
            let dict = PyDict::new(py);
            dict.set_item("data.max_concurrent_file_ingestion", 1usize).unwrap();
            let dup = PyString::new(py, "x");
            match base.py_with_config(dict.as_any(), Some(dup.as_any())) {
                Ok(_) => panic!("expected TypeError"),
                Err(err) => assert!(err.is_instance_of::<PyTypeError>(py)),
            }
        });
    }

    #[test]
    fn py_with_config_name_requires_value() {
        Python::attach(|py| {
            let base = PyXetConfig::from(XetConfig::new());
            let key = PyString::new(py, "data.max_concurrent_file_ingestion");
            match base.py_with_config(key.as_any(), None) {
                Ok(_) => panic!("expected TypeError"),
                Err(err) => assert!(err.is_instance_of::<PyTypeError>(py)),
            }
        });
    }

    #[test]
    fn py_with_config_unknown_path_errors() {
        Python::attach(|py| {
            let base = PyXetConfig::from(XetConfig::new());
            let key = PyString::new(py, "not_a_real_group.some_field");
            let val = 1i64.into_pyobject(py).unwrap();
            match base.py_with_config(key.as_any(), Some(&val)) {
                Ok(_) => panic!("expected ValueError"),
                Err(err) => assert!(err.is_instance_of::<PyValueError>(py)),
            }
        });
    }

    #[test]
    fn keys_include_known_setting() {
        let cfg = PyXetConfig::from(XetConfig::new());
        assert!(cfg.keys().contains(&String::from("data.max_concurrent_file_ingestion")));
    }

    #[test]
    fn items_count_matches_keys_len_under_gil() {
        Python::attach(|py| {
            let cfg = PyXetConfig::from(XetConfig::new());
            let k = cfg.keys().len();
            assert_eq!(cfg.items(py).expect("items").len(), k);
        });
    }

    #[test]
    fn py_get_reads_back_roundtrip_via_python_extract() {
        Python::attach(|py| {
            let cfg = PyXetConfig::from(XetConfig::new());
            let key = PyString::new(py, "data.max_concurrent_file_ingestion");
            let val = 3usize.into_pyobject(py).unwrap();
            let updated = cfg.py_with_config(key.as_any(), Some(&val)).unwrap();
            let got = updated.py_get(py, "data.max_concurrent_file_ingestion").unwrap();
            let py_int: isize = got.extract(py).unwrap();
            assert_eq!(py_int, 3);
        });
    }
}
