use pyo3::conversion::IntoPyObjectExt;
use pyo3::prelude::*;

use crate::utils::ByteSize;
#[cfg(not(target_family = "wasm"))]
use crate::utils::TemplatedPathBuf;

/// Trait for converting config values to/from Python objects.
///
/// Each config field type implements this to provide native Python type mappings:
/// - Numeric types <-> Python int/float
/// - String <-> Python str
/// - bool <-> Python bool
/// - Duration <-> Python datetime.timedelta
/// - ByteSize <-> Python int (bytes)
/// - Option<T> <-> Optional[T]
pub trait PythonConfigValue {
    fn to_python(&self, py: Python<'_>) -> Py<PyAny>;
    fn from_python(obj: &Bound<'_, PyAny>) -> PyResult<Self>
    where
        Self: Sized;
}

macro_rules! impl_python_extract {
    ($($ty:ty),+) => {
        $(
            impl PythonConfigValue for $ty {
                fn to_python(&self, py: Python<'_>) -> Py<PyAny> {
                    self.into_py_any(py).expect("Python conversion")
                }
                fn from_python(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
                    obj.extract()
                }
            }
        )+
    };
}

impl_python_extract!(usize, u8, u16, u32, u64, isize, i8, i16, i32, i64, f32, f64, bool, String, std::time::Duration);

impl PythonConfigValue for ByteSize {
    fn to_python(&self, py: Python<'_>) -> Py<PyAny> {
        self.as_u64().into_py_any(py).expect("Python conversion")
    }

    fn from_python(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(v) = obj.extract::<u64>() {
            Ok(ByteSize::new(v))
        } else if let Ok(s) = obj.extract::<String>() {
            s.parse::<ByteSize>()
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Invalid byte size: {e}")))
        } else {
            Err(pyo3::exceptions::PyTypeError::new_err("Expected int (bytes) or string (e.g. '8mb')"))
        }
    }
}

impl<T: PythonConfigValue> PythonConfigValue for Option<T> {
    fn to_python(&self, py: Python<'_>) -> Py<PyAny> {
        match self {
            Some(v) => v.to_python(py),
            None => py.None(),
        }
    }

    fn from_python(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        if obj.is_none() {
            Ok(None)
        } else {
            T::from_python(obj).map(Some)
        }
    }
}

#[cfg(not(target_family = "wasm"))]
impl PythonConfigValue for TemplatedPathBuf {
    fn to_python(&self, py: Python<'_>) -> Py<PyAny> {
        self.template_string().into_py_any(py).expect("Python conversion")
    }

    fn from_python(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let s: String = obj.extract()?;
        Ok(TemplatedPathBuf::new(s))
    }
}
