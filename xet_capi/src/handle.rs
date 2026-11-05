//! Helpers for boxing Rust values into opaque C handles and freeing them.

/// Box `value` and return a raw pointer C code will treat as opaque.
pub(crate) fn into_handle<T>(value: T) -> *mut T {
    Box::into_raw(Box::new(value))
}

/// Reconstruct and drop a handle. Null-safe.
pub(crate) fn free_handle<T>(ptr: *mut T) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr) });
    }
}
