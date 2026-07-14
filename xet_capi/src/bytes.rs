use std::os::raw::c_uchar;

use bytes::Bytes;

use crate::handle::free_handle;

/// An owned byte buffer produced by streaming downloads. Read via
/// [`xet_bytes_data`] / [`xet_bytes_len`]; free with [`xet_bytes_free`].
pub struct XetBytes {
    data: Bytes,
}

impl XetBytes {
    pub(crate) fn new(data: Bytes) -> Self {
        Self { data }
    }
}

/// # Safety
/// `b` must be null or a valid pointer to a live `XetBytes` produced by this crate.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_bytes_data(b: *const XetBytes) -> *const c_uchar {
    match unsafe { b.as_ref() } {
        Some(b) => b.data.as_ptr(),
        None => std::ptr::null(),
    }
}

/// # Safety
/// `b` must be null or a valid pointer to a live `XetBytes` produced by this crate.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn xet_bytes_len(b: *const XetBytes) -> usize {
    match unsafe { b.as_ref() } {
        Some(b) => b.data.len(),
        None => 0,
    }
}

/// Free a `XetBytes` buffer. Safe to call with null.
#[unsafe(no_mangle)]
pub extern "C" fn xet_bytes_free(b: *mut XetBytes) {
    free_handle(b);
}

/// Test-only constructor used by ffi_tests.
#[unsafe(no_mangle)]
pub extern "C" fn xet_test_make_bytes() -> *mut XetBytes {
    crate::handle::into_handle(XetBytes::new(Bytes::from_static(&[1, 2, 3])))
}
