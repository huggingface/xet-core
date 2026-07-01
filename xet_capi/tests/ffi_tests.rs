use std::ffi::CStr;

use xet_capi::*;

#[test]
fn error_message_and_code_roundtrip() {
    // Build an error from a xet::XetError and read it back through the C API.
    let mut err: *mut XetError = std::ptr::null_mut();
    unsafe {
        xet_test_make_auth_error(&mut err);
        assert!(!err.is_null());
        assert_eq!(xet_error_code(err), XetStatus::XetErrAuth);
        let msg = CStr::from_ptr(xet_error_message(err)).to_str().unwrap();
        assert!(msg.contains("nope"));
        xet_error_free(err);
    }
}

#[test]
fn bytes_roundtrip() {
    unsafe {
        let b = xet_test_make_bytes(); // XetBytes* holding [1,2,3]
        assert!(!b.is_null());
        assert_eq!(xet_bytes_len(b), 3);
        let data = std::slice::from_raw_parts(xet_bytes_data(b), 3);
        assert_eq!(data, &[1u8, 2, 3]);
        xet_bytes_free(b);
    }
}
