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
