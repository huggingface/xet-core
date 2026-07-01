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

#[test]
fn op_poll_completes_and_takes_void() {
    unsafe {
        let op = xet_test_make_void_op(); // completes after ~50ms
        let mut state = xet_op_poll(op);
        let mut spins = 0;
        while state == XetPollState::XetPollPending {
            std::thread::sleep(std::time::Duration::from_millis(5));
            state = xet_op_poll(op);
            spins += 1;
            assert!(spins < 1000, "op never completed");
        }
        assert_eq!(state, XetPollState::XetPollReady);
        let mut err: *mut XetError = std::ptr::null_mut();
        assert_eq!(xet_op_take_void(op, &mut err), XetStatus::XetOk);
        assert!(err.is_null());
    }
}

#[test]
fn op_error_is_taken() {
    unsafe {
        let op = xet_test_make_error_op();
        while xet_op_poll(op) == XetPollState::XetPollPending {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_eq!(xet_op_poll(op), XetPollState::XetPollError);
        let mut err: *mut XetError = std::ptr::null_mut();
        assert_eq!(xet_op_take_error(op, &mut err), XetStatus::XetOk);
        assert!(!err.is_null());
        assert_eq!(xet_error_code(err), XetStatus::XetErr);
        xet_error_free(err);
    }
}

#[test]
fn session_new_and_free() {
    unsafe {
        let mut session: *mut XetSession = std::ptr::null_mut();
        let mut err: *mut XetError = std::ptr::null_mut();
        assert_eq!(xet_session_new(&mut session, &mut err), XetStatus::XetOk);
        assert!(!session.is_null());
        assert!(err.is_null());
        xet_session_free(session);
    }
}

#[test]
fn file_info_new_and_free() {
    use std::ffi::CString;
    unsafe {
        let hash = CString::new("abc123").unwrap();
        let mut fi: *mut XetFileInfo = std::ptr::null_mut();
        let mut err: *mut XetError = std::ptr::null_mut();
        assert_eq!(xet_file_info_new(hash.as_ptr(), 42, &mut fi, &mut err), XetStatus::XetOk);
        assert!(!fi.is_null());
        xet_file_info_free(fi);
    }
}
