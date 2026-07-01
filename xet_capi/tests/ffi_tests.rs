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

#[test]
fn upload_symbols_link() {
    // Compile/link check: take fn pointers so the linker resolves them.
    let _f: [*const (); 9] = [
        xet_capi::xet_upload_commit_upload_from_path as *const (),
        xet_capi::xet_upload_commit_upload_bytes as *const (),
        xet_capi::xet_upload_commit_commit_start as *const (),
        xet_capi::xet_file_upload_finalize_start as *const (),
        xet_capi::xet_upload_commit_progress as *const (),
        xet_capi::xet_upload_commit_abort as *const (),
        xet_capi::xet_file_metadata_hash as *const (),
        xet_capi::xet_file_metadata_file_size as *const (),
        xet_capi::xet_file_metadata_free as *const (),
    ];
}

#[test]
fn stream_upload_symbols_link() {
    let _f: [*const (); 4] = [
        xet_capi::xet_upload_commit_upload_stream as *const (),
        xet_capi::xet_stream_upload_write_start as *const (),
        xet_capi::xet_stream_upload_finish_start as *const (),
        xet_capi::xet_stream_upload_free as *const (),
    ];
}

#[test]
fn download_file_symbols_link() {
    let _f: [*const (); 6] = [
        xet_capi::xet_file_download_group_download_to_path as *const (),
        xet_capi::xet_file_download_group_finish_start as *const (),
        xet_capi::xet_file_download_group_progress as *const (),
        xet_capi::xet_file_download_group_abort as *const (),
        xet_capi::xet_file_download_task_id as *const (),
        xet_capi::xet_file_download_group_free as *const (),
    ];
}

#[test]
fn download_stream_symbols_link() {
    let _f: [*const (); 5] = [
        xet_capi::xet_download_stream_group_download_stream as *const (),
        xet_capi::xet_download_stream_group_download_unordered_stream as *const (),
        xet_capi::xet_download_stream_next_start as *const (),
        xet_capi::xet_download_stream_cancel as *const (),
        xet_capi::xet_download_stream_free as *const (),
    ];
}

#[test]
fn session_builder_symbols_link() {
    let _f: [*const (); 3] = [
        xet_capi::xet_session_new_upload_commit as *const (),
        xet_capi::xet_session_new_file_download_group as *const (),
        xet_capi::xet_session_new_download_stream_group as *const (),
    ];
}

#[test]
fn reports_symbols_link() {
    let _f: [*const (); 9] = [
        xet_capi::xet_commit_report_file_count as *const (),
        xet_capi::xet_commit_report_file_at as *const (),
        xet_capi::xet_commit_report_dedup as *const (),
        xet_capi::xet_commit_report_progress as *const (),
        xet_capi::xet_commit_report_free as *const (),
        xet_capi::xet_download_group_report_count as *const (),
        xet_capi::xet_download_group_report_at as *const (),
        xet_capi::xet_op_take_commit_report as *const (),
        xet_capi::xet_op_take_download_report as *const (),
    ];
}
