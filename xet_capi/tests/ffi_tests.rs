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
        xet_capi::xet_upload_commit_commit as *const (),
        xet_capi::xet_file_upload_finalize as *const (),
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
        xet_capi::xet_stream_upload_write as *const (),
        xet_capi::xet_stream_upload_finish as *const (),
        xet_capi::xet_stream_upload_free as *const (),
    ];
}

#[test]
fn download_file_symbols_link() {
    let _f: [*const (); 6] = [
        xet_capi::xet_file_download_group_download_to_path as *const (),
        xet_capi::xet_file_download_group_finish as *const (),
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
        xet_capi::xet_download_stream_next as *const (),
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
    let _f: [*const (); 7] = [
        xet_capi::xet_commit_report_file_count as *const (),
        xet_capi::xet_commit_report_file_at as *const (),
        xet_capi::xet_commit_report_dedup as *const (),
        xet_capi::xet_commit_report_progress as *const (),
        xet_capi::xet_commit_report_free as *const (),
        xet_capi::xet_download_group_report_count as *const (),
        xet_capi::xet_download_group_report_at as *const (),
    ];
}

#[test]
fn header_is_up_to_date() {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let committed = std::fs::read_to_string(crate_dir.join("include/hf_xet.h")).unwrap();
    let generated = {
        let mut buf = Vec::new();
        cbindgen::generate(&crate_dir).expect("cbindgen generate").write(&mut buf);
        String::from_utf8(buf).unwrap()
    };
    assert_eq!(
        committed.trim(),
        generated.trim(),
        "include/hf_xet.h is stale — run `cargo build -p xet_capi` and commit the result"
    );
}

#[test]
fn header_declares_blocking_fns() {
    let header = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/include/hf_xet.h")).unwrap();
    for sym in [
        "xet_file_upload_finalize",
        "xet_upload_commit_commit",
        "xet_stream_upload_write",
        "xet_stream_upload_finish",
        "xet_file_download_group_finish",
        "xet_download_stream_next",
        "xet_download_stream_task_id",
    ] {
        assert!(header.contains(sym), "committed header is missing declaration for {sym}");
    }
    for sym in ["xet_op_poll", "xet_op_free", "xet_op_take", "XetPollState", "XetOp"] {
        assert!(!header.contains(sym), "committed header still declares removed symbol {sym}");
    }
}

#[test]
fn c_smoke_compiles() {
    // Compile smoke.c against the committed header via the cc crate. A full
    // link+run against the staticlib is left to CI/CGo; compiling the TU
    // against the header already catches ABI/header regressions.
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let out_dir = tempfile::tempdir().unwrap();
    // `cc` normally reads TARGET/HOST/OPT_LEVEL from the build-script environment.
    // Tests do not run under a build script, so provide them explicitly.
    let target = env!("CAPI_TARGET");
    let objs = cc::Build::new()
        .file(crate_dir.join("tests/smoke.c"))
        .include(crate_dir.join("include"))
        .cargo_metadata(false)
        .warnings(true)
        .target(target)
        .host(target)
        .opt_level(0)
        .out_dir(out_dir.path())
        .compile_intermediates();
    assert!(!objs.is_empty(), "smoke.c did not compile to an object");
}

#[cfg(feature = "simulation")]
#[test]
fn e2e_upload_then_download_via_ffi() {
    use std::ffi::CString;

    unsafe {
        let dir = tempfile::tempdir().unwrap();
        let endpoint = CString::new(format!("local://{}", dir.path().join("cas").display())).unwrap();

        let mut err: *mut XetError = std::ptr::null_mut();
        let mut session: *mut XetSession = std::ptr::null_mut();
        assert_eq!(xet_session_new(&mut session, &mut err), XetStatus::XetOk);

        let cfg = XetAuthConfig {
            endpoint: endpoint.as_ptr(),
            token: std::ptr::null(),
            token_expiry: 0,
            token_refresh_url: std::ptr::null(),
            refresh_headers: std::ptr::null(),
            refresh_header_count: 0,
        };

        // Upload
        let mut commit: *mut XetUploadCommit = std::ptr::null_mut();
        assert_eq!(xet_session_new_upload_commit(session, &cfg, &mut commit, &mut err), XetStatus::XetOk);

        let payload = b"hello xet c api";
        let name = CString::new("greeting.txt").unwrap();
        let mut upload: *mut XetFileUpload = std::ptr::null_mut();
        assert_eq!(
            xet_upload_commit_upload_bytes(
                commit,
                payload.as_ptr(),
                payload.len(),
                name.as_ptr(),
                XetSha256Policy::XetSha256Compute,
                std::ptr::null(),
                &mut upload,
                &mut err,
            ),
            XetStatus::XetOk
        );

        let mut meta: *mut XetFileMetadataHandle = std::ptr::null_mut();
        assert_eq!(xet_file_upload_finalize(upload, &mut meta, &mut err), XetStatus::XetOk);

        let mut report: *mut XetCommitReportHandle = std::ptr::null_mut();
        assert_eq!(xet_upload_commit_commit(commit, &mut report, &mut err), XetStatus::XetOk);
        assert!(xet_commit_report_file_count(report) >= 1);

        // Build a file_info from the uploaded metadata
        let hash_ptr = xet_file_metadata_hash(meta);
        assert!(!hash_ptr.is_null());
        let size = xet_file_metadata_file_size(meta);
        let mut fi: *mut XetFileInfo = std::ptr::null_mut();
        assert_eq!(xet_file_info_new(hash_ptr, size, &mut fi, &mut err), XetStatus::XetOk);

        // Download to a path
        let mut group: *mut XetFileDownloadGroup = std::ptr::null_mut();
        assert_eq!(xet_session_new_file_download_group(session, &cfg, &mut group, &mut err), XetStatus::XetOk);

        let dest_path = dir.path().join("out.txt");
        let dest = CString::new(dest_path.to_str().unwrap()).unwrap();
        let mut dl: *mut XetFileDownload = std::ptr::null_mut();
        assert_eq!(
            xet_file_download_group_download_to_path(group, fi, dest.as_ptr(), &mut dl, &mut err),
            XetStatus::XetOk
        );

        // Poll progress from another thread while the main thread blocks in
        // xet_file_download_group_finish. Raw handle pointers are not Send, so
        // pass the address as usize.
        let group_addr = group as usize;
        let poller = std::thread::spawn(move || {
            let group = group_addr as *const XetFileDownloadGroup;
            let mut polls = 0u32;
            let mut p = XetProgress {
                total_bytes: 0,
                total_bytes_completed: 0,
                total_transfer_bytes: 0,
                total_transfer_bytes_completed: 0,
            };
            for _ in 0..200 {
                if unsafe { xet_file_download_group_progress(group, &mut p) } == XetStatus::XetOk {
                    polls += 1;
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            polls
        });

        let mut dreport: *mut XetDownloadGroupReportHandle = std::ptr::null_mut();
        assert_eq!(xet_file_download_group_finish(group, &mut dreport, &mut err), XetStatus::XetOk);
        assert!(poller.join().unwrap() > 0, "progress polling from a second thread never succeeded");

        // Verify round-trip
        let got = std::fs::read(&dest_path).unwrap();
        assert_eq!(got, payload);

        // Cleanup
        xet_file_metadata_free(meta);
        xet_commit_report_free(report);
        xet_download_group_report_free(dreport);
        xet_file_info_free(fi);
        xet_file_upload_free(upload);
        xet_file_download_free(dl);
        xet_upload_commit_free(commit);
        xet_file_download_group_free(group);
        xet_session_free(session);
    }
}
