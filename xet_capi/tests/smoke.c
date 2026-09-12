#include "hf_xet.h"
#include <assert.h>
#include <stdio.h>

/* Exercises the ABI surface without network: build a session and a file_info,
   confirm error handling round-trips. Returns 0 on success. */
int run_smoke(void) {
    XetSession *session = NULL;
    XetError *err = NULL;
    if (xet_session_new(&session, &err) != XetStatus_XetOk) {
        fprintf(stderr, "session_new failed: %s\n", xet_error_message(err));
        xet_error_free(err);
        return 1;
    }

    XetFileInfo *fi = NULL;
    if (xet_file_info_new("deadbeef", 123, &fi, &err) != XetStatus_XetOk) {
        xet_error_free(err);
        xet_session_free(session);
        return 2;
    }

    xet_file_info_free(fi);
    xet_session_free(session);
    return 0;
}

/* Entry point so CI can link this TU against the real xet_capi library and run
   it, exercising actual ABI linkage (the Rust `c_smoke_compiles` test only
   compiles the translation unit against the header). */
int main(void) {
    return run_smoke();
}
