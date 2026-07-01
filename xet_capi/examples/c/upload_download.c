/*
 * Minimal upload + download round-trip against a real Hugging Face Xet repo,
 * using only the hf_xet C API.
 *
 * Flow:
 *   1. Create a session.
 *   2. Build an upload commit whose auth is a Hub xet-write-token refresh URL
 *      (the library fetches the CAS endpoint + short-lived token itself).
 *   3. Upload random bytes -> finalize -> read back the content hash + size.
 *   4. Commit (registers the shard so the hash is reconstructable).
 *   5. Build a download group from a xet-read-token refresh URL.
 *   6. Download by hash to a file and verify the bytes match.
 *
 * Auth: reads $HF_TOKEN from the environment.
 *
 * Build/run: see the Makefile in this directory (`make run`).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "hf_xet.h"

/* Repo to round-trip through. Override with argv[1] (e.g. "user/name"). */
#define DEFAULT_REPO "assafvayner/xet-c-api-test"
#define REPO_TYPE "datasets"
#define REVISION "main"
#define HUB "https://huggingface.co"

/* Print the message carried by `err` (if any), free it, and return -1. */
static int fail(const char *ctx, XetError *err) {
    if (err) {
        fprintf(stderr, "%s: [%d] %s\n", ctx, xet_error_code(err), xet_error_message(err));
        xet_error_free(err);
    } else {
        fprintf(stderr, "%s: (no error detail)\n", ctx);
    }
    return -1;
}

/*
 * Poll `op` to completion. On success returns 0. On failure prints the op's
 * error and returns -1. The op is NOT consumed here; the caller still owns it
 * and must take its result (or free it).
 */
static int drive(XetOp *op) {
    XetPollState state;
    while ((state = xet_op_poll(op)) == XetPollState_XetPollPending) {
        struct timespec ts = {0, 20 * 1000 * 1000}; /* 20ms */
        nanosleep(&ts, NULL);
    }
    if (state == XetPollState_XetPollError) {
        XetError *err = NULL;
        xet_op_take_error(op, &err);
        return fail("operation failed", err);
    }
    return 0;
}

int main(int argc, char **argv) {
    const char *repo = argc > 1 ? argv[1] : DEFAULT_REPO;

    const char *hf_token = getenv("HF_TOKEN");
    if (!hf_token || !*hf_token) {
        fprintf(stderr, "HF_TOKEN environment variable is not set\n");
        return 1;
    }

    printf("hf_xet C example (version %s)\n", xet_version());
    printf("repo: %s (%s)\n\n", repo, REPO_TYPE);

    /* Build the two token-refresh URLs and the shared Authorization header. */
    char write_url[512], read_url[512], bearer[4096];
    snprintf(write_url, sizeof write_url, "%s/api/%s/%s/xet-write-token/%s", HUB, REPO_TYPE, repo, REVISION);
    snprintf(read_url, sizeof read_url, "%s/api/%s/%s/xet-read-token/%s", HUB, REPO_TYPE, repo, REVISION);
    snprintf(bearer, sizeof bearer, "Bearer %s", hf_token);

    XetHeader auth_header = {.key = "Authorization", .value = bearer};

    int rc = 1;
    XetError *err = NULL;

    /* Owned handles; freed in reverse order at `cleanup`. */
    XetSession *session = NULL;
    XetUploadCommit *commit = NULL;
    XetFileUpload *upload = NULL;
    XetOp *op = NULL;
    XetFileMetadataHandle *meta = NULL;
    XetOp *commit_op = NULL;
    XetCommitReportHandle *report = NULL;
    XetFileDownloadGroup *group = NULL;
    XetFileInfo *file_info = NULL;
    XetFileDownload *download = NULL;
    XetOp *dl_op = NULL;
    XetDownloadGroupReportHandle *dl_report = NULL;
    unsigned char *payload = NULL;

    if (xet_session_new(&session, &err) != XetStatus_XetOk) {
        fail("xet_session_new", err);
        goto cleanup;
    }

    /* ---- Upload ---- */
    XetAuthConfig write_cfg = {
        .endpoint = NULL, /* filled in by the refresh response */
        .token = NULL,
        .token_expiry = 0,
        .token_refresh_url = write_url,
        .refresh_headers = &auth_header,
        .refresh_header_count = 1,
    };
    if (xet_session_new_upload_commit(session, &write_cfg, &commit, &err) != XetStatus_XetOk) {
        fail("xet_session_new_upload_commit", err);
        goto cleanup;
    }

    /* Generate random content so each run uploads distinct bytes. */
    const size_t payload_len = 128 * 1024;
    payload = malloc(payload_len);
    srand((unsigned)time(NULL));
    for (size_t i = 0; i < payload_len; i++) {
        payload[i] = (unsigned char)rand();
    }

    if (xet_upload_commit_upload_bytes(commit, payload, payload_len, "random.bin",
                                       XetSha256Policy_XetSha256Compute, NULL, &upload, &err) != XetStatus_XetOk) {
        fail("xet_upload_commit_upload_bytes", err);
        goto cleanup;
    }

    if (xet_file_upload_finalize_start(upload, &op, &err) != XetStatus_XetOk) {
        fail("xet_file_upload_finalize_start", err);
        goto cleanup;
    }
    if (drive(op) != 0) goto cleanup;
    if (xet_op_take_file_metadata(op, &meta, &err) != XetStatus_XetOk) {
        fail("xet_op_take_file_metadata", err);
        goto cleanup;
    }

    const char *hash = xet_file_metadata_hash(meta);
    uint64_t size = xet_file_metadata_file_size(meta);
    printf("uploaded %zu bytes\n  hash: %s\n  size: %llu\n", payload_len, hash, (unsigned long long)size);

    if (xet_upload_commit_commit_start(commit, &commit_op, &err) != XetStatus_XetOk) {
        fail("xet_upload_commit_commit_start", err);
        goto cleanup;
    }
    if (drive(commit_op) != 0) goto cleanup;
    if (xet_op_take_commit_report(commit_op, &report, &err) != XetStatus_XetOk) {
        fail("xet_op_take_commit_report", err);
        goto cleanup;
    }
    XetDedupMetrics metrics;
    if (xet_commit_report_dedup(report, &metrics) == XetStatus_XetOk) {
        printf("  committed: %llu new bytes, %llu deduped bytes\n\n",
               (unsigned long long)metrics.new_bytes, (unsigned long long)metrics.deduped_bytes);
    }

    /* ---- Download ---- */
    XetAuthConfig read_cfg = write_cfg;
    read_cfg.token_refresh_url = read_url;

    if (xet_session_new_file_download_group(session, &read_cfg, &group, &err) != XetStatus_XetOk) {
        fail("xet_session_new_file_download_group", err);
        goto cleanup;
    }
    if (xet_file_info_new(hash, size, &file_info, &err) != XetStatus_XetOk) {
        fail("xet_file_info_new", err);
        goto cleanup;
    }

    const char *dest = "downloaded.bin";
    if (xet_file_download_group_download_to_path(group, file_info, dest, &download, &err) != XetStatus_XetOk) {
        fail("xet_file_download_group_download_to_path", err);
        goto cleanup;
    }
    if (xet_file_download_group_finish_start(group, &dl_op, &err) != XetStatus_XetOk) {
        fail("xet_file_download_group_finish_start", err);
        goto cleanup;
    }
    if (drive(dl_op) != 0) goto cleanup;
    if (xet_op_take_download_report(dl_op, &dl_report, &err) != XetStatus_XetOk) {
        fail("xet_op_take_download_report", err);
        goto cleanup;
    }

    /* ---- Verify ---- */
    FILE *f = fopen(dest, "rb");
    if (!f) {
        perror("open downloaded file");
        goto cleanup;
    }
    unsigned char *got = malloc(payload_len);
    size_t n = fread(got, 1, payload_len, f);
    fclose(f);
    if (n == payload_len && memcmp(got, payload, payload_len) == 0) {
        printf("downloaded %zu bytes -> %s\nSUCCESS: round-trip content matches\n", n, dest);
        rc = 0;
    } else {
        fprintf(stderr, "MISMATCH: read %zu of %zu bytes\n", n, payload_len);
    }
    free(got);

cleanup:
    free(payload);
    if (dl_report) xet_download_group_report_free(dl_report);
    if (dl_op) xet_op_free(dl_op);
    if (download) xet_file_download_free(download);
    if (file_info) xet_file_info_free(file_info);
    if (group) xet_file_download_group_free(group);
    if (report) xet_commit_report_free(report);
    if (commit_op) xet_op_free(commit_op);
    if (meta) xet_file_metadata_free(meta);
    if (op) xet_op_free(op);
    if (upload) xet_file_upload_free(upload);
    if (commit) xet_upload_commit_free(commit);
    if (session) xet_session_free(session);
    return rc;
}
