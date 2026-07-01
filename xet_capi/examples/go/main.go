// Upload + download round-trip against a real Hugging Face Xet repo, using the
// hf_xet C API from Go via cgo.
//
// Auth: reads $HF_TOKEN from the environment.
// Build/run: see the README in this directory (`go run .`).
package main

/*
#cgo CFLAGS: -I${SRCDIR}/../../include
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -lxet_capi -Wl,-rpath,${SRCDIR}/../../../target/release
#include <stdlib.h>
#include "hf_xet.h"
*/
import "C"

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"runtime"
	"time"
	"unsafe"
)

const (
	defaultRepo = "assafvayner/xet-c-api-test"
	repoType    = "datasets"
	revision    = "main"
	hub         = "https://huggingface.co"
)

// xetError turns a *C.XetError into a Go error, freeing the C error.
func xetError(ctx string, err *C.XetError) error {
	if err == nil {
		return fmt.Errorf("%s: (no error detail)", ctx)
	}
	msg := C.GoString(C.xet_error_message(err))
	code := int(C.xet_error_code(err))
	C.xet_error_free(err)
	return fmt.Errorf("%s: [%d] %s", ctx, code, msg)
}

// drive polls op to completion, returning the op's error on failure. The op is
// not freed here; the caller still owns it.
func drive(op *C.XetOp) error {
	for {
		switch C.xet_op_poll(op) {
		case C.XetPollState_XetPollPending:
			time.Sleep(20 * time.Millisecond)
		case C.XetPollState_XetPollError:
			var err *C.XetError
			C.xet_op_take_error(op, &err)
			return xetError("operation failed", err)
		default: // XetPollReady
			return nil
		}
	}
}

func run(repo, token string) error {
	fmt.Printf("hf_xet Go/cgo example (version %s)\n", C.GoString(C.xet_version()))
	fmt.Printf("repo: %s (%s)\n\n", repo, repoType)

	writeURL := C.CString(fmt.Sprintf("%s/api/%s/%s/xet-write-token/%s", hub, repoType, repo, revision))
	readURL := C.CString(fmt.Sprintf("%s/api/%s/%s/xet-read-token/%s", hub, repoType, repo, revision))
	authKey := C.CString("Authorization")
	bearer := C.CString("Bearer " + token)
	defer C.free(unsafe.Pointer(writeURL))
	defer C.free(unsafe.Pointer(readURL))
	defer C.free(unsafe.Pointer(authKey))
	defer C.free(unsafe.Pointer(bearer))

	header := C.XetHeader{key: authKey, value: bearer}
	// The auth config embeds &header, so passing &cfg to C would hand cgo a Go
	// pointer to another (unpinned) Go pointer. Pin `header` for the duration.
	var pinner runtime.Pinner
	defer pinner.Unpin()
	pinner.Pin(&header)

	mkCfg := func(refreshURL *C.char) C.XetAuthConfig {
		// endpoint/token stay NULL; the refresh response provides them.
		return C.XetAuthConfig{
			token_refresh_url:    refreshURL,
			refresh_headers:      &header,
			refresh_header_count: 1,
		}
	}

	var xerr *C.XetError

	var session *C.XetSession
	if C.xet_session_new(&session, &xerr) != C.XetStatus_XetOk {
		return xetError("xet_session_new", xerr)
	}
	defer C.xet_session_free(session)

	// ---- Upload ----
	writeCfg := mkCfg(writeURL)
	var commit *C.XetUploadCommit
	if C.xet_session_new_upload_commit(session, &writeCfg, &commit, &xerr) != C.XetStatus_XetOk {
		return xetError("xet_session_new_upload_commit", xerr)
	}
	defer C.xet_upload_commit_free(commit)

	// Random payload so each run uploads distinct bytes.
	payload := make([]byte, 128*1024)
	if _, err := rand.Read(payload); err != nil {
		return err
	}

	name := C.CString("random.bin")
	defer C.free(unsafe.Pointer(name))
	var upload *C.XetFileUpload
	if C.xet_upload_commit_upload_bytes(commit, (*C.uint8_t)(unsafe.Pointer(&payload[0])),
		C.uintptr_t(len(payload)), name, C.XetSha256Policy_XetSha256Compute, nil, &upload, &xerr) != C.XetStatus_XetOk {
		return xetError("xet_upload_commit_upload_bytes", xerr)
	}
	defer C.xet_file_upload_free(upload)

	var op *C.XetOp
	if C.xet_file_upload_finalize_start(upload, &op, &xerr) != C.XetStatus_XetOk {
		return xetError("xet_file_upload_finalize_start", xerr)
	}
	defer C.xet_op_free(op)
	if err := drive(op); err != nil {
		return err
	}
	var meta *C.XetFileMetadataHandle
	if C.xet_op_take_file_metadata(op, &meta, &xerr) != C.XetStatus_XetOk {
		return xetError("xet_op_take_file_metadata", xerr)
	}
	defer C.xet_file_metadata_free(meta)

	hash := C.GoString(C.xet_file_metadata_hash(meta))
	size := uint64(C.xet_file_metadata_file_size(meta))
	fmt.Printf("uploaded %d bytes\n  hash: %s\n  size: %d\n", len(payload), hash, size)

	var commitOp *C.XetOp
	if C.xet_upload_commit_commit_start(commit, &commitOp, &xerr) != C.XetStatus_XetOk {
		return xetError("xet_upload_commit_commit_start", xerr)
	}
	defer C.xet_op_free(commitOp)
	if err := drive(commitOp); err != nil {
		return err
	}
	var report *C.XetCommitReportHandle
	if C.xet_op_take_commit_report(commitOp, &report, &xerr) != C.XetStatus_XetOk {
		return xetError("xet_op_take_commit_report", xerr)
	}
	defer C.xet_commit_report_free(report)
	var metrics C.XetDedupMetrics
	if C.xet_commit_report_dedup(report, &metrics) == C.XetStatus_XetOk {
		fmt.Printf("  committed: %d new bytes, %d deduped bytes\n\n", uint64(metrics.new_bytes), uint64(metrics.deduped_bytes))
	}

	// ---- Download ----
	readCfg := mkCfg(readURL)
	var group *C.XetFileDownloadGroup
	if C.xet_session_new_file_download_group(session, &readCfg, &group, &xerr) != C.XetStatus_XetOk {
		return xetError("xet_session_new_file_download_group", xerr)
	}
	defer C.xet_file_download_group_free(group)

	cHash := C.CString(hash)
	defer C.free(unsafe.Pointer(cHash))
	var fileInfo *C.XetFileInfo
	if C.xet_file_info_new(cHash, C.uint64_t(size), &fileInfo, &xerr) != C.XetStatus_XetOk {
		return xetError("xet_file_info_new", xerr)
	}
	defer C.xet_file_info_free(fileInfo)

	dest := "downloaded.bin"
	cDest := C.CString(dest)
	defer C.free(unsafe.Pointer(cDest))
	var download *C.XetFileDownload
	if C.xet_file_download_group_download_to_path(group, fileInfo, cDest, &download, &xerr) != C.XetStatus_XetOk {
		return xetError("xet_file_download_group_download_to_path", xerr)
	}
	defer C.xet_file_download_free(download)

	var dlOp *C.XetOp
	if C.xet_file_download_group_finish_start(group, &dlOp, &xerr) != C.XetStatus_XetOk {
		return xetError("xet_file_download_group_finish_start", xerr)
	}
	defer C.xet_op_free(dlOp)
	if err := drive(dlOp); err != nil {
		return err
	}
	var dlReport *C.XetDownloadGroupReportHandle
	if C.xet_op_take_download_report(dlOp, &dlReport, &xerr) != C.XetStatus_XetOk {
		return xetError("xet_op_take_download_report", xerr)
	}
	defer C.xet_download_group_report_free(dlReport)

	// ---- Verify ----
	got, err := os.ReadFile(dest)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, payload) {
		return fmt.Errorf("MISMATCH: read %d of %d bytes", len(got), len(payload))
	}
	fmt.Printf("downloaded %d bytes -> %s\nSUCCESS: round-trip content matches\n", len(got), dest)
	return nil
}

func main() {
	repo := defaultRepo
	if len(os.Args) > 1 {
		repo = os.Args[1]
	}
	token := os.Getenv("HF_TOKEN")
	if token == "" {
		fmt.Fprintln(os.Stderr, "HF_TOKEN environment variable is not set")
		os.Exit(1)
	}
	if err := run(repo, token); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
