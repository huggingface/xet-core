// Upload + download round-trip against a real Hugging Face Xet repo, using the
// hf_xet C API from C++ with RAII wrappers so every handle is freed exactly once.
//
// Auth: reads $HF_TOKEN from the environment.
// Build/run: see the Makefile in this directory (`make run`).

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

extern "C" {
#include "hf_xet.h"
}

namespace {

constexpr const char* kDefaultRepo = "assafvayner/xet-c-api-test";
constexpr const char* kRepoType = "datasets";
constexpr const char* kRevision = "main";
constexpr const char* kHub = "https://huggingface.co";

// Turn a XetError* into an exception message, freeing the error.
[[noreturn]] void throw_xet(const char* ctx, XetError* err) {
    std::string msg = ctx;
    if (err) {
        msg += ": [" + std::to_string(xet_error_code(err)) + "] " + xet_error_message(err);
        xet_error_free(err);
    }
    throw std::runtime_error(msg);
}

// One RAII owner per handle type: a unique_ptr with the matching free fn.
template <typename T, void (*Free)(T*)>
struct Deleter {
    void operator()(T* p) const noexcept {
        if (p) Free(p);
    }
};
template <typename T, void (*Free)(T*)>
using Handle = std::unique_ptr<T, Deleter<T, Free>>;

using Session = Handle<XetSession, xet_session_free>;
using UploadCommit = Handle<XetUploadCommit, xet_upload_commit_free>;
using FileUpload = Handle<XetFileUpload, xet_file_upload_free>;
using Op = Handle<XetOp, xet_op_free>;
using FileMetadata = Handle<XetFileMetadataHandle, xet_file_metadata_free>;
using CommitReport = Handle<XetCommitReportHandle, xet_commit_report_free>;
using DownloadGroup = Handle<XetFileDownloadGroup, xet_file_download_group_free>;
using FileInfo = Handle<XetFileInfo, xet_file_info_free>;
using FileDownload = Handle<XetFileDownload, xet_file_download_free>;
using DownloadReport = Handle<XetDownloadGroupReportHandle, xet_download_group_report_free>;

// Poll an op to completion; throw with the op's error on failure. The op stays
// owned by the caller (its RAII wrapper frees it).
void drive(const Op& op) {
    XetPollState state;
    while ((state = xet_op_poll(op.get())) == XetPollState_XetPollPending) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    if (state == XetPollState_XetPollError) {
        XetError* err = nullptr;
        xet_op_take_error(op.get(), &err);
        throw_xet("operation failed", err);
    }
}

int run(const std::string& repo, const std::string& hf_token) {
    std::cout << "hf_xet C++ example (version " << xet_version() << ")\n"
              << "repo: " << repo << " (" << kRepoType << ")\n\n";

    const std::string base = std::string(kHub) + "/api/" + kRepoType + "/" + repo;
    const std::string write_url = base + "/xet-write-token/" + kRevision;
    const std::string read_url = base + "/xet-read-token/" + kRevision;
    const std::string bearer = "Bearer " + hf_token;

    XetHeader auth_header{"Authorization", bearer.c_str()};
    auto make_cfg = [&](const char* refresh_url) {
        XetAuthConfig cfg{};
        cfg.token_refresh_url = refresh_url;
        cfg.refresh_headers = &auth_header;
        cfg.refresh_header_count = 1;
        return cfg;  // endpoint/token supplied by the refresh response
    };

    XetError* err = nullptr;

    XetSession* raw_session = nullptr;
    if (xet_session_new(&raw_session, &err) != XetStatus_XetOk) throw_xet("xet_session_new", err);
    Session session(raw_session);

    // ---- Upload ----
    XetAuthConfig write_cfg = make_cfg(write_url.c_str());
    XetUploadCommit* raw_commit = nullptr;
    if (xet_session_new_upload_commit(session.get(), &write_cfg, &raw_commit, &err) != XetStatus_XetOk)
        throw_xet("xet_session_new_upload_commit", err);
    UploadCommit commit(raw_commit);

    // Random payload so each run uploads distinct bytes.
    std::vector<std::uint8_t> payload(128 * 1024);
    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> byte(0, 255);
    for (auto& b : payload) b = static_cast<std::uint8_t>(byte(rng));

    XetFileUpload* raw_upload = nullptr;
    if (xet_upload_commit_upload_bytes(commit.get(), payload.data(), payload.size(), "random.bin",
                                       XetSha256Policy_XetSha256Compute, nullptr, &raw_upload, &err) != XetStatus_XetOk)
        throw_xet("xet_upload_commit_upload_bytes", err);
    FileUpload upload(raw_upload);

    XetOp* raw_op = nullptr;
    if (xet_file_upload_finalize_start(upload.get(), &raw_op, &err) != XetStatus_XetOk)
        throw_xet("xet_file_upload_finalize_start", err);
    Op finalize_op(raw_op);
    drive(finalize_op);

    XetFileMetadataHandle* raw_meta = nullptr;
    if (xet_op_take_file_metadata(finalize_op.get(), &raw_meta, &err) != XetStatus_XetOk)
        throw_xet("xet_op_take_file_metadata", err);
    FileMetadata meta(raw_meta);

    const std::string hash = xet_file_metadata_hash(meta.get());
    const std::uint64_t size = xet_file_metadata_file_size(meta.get());
    std::cout << "uploaded " << payload.size() << " bytes\n  hash: " << hash << "\n  size: " << size << "\n";

    XetOp* raw_commit_op = nullptr;
    if (xet_upload_commit_commit_start(commit.get(), &raw_commit_op, &err) != XetStatus_XetOk)
        throw_xet("xet_upload_commit_commit_start", err);
    Op commit_op(raw_commit_op);
    drive(commit_op);

    XetCommitReportHandle* raw_report = nullptr;
    if (xet_op_take_commit_report(commit_op.get(), &raw_report, &err) != XetStatus_XetOk)
        throw_xet("xet_op_take_commit_report", err);
    CommitReport report(raw_report);
    XetDedupMetrics metrics{};
    if (xet_commit_report_dedup(report.get(), &metrics) == XetStatus_XetOk)
        std::cout << "  committed: " << metrics.new_bytes << " new bytes, " << metrics.deduped_bytes
                  << " deduped bytes\n\n";

    // ---- Download ----
    XetAuthConfig read_cfg = make_cfg(read_url.c_str());
    XetFileDownloadGroup* raw_group = nullptr;
    if (xet_session_new_file_download_group(session.get(), &read_cfg, &raw_group, &err) != XetStatus_XetOk)
        throw_xet("xet_session_new_file_download_group", err);
    DownloadGroup group(raw_group);

    XetFileInfo* raw_fi = nullptr;
    if (xet_file_info_new(hash.c_str(), size, &raw_fi, &err) != XetStatus_XetOk)
        throw_xet("xet_file_info_new", err);
    FileInfo file_info(raw_fi);

    const std::string dest = "downloaded.bin";
    XetFileDownload* raw_dl = nullptr;
    if (xet_file_download_group_download_to_path(group.get(), file_info.get(), dest.c_str(), &raw_dl, &err) !=
        XetStatus_XetOk)
        throw_xet("xet_file_download_group_download_to_path", err);
    FileDownload download(raw_dl);

    XetOp* raw_dl_op = nullptr;
    if (xet_file_download_group_finish_start(group.get(), &raw_dl_op, &err) != XetStatus_XetOk)
        throw_xet("xet_file_download_group_finish_start", err);
    Op dl_op(raw_dl_op);
    drive(dl_op);

    XetDownloadGroupReportHandle* raw_dl_report = nullptr;
    if (xet_op_take_download_report(dl_op.get(), &raw_dl_report, &err) != XetStatus_XetOk)
        throw_xet("xet_op_take_download_report", err);
    DownloadReport dl_report(raw_dl_report);

    // ---- Verify ----
    std::ifstream f(dest, std::ios::binary);
    std::vector<std::uint8_t> got((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    if (got == payload) {
        std::cout << "downloaded " << got.size() << " bytes -> " << dest << "\nSUCCESS: round-trip content matches\n";
        return 0;
    }
    std::cerr << "MISMATCH: read " << got.size() << " of " << payload.size() << " bytes\n";
    return 1;
}

}  // namespace

int main(int argc, char** argv) {
    const std::string repo = argc > 1 ? argv[1] : kDefaultRepo;
    const char* token = std::getenv("HF_TOKEN");
    if (!token || !*token) {
        std::cerr << "HF_TOKEN environment variable is not set\n";
        return 1;
    }
    try {
        return run(repo, token);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }
}
