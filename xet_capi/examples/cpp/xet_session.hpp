// C++ RAII wrapper over the hf_xet C API. Header-only; example-local (not a
// shipped public API). Every class is move-only and frees its handle exactly
// once in its destructor; fallible C calls throw xet::Exception on failure.
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

extern "C" {
#include "hf_xet.h"
}

namespace xet {

// Exception carrying the originating XetStatus code.
class Exception : public std::runtime_error {
public:
    Exception(XetStatus code, const std::string& what) : std::runtime_error(what), code_(code) {}
    XetStatus code() const noexcept { return code_; }

private:
    XetStatus code_;
};

// Move-only owning handle: unique_ptr with the matching C free fn.
template <typename T, void (*Free)(T*)>
struct Deleter {
    void operator()(T* p) const noexcept {
        if (p) Free(p);
    }
};
template <typename T, void (*Free)(T*)>
using Handle = std::unique_ptr<T, Deleter<T, Free>>;

// Throw on failure for C calls that fill an XetError** out-param. Frees err.
inline void check(XetStatus st, const char* ctx, XetError* err) {
    if (st == XetStatus_XetOk) return;
    Handle<XetError, xet_error_free> owned(err);  // frees err on every exit path
    std::string msg = ctx;
    if (owned) {
        const char* m = xet_error_message(owned.get());
        msg += ": [" + std::to_string(xet_error_code(owned.get())) + "] " + (m ? m : "");
    }
    throw Exception(st, msg);
}

// Throw on failure for status-only C calls (no XetError** out-param).
inline void check(XetStatus st, const char* ctx) {
    if (st == XetStatus_XetOk) return;
    throw Exception(st, std::string(ctx) + ": status " + std::to_string(st));
}

namespace detail {
// Nullable C string -> optional<string>.
inline std::optional<std::string> opt_str(const char* s) {
    if (!s) return std::nullopt;
    return std::string(s);
}
}  // namespace detail

inline std::string version() {
    return xet_version();
}

inline void init_logging(const char* version) {
    xet_init_logging(version);
}

// Owned byte buffer from streaming downloads.
class Bytes {
public:
    explicit Bytes(XetBytes* raw) : h_(raw) {}
    const uint8_t* data() const { return xet_bytes_data(h_.get()); }
    std::size_t size() const { return xet_bytes_len(h_.get()); }

private:
    Handle<XetBytes, xet_bytes_free> h_;
};

// Download file descriptor: content hash (hex) + size, optionally a sha256.
class FileInfo {
public:
    FileInfo(const char* hash, std::uint64_t size) {
        XetFileInfo* raw = nullptr;
        XetError* err = nullptr;
        check(xet_file_info_new(hash, size, &raw, &err), "xet_file_info_new", err);
        fi_.reset(raw);
    }
    FileInfo(const char* hash, std::uint64_t size, const char* sha256) {
        XetFileInfo* raw = nullptr;
        XetError* err = nullptr;
        check(xet_file_info_new_with_sha256(hash, size, sha256, &raw, &err),
              "xet_file_info_new_with_sha256", err);
        fi_.reset(raw);
    }
    const XetFileInfo* get() const { return fi_.get(); }

private:
    Handle<XetFileInfo, xet_file_info_free> fi_;
};

// Borrowed metadata view (e.g. from CommitReport::file_at). Non-owning: the
// underlying handle is owned by the report and must NOT be freed here.
class FileMetadataView {
public:
    explicit FileMetadataView(const XetFileMetadataHandle* h) : h_(h) {}
    std::string hash() const { return xet_file_metadata_hash(h_); }
    std::uint64_t file_size() const { return xet_file_metadata_file_size(h_); }
    std::optional<std::string> sha256() const { return detail::opt_str(xet_file_metadata_sha256(h_)); }
    std::optional<std::string> tracking_name() const {
        return detail::opt_str(xet_file_metadata_tracking_name(h_));
    }

private:
    const XetFileMetadataHandle* h_;
};

// Owned metadata (from Op::take_file_metadata). Frees the handle in dtor.
class FileMetadata {
public:
    explicit FileMetadata(XetFileMetadataHandle* raw) : h_(raw) {}
    FileMetadataView view() const { return FileMetadataView(h_.get()); }
    std::string hash() const { return view().hash(); }
    std::uint64_t file_size() const { return view().file_size(); }
    std::optional<std::string> sha256() const { return view().sha256(); }
    std::optional<std::string> tracking_name() const { return view().tracking_name(); }

private:
    Handle<XetFileMetadataHandle, xet_file_metadata_free> h_;
};

// Owned commit report.
class CommitReport {
public:
    explicit CommitReport(XetCommitReportHandle* raw) : r_(raw) {}
    std::size_t file_count() const { return xet_commit_report_file_count(r_.get()); }
    // Borrowed view; valid until this report is freed.
    FileMetadataView file_at(std::size_t index) const {
        const XetFileMetadataHandle* out = nullptr;
        check(xet_commit_report_file_at(r_.get(), index, &out), "xet_commit_report_file_at");
        return FileMetadataView(out);
    }
    XetDedupMetrics dedup() const {
        XetDedupMetrics m{};
        check(xet_commit_report_dedup(r_.get(), &m), "xet_commit_report_dedup");
        return m;
    }
    XetProgress progress() const {
        XetProgress p{};
        check(xet_commit_report_progress(r_.get(), &p), "xet_commit_report_progress");
        return p;
    }

private:
    Handle<XetCommitReportHandle, xet_commit_report_free> r_;
};

// Owned download-group report.
class DownloadGroupReport {
public:
    struct Entry {
        std::uint64_t task_id;
        std::uint64_t bytes_completed;
    };
    explicit DownloadGroupReport(XetDownloadGroupReportHandle* raw) : r_(raw) {}
    std::size_t count() const { return xet_download_group_report_count(r_.get()); }
    Entry at(std::size_t index) const {
        Entry e{};
        check(xet_download_group_report_at(r_.get(), index, &e.task_id, &e.bytes_completed),
              "xet_download_group_report_at");
        return e;
    }

private:
    Handle<XetDownloadGroupReportHandle, xet_download_group_report_free> r_;
};

}  // namespace xet
