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

// A spawned, poll-able operation. Always freed exactly once in its dtor,
// whether or not a take_* succeeded (matches the C ownership contract).
class Op {
public:
    explicit Op(XetOp* raw) : op_(raw) {}

    XetPollState poll() { return xet_op_poll(op_.get()); }

    // Poll until Ready or Error; throw xet::Exception on Error.
    void wait(std::chrono::milliseconds interval = std::chrono::milliseconds(20)) {
        XetPollState state;
        while ((state = xet_op_poll(op_.get())) == XetPollState_XetPollPending) {
            std::this_thread::sleep_for(interval);
        }
        if (state == XetPollState_XetPollError) {
            XetError* err = nullptr;
            xet_op_take_error(op_.get(), &err);
            check(XetStatus_XetErr, "operation failed", err);
        }
    }

    FileMetadata take_file_metadata() {
        XetFileMetadataHandle* raw = nullptr;
        XetError* err = nullptr;
        check(xet_op_take_file_metadata(op_.get(), &raw, &err), "xet_op_take_file_metadata", err);
        return FileMetadata(raw);
    }
    CommitReport take_commit_report() {
        XetCommitReportHandle* raw = nullptr;
        XetError* err = nullptr;
        check(xet_op_take_commit_report(op_.get(), &raw, &err), "xet_op_take_commit_report", err);
        return CommitReport(raw);
    }
    DownloadGroupReport take_download_report() {
        XetDownloadGroupReportHandle* raw = nullptr;
        XetError* err = nullptr;
        check(xet_op_take_download_report(op_.get(), &raw, &err), "xet_op_take_download_report", err);
        return DownloadGroupReport(raw);
    }
    void take_void() {
        XetError* err = nullptr;
        check(xet_op_take_void(op_.get(), &err), "xet_op_take_void", err);
    }
    // nullopt at stream EOF.
    std::optional<Bytes> take_bytes() {
        XetBytes* raw = nullptr;
        XetError* err = nullptr;
        check(xet_op_take_bytes(op_.get(), &raw, &err), "xet_op_take_bytes", err);
        if (!raw) return std::nullopt;
        return Bytes(raw);
    }
    // nullopt at stream EOF; otherwise {offset, chunk}.
    std::optional<std::pair<std::uint64_t, Bytes>> take_chunk() {
        std::uint64_t offset = 0;
        XetBytes* raw = nullptr;
        XetError* err = nullptr;
        check(xet_op_take_chunk(op_.get(), &offset, &raw, &err), "xet_op_take_chunk", err);
        if (!raw) return std::nullopt;
        return std::make_pair(offset, Bytes(raw));
    }

private:
    Handle<XetOp, xet_op_free> op_;
};

// Builder for XetAuthConfig. Owns its strings and header array; c_config()
// returns a XetAuthConfig borrowing this object's storage — it must not
// outlive the AuthConfig (the Session::new_* builders consume it immediately).
class AuthConfig {
public:
    AuthConfig& endpoint(std::string v) {
        endpoint_ = std::move(v);
        return *this;
    }
    AuthConfig& token(std::string v) {
        token_ = std::move(v);
        return *this;
    }
    AuthConfig& token_expiry(std::int64_t v) {
        expiry_ = v;
        return *this;
    }
    AuthConfig& token_refresh_url(std::string v) {
        refresh_url_ = std::move(v);
        return *this;
    }
    AuthConfig& add_refresh_header(std::string key, std::string value) {
        headers_.emplace_back(std::move(key), std::move(value));
        return *this;
    }

    XetAuthConfig c_config() const {
        header_view_.clear();
        header_view_.reserve(headers_.size());
        for (const auto& kv : headers_) {
            header_view_.push_back(XetHeader{kv.first.c_str(), kv.second.c_str()});
        }
        XetAuthConfig cfg{};
        cfg.endpoint = endpoint_ ? endpoint_->c_str() : nullptr;
        cfg.token = token_ ? token_->c_str() : nullptr;
        cfg.token_expiry = expiry_;
        cfg.token_refresh_url = refresh_url_ ? refresh_url_->c_str() : nullptr;
        cfg.refresh_headers = header_view_.empty() ? nullptr : header_view_.data();
        cfg.refresh_header_count = header_view_.size();
        return cfg;
    }

private:
    std::optional<std::string> endpoint_;
    std::optional<std::string> token_;
    std::optional<std::string> refresh_url_;
    std::int64_t expiry_ = 0;
    std::vector<std::pair<std::string, std::string>> headers_;
    mutable std::vector<XetHeader> header_view_;
};

// A queued file upload.
class FileUpload {
public:
    explicit FileUpload(XetFileUpload* raw) : u_(raw) {}
    Op finalize_start() {
        XetOp* raw = nullptr;
        XetError* err = nullptr;
        check(xet_file_upload_finalize_start(u_.get(), &raw, &err), "xet_file_upload_finalize_start", err);
        return Op(raw);
    }

private:
    Handle<XetFileUpload, xet_file_upload_free> u_;
};

// A streaming upload.
class StreamUpload {
public:
    explicit StreamUpload(XetStreamUpload* raw) : su_(raw) {}
    Op write_start(const std::uint8_t* data, std::size_t len) {
        XetOp* raw = nullptr;
        XetError* err = nullptr;
        check(xet_stream_upload_write_start(su_.get(), data, len, &raw, &err),
              "xet_stream_upload_write_start", err);
        return Op(raw);
    }
    Op finish_start() {
        XetOp* raw = nullptr;
        XetError* err = nullptr;
        check(xet_stream_upload_finish_start(su_.get(), &raw, &err), "xet_stream_upload_finish_start", err);
        return Op(raw);
    }

private:
    Handle<XetStreamUpload, xet_stream_upload_free> su_;
};

// An upload commit (Arc-backed in C; cheap to build from a session).
class UploadCommit {
public:
    explicit UploadCommit(XetUploadCommit* raw) : c_(raw) {}
    FileUpload upload_from_path(const char* path, XetSha256Policy policy,
                                const char* provided_sha256 = nullptr) {
        XetFileUpload* raw = nullptr;
        XetError* err = nullptr;
        check(xet_upload_commit_upload_from_path(c_.get(), path, policy, provided_sha256, &raw, &err),
              "xet_upload_commit_upload_from_path", err);
        return FileUpload(raw);
    }
    FileUpload upload_bytes(const std::uint8_t* data, std::size_t len, const char* name,
                            XetSha256Policy policy, const char* provided_sha256 = nullptr) {
        XetFileUpload* raw = nullptr;
        XetError* err = nullptr;
        check(xet_upload_commit_upload_bytes(c_.get(), data, len, name, policy, provided_sha256, &raw, &err),
              "xet_upload_commit_upload_bytes", err);
        return FileUpload(raw);
    }
    StreamUpload upload_stream(const char* name, XetSha256Policy policy,
                               const char* provided_sha256 = nullptr) {
        XetStreamUpload* raw = nullptr;
        XetError* err = nullptr;
        check(xet_upload_commit_upload_stream(c_.get(), name, policy, provided_sha256, &raw, &err),
              "xet_upload_commit_upload_stream", err);
        return StreamUpload(raw);
    }
    Op commit_start() {
        XetOp* raw = nullptr;
        XetError* err = nullptr;
        check(xet_upload_commit_commit_start(c_.get(), &raw, &err), "xet_upload_commit_commit_start", err);
        return Op(raw);
    }
    XetProgress progress() const {
        XetProgress p{};
        check(xet_upload_commit_progress(c_.get(), &p), "xet_upload_commit_progress");
        return p;
    }
    void abort() {
        XetError* err = nullptr;
        check(xet_upload_commit_abort(c_.get(), &err), "xet_upload_commit_abort", err);
    }

private:
    Handle<XetUploadCommit, xet_upload_commit_free> c_;
};

// A queued file download.
class FileDownload {
public:
    explicit FileDownload(XetFileDownload* raw) : d_(raw) {}
    std::uint64_t task_id() const { return xet_file_download_task_id(d_.get()); }

private:
    Handle<XetFileDownload, xet_file_download_free> d_;
};

// A file-download group.
class DownloadGroup {
public:
    explicit DownloadGroup(XetFileDownloadGroup* raw) : g_(raw) {}
    FileDownload download_to_path(const FileInfo& file_info, const char* dest_path) {
        XetFileDownload* raw = nullptr;
        XetError* err = nullptr;
        check(xet_file_download_group_download_to_path(g_.get(), file_info.get(), dest_path, &raw, &err),
              "xet_file_download_group_download_to_path", err);
        return FileDownload(raw);
    }
    Op finish_start() {
        XetOp* raw = nullptr;
        XetError* err = nullptr;
        check(xet_file_download_group_finish_start(g_.get(), &raw, &err),
              "xet_file_download_group_finish_start", err);
        return Op(raw);
    }
    XetProgress progress() const {
        XetProgress p{};
        check(xet_file_download_group_progress(g_.get(), &p), "xet_file_download_group_progress");
        return p;
    }
    void abort() {
        XetError* err = nullptr;
        check(xet_file_download_group_abort(g_.get(), &err), "xet_file_download_group_abort", err);
    }

private:
    Handle<XetFileDownloadGroup, xet_file_download_group_free> g_;
};

// An active download stream (ordered or unordered).
class DownloadStream {
public:
    explicit DownloadStream(XetDownloadStream* raw) : s_(raw) {}
    Op next_start() {
        XetOp* raw = nullptr;
        XetError* err = nullptr;
        check(xet_download_stream_next_start(s_.get(), &raw, &err), "xet_download_stream_next_start", err);
        return Op(raw);
    }
    XetProgress progress() const {
        XetProgress p{};
        check(xet_download_stream_progress(s_.get(), &p), "xet_download_stream_progress");
        return p;
    }
    void cancel() { xet_download_stream_cancel(s_.get()); }
    std::uint64_t task_id() const { return xet_download_stream_task_id(s_.get()); }

private:
    Handle<XetDownloadStream, xet_download_stream_free> s_;
};

// A stream-download group. `range` maps to has_range/range_start/range_end.
class DownloadStreamGroup {
public:
    explicit DownloadStreamGroup(XetDownloadStreamGroup* raw) : g_(raw) {}
    DownloadStream download_stream(const FileInfo& file_info,
                                   std::optional<std::pair<std::uint64_t, std::uint64_t>> range = std::nullopt) {
        XetDownloadStream* raw = nullptr;
        XetError* err = nullptr;
        check(xet_download_stream_group_download_stream(g_.get(), file_info.get(), range.has_value(),
                                                        range ? range->first : 0, range ? range->second : 0,
                                                        &raw, &err),
              "xet_download_stream_group_download_stream", err);
        return DownloadStream(raw);
    }
    DownloadStream download_unordered_stream(
        const FileInfo& file_info,
        std::optional<std::pair<std::uint64_t, std::uint64_t>> range = std::nullopt) {
        XetDownloadStream* raw = nullptr;
        XetError* err = nullptr;
        check(xet_download_stream_group_download_unordered_stream(
                  g_.get(), file_info.get(), range.has_value(), range ? range->first : 0,
                  range ? range->second : 0, &raw, &err),
              "xet_download_stream_group_download_unordered_stream", err);
        return DownloadStream(raw);
    }

private:
    Handle<XetDownloadStreamGroup, xet_download_stream_group_free> g_;
};

}  // namespace xet
