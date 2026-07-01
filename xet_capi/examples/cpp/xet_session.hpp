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

}  // namespace xet
