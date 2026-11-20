// Upload + download round-trip against a real Hugging Face Xet repo, using the
// hf_xet C API from Swift (imported as the `CHfXet` module).
//
// Auth: reads $HF_TOKEN from the environment.
// Build/run: see the build.sh script in this directory.

import CHfXet
import Foundation

let defaultRepo = "assafvayner/xet-c-api-test"
let repoType = "datasets"
let revision = "main"
let hub = "https://huggingface.co"

struct XetFail: Error { let message: String }

// Throw a XetFail carrying the error's message, freeing the error.
func fail(_ ctx: String, _ err: OpaquePointer?) -> XetFail {
    guard let e = err else { return XetFail(message: "\(ctx): (no error detail)") }
    let msg = "\(ctx): [\(xet_error_code(e).rawValue)] \(String(cString: xet_error_message(e)))"
    xet_error_free(e)
    return XetFail(message: msg)
}

func check(_ status: XetStatus, _ ctx: String, _ err: OpaquePointer?) throws {
    if status != XetStatus_XetOk { throw fail(ctx, err) }
}

func run(repo: String, token: String) throws {
    print("hf_xet Swift example (version \(String(cString: xet_version())))")
    print("repo: \(repo) (\(repoType))\n")

    // strdup the C strings we need to keep alive across calls; freed at exit.
    let writeURL = strdup("\(hub)/api/\(repoType)/\(repo)/xet-write-token/\(revision)")
    let readURL = strdup("\(hub)/api/\(repoType)/\(repo)/xet-read-token/\(revision)")
    let authKey = strdup("Authorization")
    let bearer = strdup("Bearer \(token)")
    defer { free(writeURL); free(readURL); free(authKey); free(bearer) }

    var header = XetHeader(key: authKey, value: bearer)

    // Build an auth config borrowing `header`, then run `body` with a pointer to it.
    func withAuthConfig<R>(_ refreshURL: UnsafeMutablePointer<CChar>?,
                           _ body: (UnsafePointer<XetAuthConfig>) throws -> R) rethrows -> R {
        return try withUnsafePointer(to: &header) { hp in
            var cfg = XetAuthConfig(endpoint: nil, token: nil, token_expiry: 0,
                                    token_refresh_url: refreshURL, refresh_headers: hp, refresh_header_count: 1)
            return try withUnsafePointer(to: &cfg) { try body($0) }
        }
    }

    var err: OpaquePointer?

    var session: OpaquePointer?
    try check(xet_session_new(&session, &err), "xet_session_new", err)
    defer { xet_session_free(session) }

    // ---- Upload ----
    var commit: OpaquePointer?
    try withAuthConfig(writeURL) { cfg in
        try check(xet_session_new_upload_commit(session, cfg, &commit, &err), "xet_session_new_upload_commit", err)
    }
    defer { xet_upload_commit_free(commit) }

    // Random payload so each run uploads distinct bytes.
    var payload = [UInt8](repeating: 0, count: 128 * 1024)
    for i in payload.indices { payload[i] = UInt8.random(in: 0...255) }

    var upload: OpaquePointer?
    try payload.withUnsafeBufferPointer { buf in
        try check(xet_upload_commit_upload_bytes(commit, buf.baseAddress, UInt(buf.count), "random.bin",
                                                 XetSha256Policy_XetSha256Compute, nil, &upload, &err),
                  "xet_upload_commit_upload_bytes", err)
    }
    defer { xet_file_upload_free(upload) }

    var meta: OpaquePointer?
    try check(xet_file_upload_finalize(upload, &meta, &err), "xet_file_upload_finalize", err)
    defer { xet_file_metadata_free(meta) }

    let hash = String(cString: xet_file_metadata_hash(meta))
    let size = xet_file_metadata_file_size(meta)
    print("uploaded \(payload.count) bytes\n  hash: \(hash)\n  size: \(size)")

    var report: OpaquePointer?
    try check(xet_upload_commit_commit(commit, &report, &err), "xet_upload_commit_commit", err)
    defer { xet_commit_report_free(report) }
    var metrics = XetDedupMetrics()
    if xet_commit_report_dedup(report, &metrics) == XetStatus_XetOk {
        print("  committed: \(metrics.new_bytes) new bytes, \(metrics.deduped_bytes) deduped bytes\n")
    }

    // ---- Download ----
    var group: OpaquePointer?
    try withAuthConfig(readURL) { cfg in
        try check(xet_session_new_file_download_group(session, cfg, &group, &err),
                  "xet_session_new_file_download_group", err)
    }
    defer { xet_file_download_group_free(group) }

    var fileInfo: OpaquePointer?
    try check(xet_file_info_new(hash, size, &fileInfo, &err), "xet_file_info_new", err)
    defer { xet_file_info_free(fileInfo) }

    let dest = "downloaded.bin"
    var download: OpaquePointer?
    try check(xet_file_download_group_download_to_path(group, fileInfo, dest, &download, &err),
              "xet_file_download_group_download_to_path", err)
    defer { xet_file_download_free(download) }

    var dlReport: OpaquePointer?
    try check(xet_file_download_group_finish(group, &dlReport, &err), "xet_file_download_group_finish", err)
    defer { xet_download_group_report_free(dlReport) }

    // ---- Verify ----
    let got = try Data(contentsOf: URL(fileURLWithPath: dest))
    guard got.elementsEqual(payload) else {
        throw XetFail(message: "MISMATCH: read \(got.count) of \(payload.count) bytes")
    }
    print("downloaded \(got.count) bytes -> \(dest)\nSUCCESS: round-trip content matches")
}

let repo = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : defaultRepo
guard let token = ProcessInfo.processInfo.environment["HF_TOKEN"], !token.isEmpty else {
    FileHandle.standardError.write(Data("HF_TOKEN environment variable is not set\n".utf8))
    exit(1)
}
do {
    try run(repo: repo, token: token)
} catch let e as XetFail {
    FileHandle.standardError.write(Data("error: \(e.message)\n".utf8))
    exit(1)
} catch {
    FileHandle.standardError.write(Data("error: \(error)\n".utf8))
    exit(1)
}
