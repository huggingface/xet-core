// Upload + download round-trip against a real Hugging Face Xet repo, using the
// hf_xet C API from Zig via @cImport.
//
// Auth: reads $HF_TOKEN from the environment.
// Build/run: see the README in this directory (`zig build run`).

const std = @import("std");

const c = @cImport({
    @cInclude("hf_xet.h");
});

const default_repo = "assafvayner/xet-c-api-test";
const repo_type = "datasets";
const revision = "main";
const hub = "https://huggingface.co";

const XetFail = error{XetFailed};

// Print `ctx` plus the error's code/message, free the error, and signal failure.
fn fail(ctx: []const u8, err: ?*c.XetError) XetFail {
    if (err) |e| {
        const msg = std.mem.span(c.xet_error_message(e));
        const code = c.xet_error_code(e);
        std.debug.print("error: {s}: [{d}] {s}\n", .{ ctx, code, msg });
        c.xet_error_free(e);
    } else {
        std.debug.print("error: {s}: (no error detail)\n", .{ctx});
    }
    return XetFail.XetFailed;
}

fn check(status: c.XetStatus, ctx: []const u8, err: ?*c.XetError) XetFail!void {
    if (status != c.XetStatus_XetOk) return fail(ctx, err);
}

fn run(io: std.Io, allocator: std.mem.Allocator, repo: []const u8, token: []const u8) !void {
    std.debug.print("hf_xet Zig example (version {s})\n", .{std.mem.span(c.xet_version())});
    std.debug.print("repo: {s} ({s})\n\n", .{ repo, repo_type });

    const write_url = try std.fmt.allocPrintSentinel(allocator, "{s}/api/{s}/{s}/xet-write-token/{s}", .{ hub, repo_type, repo, revision }, 0);
    defer allocator.free(write_url);
    const read_url = try std.fmt.allocPrintSentinel(allocator, "{s}/api/{s}/{s}/xet-read-token/{s}", .{ hub, repo_type, repo, revision }, 0);
    defer allocator.free(read_url);
    const bearer = try std.fmt.allocPrintSentinel(allocator, "Bearer {s}", .{token}, 0);
    defer allocator.free(bearer);

    var header = c.XetHeader{ .key = "Authorization", .value = bearer.ptr };

    // endpoint/token stay NULL; the refresh response provides them.
    const mkCfg = struct {
        fn cfg(refresh_url: [*c]const u8, hdr: *c.XetHeader) c.XetAuthConfig {
            return c.XetAuthConfig{
                .endpoint = null,
                .token = null,
                .token_expiry = 0,
                .token_refresh_url = refresh_url,
                .refresh_headers = hdr,
                .refresh_header_count = 1,
            };
        }
    }.cfg;

    var err: ?*c.XetError = null;

    var session: ?*c.XetSession = null;
    try check(c.xet_session_new(&session, &err), "xet_session_new", err);
    defer c.xet_session_free(session);

    // ---- Upload ----
    var write_cfg = mkCfg(write_url.ptr, &header);
    var commit: ?*c.XetUploadCommit = null;
    try check(c.xet_session_new_upload_commit(session, &write_cfg, &commit, &err), "xet_session_new_upload_commit", err);
    defer c.xet_upload_commit_free(commit);

    // Random payload so each run uploads distinct bytes.
    var payload: [128 * 1024]u8 = undefined;
    io.random(&payload);

    var upload: ?*c.XetFileUpload = null;
    try check(c.xet_upload_commit_upload_bytes(commit, &payload, payload.len, "random.bin", c.XetSha256Policy_XetSha256Compute, null, &upload, &err), "xet_upload_commit_upload_bytes", err);
    defer c.xet_file_upload_free(upload);

    var meta: ?*c.XetFileMetadataHandle = null;
    try check(c.xet_file_upload_finalize(upload, &meta, &err), "xet_file_upload_finalize", err);
    defer c.xet_file_metadata_free(meta);

    const hash = std.mem.span(c.xet_file_metadata_hash(meta));
    const size = c.xet_file_metadata_file_size(meta);
    std.debug.print("uploaded {d} bytes\n  hash: {s}\n  size: {d}\n", .{ payload.len, hash, size });

    var report: ?*c.XetCommitReportHandle = null;
    try check(c.xet_upload_commit_commit(commit, &report, &err), "xet_upload_commit_commit", err);
    defer c.xet_commit_report_free(report);
    var metrics: c.XetDedupMetrics = undefined;
    if (c.xet_commit_report_dedup(report, &metrics) == c.XetStatus_XetOk) {
        std.debug.print("  committed: {d} new bytes, {d} deduped bytes\n\n", .{ metrics.new_bytes, metrics.deduped_bytes });
    }

    // ---- Download ----
    var read_cfg = mkCfg(read_url.ptr, &header);
    var group: ?*c.XetFileDownloadGroup = null;
    try check(c.xet_session_new_file_download_group(session, &read_cfg, &group, &err), "xet_session_new_file_download_group", err);
    defer c.xet_file_download_group_free(group);

    const c_hash = try allocator.dupeZ(u8, hash);
    defer allocator.free(c_hash);
    var file_info: ?*c.XetFileInfo = null;
    try check(c.xet_file_info_new(c_hash.ptr, size, &file_info, &err), "xet_file_info_new", err);
    defer c.xet_file_info_free(file_info);

    const dest = "downloaded.bin";
    var download: ?*c.XetFileDownload = null;
    try check(c.xet_file_download_group_download_to_path(group, file_info, dest, &download, &err), "xet_file_download_group_download_to_path", err);
    defer c.xet_file_download_free(download);

    var dl_report: ?*c.XetDownloadGroupReportHandle = null;
    try check(c.xet_file_download_group_finish(group, &dl_report, &err), "xet_file_download_group_finish", err);
    defer c.xet_download_group_report_free(dl_report);

    // ---- Verify ----
    const got = try std.Io.Dir.cwd().readFileAlloc(io, dest, allocator, .limited(1 << 30));
    defer allocator.free(got);
    if (!std.mem.eql(u8, got, &payload)) {
        std.debug.print("error: MISMATCH: read {d} of {d} bytes\n", .{ got.len, payload.len });
        return XetFail.XetFailed;
    }
    std.debug.print("downloaded {d} bytes -> {s}\nSUCCESS: round-trip content matches\n", .{ got.len, dest });
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var arg_it = init.minimal.args.iterate();
    _ = arg_it.skip(); // program name
    const repo = arg_it.next() orelse default_repo;

    const token = init.environ_map.get("HF_TOKEN") orelse "";
    if (token.len == 0) {
        std.debug.print("HF_TOKEN environment variable is not set\n", .{});
        std.process.exit(1);
    }

    run(init.io, allocator, repo, token) catch std.process.exit(1);
}
