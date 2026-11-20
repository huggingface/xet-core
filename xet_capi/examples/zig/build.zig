const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.createModule(.{
        .root_source_file = b.path("main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // -I ../../include        : the committed C header (hf_xet.h)
    // -L/-l xet_capi + rpath  : link the self-contained shared library
    mod.addIncludePath(b.path("../../include"));
    mod.addLibraryPath(b.path("../../../target/release"));
    mod.linkSystemLibrary("xet_capi", .{});
    mod.addRPath(b.path("../../../target/release"));
    mod.link_libc = true;

    const exe = b.addExecutable(.{
        .name = "upload_download",
        .root_module = mod,
    });

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);

    const run_step = b.step("run", "Build and run the example (requires $HF_TOKEN)");
    run_step.dependOn(&run_cmd.step);
}
