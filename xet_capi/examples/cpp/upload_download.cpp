// Upload + download round-trip against a real Hugging Face Xet repo, using the
// hf_xet C API through the xet_session.hpp C++ wrapper (RAII + exceptions).
//
// Auth: reads $HF_TOKEN from the environment.
// Build/run: see the Makefile in this directory (`make run`).

#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <random>
#include <string>
#include <vector>

#include "xet_session.hpp"

namespace {

constexpr const char* kDefaultRepo = "assafvayner/xet-c-api-test";
constexpr const char* kRepoType = "datasets";
constexpr const char* kRevision = "main";
constexpr const char* kHub = "https://huggingface.co";

int run(const std::string& repo, const std::string& hf_token) {
    std::cout << "hf_xet C++ example (version " << xet::version() << ")\n"
              << "repo: " << repo << " (" << kRepoType << ")\n\n";

    const std::string base = std::string(kHub) + "/api/" + kRepoType + "/" + repo;
    const std::string write_url = base + "/xet-write-token/" + kRevision;
    const std::string read_url = base + "/xet-read-token/" + kRevision;
    const std::string bearer = "Bearer " + hf_token;

    xet::Session session;

    // ---- Upload ----
    xet::AuthConfig write_cfg;
    write_cfg.token_refresh_url(write_url).add_refresh_header("Authorization", bearer);
    auto commit = session.new_upload_commit(write_cfg);

    // Random payload so each run uploads distinct bytes.
    std::vector<std::uint8_t> payload(128 * 1024);
    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> byte(0, 255);
    for (auto& b : payload) b = static_cast<std::uint8_t>(byte(rng));

    auto upload = commit.upload_bytes(payload.data(), payload.size(), "random.bin",
                                      XetSha256Policy_XetSha256Compute);
    auto finalize_op = upload.finalize_start();
    finalize_op.wait();
    auto meta = finalize_op.take_file_metadata();

    const std::string hash = meta.hash();
    const std::uint64_t size = meta.file_size();
    std::cout << "uploaded " << payload.size() << " bytes\n  hash: " << hash << "\n  size: " << size << "\n";

    auto commit_op = commit.commit_start();
    commit_op.wait();
    auto report = commit_op.take_commit_report();
    const XetDedupMetrics metrics = report.dedup();
    std::cout << "  committed: " << metrics.new_bytes << " new bytes, " << metrics.deduped_bytes
              << " deduped bytes\n\n";

    // ---- Download ----
    xet::AuthConfig read_cfg;
    read_cfg.token_refresh_url(read_url).add_refresh_header("Authorization", bearer);
    auto group = session.new_file_download_group(read_cfg);

    xet::FileInfo file_info(hash.c_str(), size);
    const std::string dest = "downloaded.bin";
    group.download_to_path(file_info, dest.c_str());

    auto dl_op = group.finish_start();
    dl_op.wait();
    auto dl_report = dl_op.take_download_report();
    (void)dl_report;

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
    } catch (const xet::Exception& e) {
        std::cerr << "error: [" << e.code() << "] " << e.what() << "\n";
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }
}
