"""TTFT benchmark: hf_xet.download_to_buffer -> CUDA pinned memory -> transformers inference.

Demonstrates zero-copy download: CAS data lands directly in pinned host memory,
then DMA-transfers to GPU without any intermediate file or copy.

Usage:
    pip install torch transformers requests
    cd hf_xet && maturin develop --release
    python demo_ttft_cuda.py
"""

import gc
import json
import os
import struct
import time

import requests
import torch
from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer

import hf_xet

MODEL_ID = "HuggingFaceTB/SmolLM2-1.7B"
SAFETENSORS_FILE = "model.safetensors"
PROMPT = "The meaning of life is"
ITERS = 5
SEP = "=" * 60

DTYPE_MAP = {
    "F32": (torch.float32, 4),
    "F16": (torch.float16, 2),
    "BF16": (torch.bfloat16, 2),
    "I64": (torch.int64, 8),
    "I32": (torch.int32, 4),
    "I16": (torch.int16, 2),
    "I8": (torch.int8, 1),
    "U8": (torch.uint8, 1),
}

HUB_ENDPOINT = "https://huggingface.co"
TOKEN = open(os.path.expanduser("~/.cache/huggingface/token")).read().strip()


def get_file_info(repo, file, revision="main"):
    """Get xet_hash and size from Hub /tree API."""
    url = f"{HUB_ENDPOINT}/api/models/{repo}/tree/{revision}"
    resp = requests.get(url, headers={"Authorization": f"Bearer {TOKEN}"})
    resp.raise_for_status()
    for entry in resp.json():
        if entry["path"] == file:
            xet_hash = entry.get("xetHash")
            size = entry.get("size")
            if not xet_hash:
                raise ValueError(f"File '{file}' has no xetHash")
            return xet_hash, size
    raise ValueError(f"File '{file}' not found in repo '{repo}'")


def get_cas_token(repo, revision="main"):
    """Get CAS JWT (endpoint + token) from Hub API."""
    url = f"{HUB_ENDPOINT}/api/models/{repo}/xet-read-token/{revision}"
    resp = requests.get(url, headers={"Authorization": f"Bearer {TOKEN}"})
    resp.raise_for_status()
    data = resp.json()
    return data["casUrl"], data["accessToken"], data["exp"]


def cleanup():
    gc.collect()
    torch.cuda.empty_cache()
    torch.cuda.synchronize()


# Preload metadata once
print("Fetching file info from Hub API...")
XET_HASH, FILE_SIZE = get_file_info(MODEL_ID, SAFETENSORS_FILE)
print(f"  xet_hash: {XET_HASH}")
print(f"  file_size: {FILE_SIZE} ({FILE_SIZE / 1e9:.2f} GB)")

CAS_URL, CAS_TOKEN, CAS_EXP = get_cas_token(MODEL_ID)
print(f"  cas_url: {CAS_URL}")

TOKENIZER = AutoTokenizer.from_pretrained(MODEL_ID)


def ttft_xet_cuda_mapped():
    """Download via hf_xet CAS directly into CUDA pinned memory, then inference."""
    inputs = TOKENIZER(PROMPT, return_tensors="pt")

    cleanup()
    t0 = time.time()

    # Allocate CUDA pinned (host-mapped) memory
    buf = torch.empty(FILE_SIZE, dtype=torch.uint8, pin_memory=True)

    # Download via hf_xet directly into pinned buffer
    hf_xet.download_to_buffer(
        hash=XET_HASH,
        file_size=FILE_SIZE,
        buf_ptr=buf.data_ptr(),
        buf_len=FILE_SIZE,
        endpoint=CAS_URL,
        token_info=(CAS_TOKEN, CAS_EXP),
        token_refresher=None,
    )
    t_download = time.time() - t0

    # Parse safetensors header from the buffer
    header_bytes = buf[:8].numpy().tobytes()
    header_len = struct.unpack("<Q", header_bytes)[0]
    metadata_bytes = buf[8 : 8 + header_len].numpy().tobytes()
    metadata = json.loads(metadata_bytes)
    data_offset = 8 + header_len

    # Load tensors directly to GPU from pinned memory (DMA transfer)
    state_dict = {}
    for name, info in metadata.items():
        if name == "__metadata__":
            continue
        start, end = info["data_offsets"]
        torch_dtype, elem_size = DTYPE_MAP[info["dtype"]]
        count = (end - start) // elem_size
        cpu_tensor = torch.frombuffer(
            buf.numpy(),
            dtype=torch_dtype,
            offset=data_offset + start,
            count=count,
        ).reshape(info["shape"])
        state_dict[name] = cpu_tensor.cuda(non_blocking=True)
    torch.cuda.synchronize()
    t_transfer = time.time() - t0

    # Build model from config and load state dict
    config = AutoConfig.from_pretrained(MODEL_ID)
    with torch.device("cuda"):
        model = AutoModelForCausalLM.from_config(config, dtype=torch.bfloat16)
    model.load_state_dict(state_dict, assign=True, strict=False)
    model.tie_weights()
    model.eval()
    del state_dict

    # Generate first token
    with torch.no_grad():
        out = model.generate(
            inputs["input_ids"].cuda(),
            attention_mask=inputs["attention_mask"].cuda(),
            max_new_tokens=1,
            pad_token_id=TOKENIZER.eos_token_id,
        )
    torch.cuda.synchronize()
    t_total = time.time() - t0

    first_token = TOKENIZER.decode(out[0, -1])

    del model, buf
    cleanup()
    return t_total, t_download, t_transfer, first_token


if __name__ == "__main__":
    print(SEP)
    print("TTFT Benchmark: hf_xet.download_to_buffer -> CUDA pinned memory")
    print(f"Model: {MODEL_ID} | GPU: {torch.cuda.get_device_name(0)}")
    print(SEP)

    times, dl_times, xfer_times = [], [], []

    for i in range(ITERS):
        t_total, t_dl, t_xfer, token = ttft_xet_cuda_mapped()
        times.append(t_total)
        dl_times.append(t_dl)
        xfer_times.append(t_xfer)
        print(
            f"  [{i+1:2d}/{ITERS}] total={t_total:.2f}s "
            f"(download={t_dl:.2f}s, dl+transfer={t_xfer:.2f}s) "
            f'token="{token}"'
        )

    if times:
        import statistics

        med = statistics.median(times)
        dl_med = statistics.median(dl_times)
        xfer_med = statistics.median(xfer_times)
        print(f"\n{SEP}")
        print(f"{'Metric':25s} {'Value':>10s}")
        print(f"{'-' * 25} {'-' * 10}")
        print(f"{'Median total TTFT':25s} {med:>9.2f}s")
        print(f"{'Min total TTFT':25s} {min(times):>9.2f}s")
        print(f"{'Max total TTFT':25s} {max(times):>9.2f}s")
        print(f"{'Median download':25s} {dl_med:>9.2f}s")
        print(f"{'Median dl+GPU transfer':25s} {xfer_med:>9.2f}s")
        print(f"{'Download speed':25s} {FILE_SIZE / dl_med / 1e9:>8.2f} GB/s")
        print(SEP)
