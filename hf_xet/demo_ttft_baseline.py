"""TTFT baseline: standard transformers from_pretrained (download to disk + load)."""

import gc
import os
import shutil
import time

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

MODEL_ID = "HuggingFaceTB/SmolLM2-1.7B"
PROMPT = "The meaning of life is"
ITERS = 5
SEP = "=" * 60
CACHE_DIR = "/tmp/hf_bench_cache"

TOKEN = open(os.path.expanduser("~/.cache/huggingface/token")).read().strip()
os.environ["HF_TOKEN"] = TOKEN


def cleanup():
    gc.collect()
    torch.cuda.empty_cache()
    torch.cuda.synchronize()


TOKENIZER = AutoTokenizer.from_pretrained(MODEL_ID)


def ttft_baseline():
    """Standard transformers: download to disk + load to GPU."""
    inputs = TOKENIZER(PROMPT, return_tensors="pt")

    # Clear cache to force re-download
    if os.path.exists(CACHE_DIR):
        shutil.rmtree(CACHE_DIR)

    cleanup()
    t0 = time.time()

    # This downloads to disk + loads weights to GPU
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_ID,
        torch_dtype=torch.bfloat16,
        device_map="cuda",
        cache_dir=CACHE_DIR,
    )
    model.eval()
    t_load = time.time() - t0

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

    del model
    cleanup()
    return t_total, t_load, first_token


if __name__ == "__main__":
    print(SEP)
    print("TTFT Baseline: transformers from_pretrained (disk)")
    print(f"Model: {MODEL_ID} | GPU: {torch.cuda.get_device_name(0)}")
    print(SEP)

    times, load_times = [], []

    for i in range(ITERS):
        t_total, t_load, token = ttft_baseline()
        times.append(t_total)
        load_times.append(t_load)
        print(
            f"  [{i+1:2d}/{ITERS}] total={t_total:.2f}s "
            f'(download+load={t_load:.2f}s) token="{token}"'
        )

    if times:
        import statistics

        med = statistics.median(times)
        load_med = statistics.median(load_times)
        print(f"\n{SEP}")
        print(f"{'Metric':25s} {'Value':>10s}")
        print(f"{'-' * 25} {'-' * 10}")
        print(f"{'Median total TTFT':25s} {med:>9.2f}s")
        print(f"{'Min total TTFT':25s} {min(times):>9.2f}s")
        print(f"{'Max total TTFT':25s} {max(times):>9.2f}s")
        print(f"{'Median download+load':25s} {load_med:>9.2f}s")
        print(SEP)
