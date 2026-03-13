# hf-xet Diagnostic Scripts

Scripts for collecting diagnostics when `hf-xet` hangs, crashes, or behaves
unexpectedly. They download debug symbols, configure logging, and periodically
capture stack traces / core dumps into a self-contained directory that is easy
to zip and attach to a [GitHub issue](https://github.com/huggingface/xet-core/issues/new/choose).

## Quick start

Pick the script for your OS and prefix your failing command with it:

| OS | Script |
|----|--------|
| Linux | `scripts/diag/hf-xet-diag-linux.sh` |
| macOS | `scripts/diag/hf-xet-diag-macos.sh` |
| Windows (Git-Bash) | `scripts/diag/hf-xet-diag-windows.sh` |

```bash
# Linux
./scripts/diag/hf-xet-diag-linux.sh -- python my-script.py

# macOS
./scripts/diag/hf-xet-diag-macos.sh -- python my-script.py

# Windows (Git-Bash)
./scripts/diag/hf-xet-diag-windows.sh -- python my-script.py
```

## Per-platform details

### Linux (`hf-xet-diag-linux.sh`)

* Uses `gdb` + `gcore` to periodically snapshot stacks and produce core dumps.
* Supports optional ptrace preload helper for debugging.
* Downloads and installs the appropriate `hf_xet-*.dbg` symbol file automatically.

**Requirements:**

```bash
sudo apt-get install gdb build-essential
```

**Example:**

```bash
./scripts/diag/hf-xet-diag-linux.sh -- python hf-download.py "Qwen/Qwen2.5-VL-3B-Instruct"
```

### macOS (`hf-xet-diag-macos.sh`)

* Uses `sample` + `lldb` to periodically snapshot stacks and produce core dumps.
* Downloads and installs the appropriate `hf_xet-*.dbg` symbol file automatically.

**Requirements:**

```bash
sudo xcode-select --install
```

**Example:**

```bash
./scripts/diag/hf-xet-diag-macos.sh -- python hf-download.py "Qwen/Qwen2.5-VL-3B-Instruct"
```

### Windows / Git-Bash (`hf-xet-diag-windows.sh`)

* Runs in **Git-Bash**, keeping usage consistent with Linux/macOS.
* Uses **Sysinternals ProcDump** for periodic mini dumps (`-mp`).
* Auto-downloads `procdump.exe` if not found.
* Downloads and installs the matching `hf_xet.pdb` debug symbol into the package directory.

**Requirements:**

* Git-Bash (from [Git for Windows](https://gitforwindows.org/))
* Python installed
* Internet access (first run downloads ProcDump and debug symbols)

**Example:**

```bash
./scripts/diag/hf-xet-diag-windows.sh -- python hf-download.py "Qwen/Qwen2.5-VL-3B-Instruct"
```

## Output layout

Each run produces a directory named `diag_<command>_<timestamp>/`:

```
diag_<command>_<timestamp>/
  ├── console.log   # Combined stdout/stderr of the process
  ├── env.log       # System/environment info
  ├── pid           # Child PID file
  ├── stacks/       # Periodic stack traces / mini dumps
  └── dumps/        # (Linux/macOS) full core dumps
```

> **Tip:** Zip and attach the entire `diag_<command>_<timestamp>/` directory
> when filing an issue — it contains everything needed to reproduce and diagnose
> the problem.

## Analyzing dumps

Use `hf-xet-diag-analyze-latest.sh` to automatically open the most recent dump
in the appropriate debugger:

```bash
# Auto-detect latest diag_* directory
./scripts/diag/hf-xet-diag-analyze-latest.sh

# Or specify a directory explicitly
./scripts/diag/hf-xet-diag-analyze-latest.sh diag_python_hfxet_test_20250127120000
```

The script:
* Auto-detects your OS (Linux, macOS, or Windows)
* Finds the most recent `diag_*` directory
* Opens the latest dump in the platform-appropriate debugger:
  * **Linux:** `gdb` with core dumps from `dumps/`
  * **macOS:** `lldb` with `.core` files from `dumps/`
  * **Windows (Git-Bash):** `windbg` with `.dmp` files from `stacks/`

### Manual analysis

**Linux**

```bash
gdb python dumps/core_<timestamp>.<pid>
(gdb) bt                    # backtrace of current thread
(gdb) thread apply all bt   # backtrace of all threads
(gdb) info threads          # list all threads
```

Debug symbols: `hf_xet-*.so.dbg` must be in the `hf_xet` package directory.

**macOS**

```bash
lldb -c dumps/dump_<pid>_<timestamp>.core python3
(lldb) bt                    # backtrace of current thread
(lldb) thread backtrace all  # backtrace of all threads
(lldb) thread list           # list all threads
```

Debug symbols: `hf_xet-*.dylib.dSYM` must be in the `hf_xet` package directory.

**Windows**

```cmd
windbg -z stacks\dump_<timestamp>.dmp
```

Useful WinDbg commands:

```
!analyze -v     # automatic analysis
~* kb           # backtrace of all threads
~               # list all threads
lm              # list loaded modules (verify hf_xet.pdb loaded)
```

Debug symbols: `hf_xet.pdb` must be in the `hf_xet` package directory.

## Installing debug symbols manually

The diagnostic scripts install symbols automatically, but you can also do it
manually:

1. Download and unzip the [debug symbols package](https://github.com/huggingface/xet-core/releases/download/latest/dbg-symbols.zip).
2. Find the `hf_xet` package location: `pip show hf-xet` — look at the `Location` field.
3. Choose the right symbol file for your platform:
   * **Windows:** `hf_xet.pdb`
   * **macOS (Apple Silicon):** `libhf_xet-macosx-aarch64.dylib.dSYM`
   * **macOS (Intel):** `libhf_xet-macosx-x86_64.dylib.dSYM`
   * **Linux:** match your wheel distribution and arch — check with:
     ```bash
     cat /path/to/site-packages/hf_xet-*.dist-info/WHEEL
     ```
     then use `hf_xet-<manylinux|musllinux>-<x86_64|arm64>.abi3.so.dbg`.
4. Copy the symbol file into the `hf_xet` package directory:
   ```bash
   cp -r hf_xet-1.1.2-manylinux-x86_64.abi3.so.dbg \
       /path/to/site-packages/hf_xet/
   ```
5. Re-run with `RUST_BACKTRACE=full` to get a full backtrace.

## Useful environment variables

```bash
RUST_BACKTRACE=full          # full Rust backtraces on panic
RUST_LOG=info                # enable hf-xet logging
HF_XET_LOG_FILE=/tmp/xet.log # write logs to a file (defaults to stdout)
```
