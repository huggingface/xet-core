#!/usr/bin/env bash
# hf-xet-diag-macos.sh — macOS-only diagnostics runner
# Runs a target command, periodically snapshots stacks with `sample`,
# detects hangs, and can dump cores with `lldb`.
# Installs hf-xet debug symbols if available.
# Output directory defaults to include mangled command string for easy correlation.

set -Eeuo pipefail

# Defaults
INTERVAL=120
OUTDIR=""
OUTDIR_SET=""

print_usage() {
  cat <<'USAGE'
Usage: hf-xet-diag-macos.sh [options] -- <command> [args...]

Runs a target command, periodically snapshots stacks (via `sample`),
detects hangs, and can dump cores (via `lldb`).
Also installs hf-xet debug symbols if available.

Requires: sample, lldb (Xcode Command Line Tools), curl, unzip, pip

Options:
  -i, --interval SECONDS   Stack snapshot cadence (default: 120)
  -o, --outdir DIR         Output directory (default: diag_<CMD>_<timestamp>)
  -h, --help               Show this help

Examples:
  ./hf-xet-diag-macos.sh -- python hfxet-test.py "Qwen/Qwen2.5-VL-3B-Instruct"
  ./hf-xet-diag-macos.sh -i 60 -- ./myapp --flag
USAGE
}

# --- option parsing ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    -i|--interval) INTERVAL="${2:-}"; shift 2 ;;
    -o|--outdir)   OUTDIR="${2:-}"; OUTDIR_SET=1; shift 2 ;;
    -h|--help)     print_usage; exit 0 ;;
    --)            shift; break ;;
    *)             break ;;
  esac
done
if [[ $# -lt 1 ]]; then
  echo "ERROR: No command provided."
  print_usage; exit 2
fi
CMD=( "$@" )

# Tool availability check
missing=()
for cmd in sample lldb curl unzip; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    missing+=("$cmd")
  fi
done
if [ ${#missing[@]} -ne 0 ]; then
  echo "Missing required tools: ${missing[*]}"
  echo "Install Xcode Command Line Tools with:"
  echo "  xcode-select --install"
  exit 2
fi

# If no outdir given, generate one based on command
if [[ -z "$OUTDIR_SET" ]]; then
  CMD_STR="${CMD[*]}"
  SAFE_CMD=$(echo "$CMD_STR" | tr -c 'A-Za-z0-9' '_' )
  OUTDIR="diag_${SAFE_CMD}_$(date +%Y%m%d%H%M%S)"
fi

mkdir -p "$OUTDIR"/{stacks,dumps}
CONSOLE_LOG="$OUTDIR/console.log"
ENV_LOG="$OUTDIR/env.log"
PID_FILE="$OUTDIR/pid"

echo "Diagnostics output: $OUTDIR"
echo "Stack trace interval: ${INTERVAL}s"
echo "Command: ${CMD[*]}"

# --- collect some quick system info ---
{
  echo "=== $(date "+%Y-%m-%dT%H:%M:%S%z") ==="
  echo "uname -a:"; uname -a
  echo
  echo "top snapshot:"; top -l 1 | grep -E "^CPU|^Phys" || true
  echo
  echo "ulimit -a:"; ulimit -a || true
  echo
  echo "python version:"; python3 --version || true
  echo
} > "$ENV_LOG" 2>&1 || true

# --- download hf-xet dbg symbols ---
WHEEL_VERSION=$(pip show hf-xet 2>/dev/null | grep Version | cut -d ' ' -f2 || true)
if [ -n "$WHEEL_VERSION" ]; then
  echo "hf-xet wheel version: $WHEEL_VERSION"
  SYMBOL_DIR="symbols-$WHEEL_VERSION"
  if [ -d "$SYMBOL_DIR" ]; then
    echo "Existing symbols dir found, assuming previously installed."
  else
    LABEL="$(pip show hf-xet | grep Location | cut -d' ' -f2)/hf_xet"
    ARCH=$(uname -m)
    if [ "$ARCH" = "x86_64" ]; then
      SYMBOL_FILENAME="libhf_xet-macosx-x86_64.dylib.dSYM"
    else
      SYMBOL_FILENAME="libhf_xet-macosx-aarch64.dylib.dSYM"
    fi

    echo "Downloading debug symbols: $SYMBOL_FILENAME"
    DOWNLOAD_URL="https://github.com/huggingface/xet-core/releases/download/v${WHEEL_VERSION}/dbg-symbols.zip"
    curl -L "$DOWNLOAD_URL" -o dbg-symbols.zip

    unzip dbg-symbols.zip -d "$SYMBOL_DIR"
    cp -r "$SYMBOL_DIR/dbg-symbols/$SYMBOL_FILENAME" "$LABEL/" || true
    echo "Installed dbg symbol $SYMBOL_FILENAME to $LABEL"
  fi
else
  echo "hf-xet not installed or no wheel version found; skipping debug symbol download."
fi

# --- launch target ---
echo "Launching target at $(date "+%Y-%m-%dT%H:%M:%S%z") ..." | tee -a "$CONSOLE_LOG"

(
  "${CMD[@]}" & echo $! > "$PID_FILE"
) 2>&1 | tee -a "$CONSOLE_LOG" &
LOGGER_BG=$!

# read PID
for _ in {1..50}; do
  [[ -s "$PID_FILE" ]] && break
  sleep 0.1
done
if [[ ! -s "$PID_FILE" ]]; then
  echo "ERROR: Could not determine child PID." | tee -a "$CONSOLE_LOG"
  exit 1
fi
TARGET_PID="$(cat "$PID_FILE")"
echo "Started PID: $TARGET_PID" | tee -a "$CONSOLE_LOG"

# --- stack capture + hang detection ---
declare -a LAST_STACKS=()

capture_stack() {
  local ts stack_file
  ts="$(date +%Y%m%d%H%M%S)"
  stack_file="$OUTDIR/stacks/stack_${ts}.txt"

  sample "$TARGET_PID" 5 -file "$stack_file" || true
  echo "$(date "+%Y-%m-%dT%H:%M:%S%z") captured stack -> $stack_file" | tee -a "$CONSOLE_LOG"

  LAST_STACKS+=("$stack_file")
  if (( ${#LAST_STACKS[@]} > 3 )); then
    LAST_STACKS=("${LAST_STACKS[@]: -3}")
  fi
  check_hang
}

check_hang() {
  # need three snapshots to decide
  if (( ${#LAST_STACKS[@]} < 3 )); then return; fi

  # normalize: strip addresses and empty lines
  norm1=$(sed 's/0x[0-9a-f]\+//g' "${LAST_STACKS[0]}" | grep -v '^$')
  norm2=$(sed 's/0x[0-9a-f]\+//g' "${LAST_STACKS[1]}" | grep -v '^$')
  norm3=$(sed 's/0x[0-9a-f]\+//g' "${LAST_STACKS[2]}" | grep -v '^$')

  diff12=$(diff <(echo "$norm1") <(echo "$norm2") || true)
  diff23=$(diff <(echo "$norm2") <(echo "$norm3") || true)

  # If either diff is non-empty => stacks changed -> NOT a hang
  if [[ -n "$diff12" || -n "$diff23" ]]; then
    return
  fi

  # Otherwise both diffs empty => stacks the same across 3 snapshots => HANG
  echo "⚠️ Hang detected at $(date "+%Y-%m-%dT%H:%M:%S%z") — taking core dump." | tee -a "$CONSOLE_LOG"
  take_core_dump
  LAST_STACKS=()
}

take_core_dump() {
  local ts core_file
  ts="$(date +%Y%m%d%H%M%S)"
  core_file="$OUTDIR/dumps/dump_${TARGET_PID}_${ts}.core"

  lldb -p "$TARGET_PID" -o "process save-core $core_file" -o "quit" >>"$CONSOLE_LOG" 2>&1 || true
  echo "Core dump saved: $core_file" | tee -a "$CONSOLE_LOG"
}

# --- monitoring loop ---
LAST_SNAPSHOT_AT=0
while kill -0 "$TARGET_PID" 2>/dev/null; do
  now=$(date +%s)
  if (( now - LAST_SNAPSHOT_AT >= INTERVAL )); then
    capture_stack || true
    LAST_SNAPSHOT_AT=$now
  fi
  sleep 1
done

echo "Process $TARGET_PID has exited at $(date "+%Y-%m-%dT%H:%M:%S%z")." | tee -a "$CONSOLE_LOG"
echo "Logs and stacks are in: $OUTDIR"
disown "$LOGGER_BG" 2>/dev/null || true

