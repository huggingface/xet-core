#!/usr/bin/env bash
# hf-xet-diag.sh — Linux-only diagnostics runner
# Runs a target command, periodically snapshots stacks, detects hangs, and can dump cores.
# Output directory defaults to include mangled command string for easy correlation.

set -Eeuo pipefail

# Defaults
INTERVAL=120
OUTDIR=""
PRELOAD_HELPER=true
OUTDIR_SET=""

print_usage() {
  cat <<'USAGE'
Usage: hf-xet-diag-linux.sh [options] -- <command> [args...]

Runs a target command, periodically snapshots stacks, detects hangs, and can dump cores.
Output directory defaults to include mangled command string for easy correlation.

Uses gdb for stack snapshots and hang detection.

Requires gdb, gcc, gcore. Install on Linux with:
  sudo apt-get install gdb build-essential

Options:
  -i, --interval SECONDS   Stack snapshot cadence (default: 120)
  -o, --outdir DIR         Output directory (default: diag_<CMD>_<timestamp>)
      --no-preload         Do NOT preload ptrace-bypass helper
  -h, --help               Show this help

Examples:
  ./hf-xet-diag-linux.sh -- python hfxet-test.py "Qwen/Qwen2.5-VL-3B-Instruct"
  ./hf-xet-diag-linux.sh -i 30 -o diag -- ./server --port 8080
USAGE
}

# --- option parsing ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    -i|--interval) INTERVAL="${2:-}"; shift 2 ;;
    -o|--outdir)   OUTDIR="${2:-}"; OUTDIR_SET=1; shift 2 ;;
    --no-preload)  PRELOAD_HELPER=false; shift ;;
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

missing=()
for cmd in gdb gcore; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    missing+=("$cmd")
  fi
done

if [ ${#missing[@]} -ne 0 ]; then
    echo "Missing required tools: ${missing[*]}"
    echo ""
    echo "Requires gdb & gcore. Install on Linux with:"
    echo "  sudo apt-get install gdb"
    exit 2
fi

# If no outdir given, generate one based on command
if [[ -z "$OUTDIR_SET" ]]; then
  CMD_STR="${CMD[*]}"
  SAFE_CMD=$(echo "$CMD_STR" | tr -cs 'A-Za-z0-9._-' '_' )
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
  echo "=== $(date -Is) ==="
  echo "uname -a:"; uname -a
  echo
  echo "top snapshot:"; top -b -n 1 | grep -E "^%Cpu|^MiB Mem|^MiB Swap" || true
  echo
  echo "ulimit -a:"; ulimit -a || true
  echo 
  echo "python version:"; python --version || true
  echo
} > "$ENV_LOG" 2>&1 || true

# --- ptrace helper build (optional) ---
ALLOW_PTRACE_SO="/tmp/liballow_ptrace.so"
maybe_build_ptrace_helper() {
  [[ "$PRELOAD_HELPER" == true ]] || return 0
  [[ -f "$ALLOW_PTRACE_SO" ]] && return 0
  if ! command -v gcc >/dev/null 2>&1; then
    echo "Note: gcc not found; skipping ptrace helper." | tee -a "$CONSOLE_LOG"
    return 0
  fi
  cat >/tmp/allow-ptrace.c <<'EOF'
#define _GNU_SOURCE
#include <sys/prctl.h>
#ifndef PR_SET_PTRACER
#define PR_SET_PTRACER 0x59616d61
#endif
#ifndef PR_SET_PTRACER_ANY
#define PR_SET_PTRACER_ANY ((unsigned long)-1)
#endif
__attribute__((constructor))
static void allow_ptrace_ctor(void) {
    prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
}
EOF
  gcc -shared -fPIC -O2 -o "$ALLOW_PTRACE_SO" /tmp/allow-ptrace.c 2>>"$CONSOLE_LOG" || {
    echo "Warning: failed to build ptrace helper; proceeding without." | tee -a "$CONSOLE_LOG"
    return
  }
  echo "Built ptrace helper: $ALLOW_PTRACE_SO" | tee -a "$CONSOLE_LOG"
}
maybe_build_ptrace_helper

# --- download hf-xet dbg symbols ---
WHEEL_VERSION=$(pip show hf-xet | grep Version | cut -d ' ' -f2)
if [ -z "$WHEEL_VERSION" ]; then
  echo "Error: hf-xet package is not installed. Please install it before running this script." >&2
  exit 1
fi
echo "hf-xet wheel version: $WHEEL_VERSION"
SYMBOL_DIR="symbols-$WHEEL_VERSION"

if [ -d "$SYMBOL_DIR" ]; then
  echo "Existing symbols dir found, assuming previously installed."
else
  LABEL="$(pip show hf-xet | grep Location | cut -d' ' -f2)/hf_xet"
  SITE_PACKAGES="$(pip show hf-xet | grep Location | cut -d' ' -f2)"
  DIST_NAME="$(pip show hf-xet | awk '/^Name:/{print $2}')"
  DIST_INFO="$(find "$SITE_PACKAGES" -maxdepth 1 -type d -iname "${DIST_NAME//-/_}-*.dist-info" | head -n1)"
  # Determine wheel type from WHEEL file
  WHEEL_FILE="$DIST_INFO/WHEEL"
  # Extract first platform tag (Tag: cp311-cp311-manylinux_2_28_x86_64)
  WHEEL_PLATFORM_TAG=$(grep '^Tag:' "$WHEEL_FILE" | head -n1 | awk -F'-' '{print $NF}')
  echo "Wheel platform tag: $WHEEL_PLATFORM_TAG"
  # Extract Linux type (musl vs manylinux) and arch
  LINUX_TYPE="$(echo "$WHEEL_PLATFORM_TAG" | grep -oE '^(musllinux|manylinux)')"
  ARCH=$(uname -m)
  # Normalize architecture names to match your debug symbols naming
  if [[ "$ARCH" == "aarch64" ]]; then
      ARCH="arm64"
  fi
  SYMBOL_FILENAME="hf_xet-${LINUX_TYPE}-${ARCH}.abi3.so.dbg"

  echo "Downloading debug symbols: $SYMBOL_FILENAME"
  # We assume the debug symbols archive is available at a predictable URL:
  # e.g. https://github.com/huggingface/xet-core/releases/download/latest/dbg-symbols.zip
  if [ -n "$WHEEL_VERSION" ]; then
      DOWNLOAD_URL="https://github.com/huggingface/xet-core/releases/download/v${WHEEL_VERSION}/dbg-symbols.zip"
  else
      DOWNLOAD_URL="https://github.com/huggingface/xet-core/releases/latest/download/dbg-symbols.zip"
  fi
  curl -L "$DOWNLOAD_URL" -o dbg-symbols.zip
  if [ $? -ne 0 ]; then
      echo "Error: Failed to download debug symbols from $DOWNLOAD_URL" >&2
      exit 1
  fi

  # Extract just the needed symbol file
  unzip dbg-symbols.zip -d "$SYMBOL_DIR"

  # Copy to package directory
  cp -r $SYMBOL_DIR/dbg-symbols/"$SYMBOL_FILENAME" "$LABEL/"
  echo "Installed dbg symbol $SYMBOL_FILENAME to $LABEL"
fi

# --- launch target ---
echo "Launching target at $(date -Is) ..." | tee -a "$CONSOLE_LOG"

LAUNCH_ENV=()
if [[ "$PRELOAD_HELPER" == true && -f "$ALLOW_PTRACE_SO" ]]; then
  LAUNCH_ENV=( "LD_PRELOAD=$ALLOW_PTRACE_SO${LD_PRELOAD:+:$LD_PRELOAD}" )
  echo "Using LD_PRELOAD=$ALLOW_PTRACE_SO to relax ptrace restrictions." | tee -a "$CONSOLE_LOG"
fi

if [[ ${#LAUNCH_ENV[@]} -gt 0 ]]; then
  (
    env "${LAUNCH_ENV[@]}" "${CMD[@]}" & echo $! > "$PID_FILE"
  ) 2>&1 | tee -a "$CONSOLE_LOG" &
else
  (
    "${CMD[@]}" & echo $! > "$PID_FILE"
  ) 2>&1 | tee -a "$CONSOLE_LOG" &
fi
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

  if command -v gdb >/dev/null 2>&1; then
    gdb -p "$TARGET_PID" -batch \
      -ex "set pagination off" \
      -ex "thread apply all bt full" >"$stack_file" 2>&1 || true
  elif command -v eu-stack >/dev/null 2>&1; then
    eu-stack -p "$TARGET_PID" >"$stack_file" 2>&1 || true
  elif command -v pstack >/dev/null 2>&1; then
    pstack "$TARGET_PID" >"$stack_file" 2>&1 || true
  else
    for t in /proc/"$TARGET_PID"/task/*/stack; do
      echo "=== $t ==="; cat "$t"; echo
    done >"$stack_file" 2>&1 || true
  fi

  echo "$(date -Is) captured stack -> $stack_file" | tee -a "$CONSOLE_LOG"

  LAST_STACKS+=("$stack_file")
  if (( ${#LAST_STACKS[@]} > 3 )); then
    LAST_STACKS=("${LAST_STACKS[@]: -3}")
  fi
  check_hang
}

check_hang() {
  if (( ${#LAST_STACKS[@]} < 3 )); then return; fi
  norm1=$(sed 's/0x[0-9a-f]\+//g' "${LAST_STACKS[0]}" | grep -v '^$')
  norm2=$(sed 's/0x[0-9a-f]\+//g' "${LAST_STACKS[1]}" | grep -v '^$')
  norm3=$(sed 's/0x[0-9a-f]\+//g' "${LAST_STACKS[2]}" | grep -v '^$')

  diff12=$(diff <(echo "$norm1") <(echo "$norm2") || true)
  diff23=$(diff <(echo "$norm2") <(echo "$norm3") || true)

  # if no diff between stacks 1-2 and 2-3, we have a hang
  if [[ -n "$diff12" || -n "$diff23" ]]; then 
    return
  fi

  echo "⚠️ Hang heuristic triggered at $(date -Is)" | tee -a "$CONSOLE_LOG"
  take_core_dump
  LAST_STACKS=()
}

take_core_dump() {
  local ts core_file
  ts="$(date +%Y%m%d%H%M%S)"
  core_file="$OUTDIR/dumps/core_${ts}"

  if command -v gcore >/dev/null 2>&1; then
    gcore -o "$core_file" "$TARGET_PID" >>"$CONSOLE_LOG" 2>&1 || true
    echo "Core dump saved: ${core_file}.${TARGET_PID}" | tee -a "$CONSOLE_LOG"
  else
    echo "gcore not available, saving partial /proc/$TARGET_PID/mem dump" | tee -a "$CONSOLE_LOG"
    dd if="/proc/$TARGET_PID/mem" of="${core_file}.raw" bs=1M count=50 status=none 2>>"$CONSOLE_LOG" || true
    echo "Partial raw dump saved: ${core_file}.raw" | tee -a "$CONSOLE_LOG"
  fi
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

echo "Process $TARGET_PID has exited at $(date -Is)." | tee -a "$CONSOLE_LOG"
echo "Logs and stacks are in: $OUTDIR"
disown "$LOGGER_BG" 2>/dev/null || true
