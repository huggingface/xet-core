#!/usr/bin/env bash
# hf-xet-diag-windows.sh — Windows diagnostics runner (Git-Bash)
# Runs a target command, periodically snapshots stacks with procdump, and saves dumps.
# Also downloads & installs hf-xet debug symbols (PDB).
# Output directory defaults to include mangled command string for easy correlation.

set -Eeuo pipefail

# Defaults
INTERVAL=120
OUTDIR=""
OUTDIR_SET=""

print_usage() {
  cat <<'USAGE'
Usage: hf-xet-diag-windows.sh [options] -- <command> [args...]

Runs a target command, periodically snapshots stacks with procdump, and saves dumps.
Also downloads & installs hf-xet debug symbols (PDB).

Options:
  -i, --interval SECONDS   Dump snapshot cadence (default: 120)
  -o, --outdir DIR         Output directory (default: diag_<CMD>_<timestamp>)
  -h, --help               Show this help

Examples:
  ./hf-xet-diag-windows.sh -- python hfxet-test.py "Qwen/Qwen2.5-VL-3B-Instruct"
  ./hf-xet-diag-windows.sh -i 30 -- ./server.exe --port 8080
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

# --- ensure procdump available ---
if ! command -v ./procdump.exe >/dev/null 2>&1; then
    echo "procdump.exe not found — downloading..."
    curl -L -o Procdump.zip https://download.sysinternals.com/files/Procdump.zip
    unzip -o Procdump.zip
    chmod +x procdump.exe
fi

# --- build outdir ---
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
echo "Dump interval: ${INTERVAL}s"
echo "Command: ${CMD[*]}"

# --- collect system info ---
{
  echo "=== $(date -Is) ==="
  echo "systeminfo:"; systeminfo || true
  echo
  echo "tasklist snapshot:"; tasklist || true
  echo
  echo "python version:"; python -VV || true
} > "$ENV_LOG" 2>&1 || true

# --- download hf-xet debug symbols (PDB) ---
WHEEL_VERSION=$(pip show hf-xet | awk '/^Version:/{printf $2}')
if [ -z "$WHEEL_VERSION" ]; then
  echo "Error: hf-xet package is not installed. Please install it before running this script." >&2
  exit 1
fi
echo "hf-xet wheel version: $WHEEL_VERSION"
SYMBOL_DIR="symbols-$WHEEL_VERSION"

if [ -d "$SYMBOL_DIR" ]; then
  echo "Existing symbols dir found, assuming previously installed."
else
  SITE_PACKAGES="$(pip show hf-xet | awk '/^Location:/{printf $2}')"
  WHEEL_DIR="$SITE_PACKAGES/hf_xet"
  DIST_INFO="$SITE_PACKAGES/hf_xet-$WHEEL_VERSION.dist-info"
  WHEEL_FILE="$DIST_INFO/WHEEL"

  # Reconstruct wheel name from wheel version and wheel tag
  WHEEL_TAG=$(awk '/^Tag:/{printf $2}' $WHEEL_FILE)
  SYMBOL_FILENAME="hf_xet-$WHEEL_VERSION-$WHEEL_TAG.pdb"

  echo "Downloading debug symbols: $SYMBOL_FILENAME"
  # If the version is of format "1.1.10rc0", change it to our release tag format like "1.1.10-rc0"
  RELEASE_TAG=$(echo -n "$WHEEL_VERSION" | sed 's/\([0-9]\)\(rc.*\)$/\1-\2/')
  DOWNLOAD_URL="https://github.com/huggingface/xet-core/releases/download/v${RELEASE_TAG}/dbg-symbols.zip"
  curl -fL "$DOWNLOAD_URL" -o dbg-symbols.zip
  if [ $? -ne 0 ]; then
      echo "Error: Failed to download debug symbols from $DOWNLOAD_URL" >&2
      exit 1
  fi

  # Extract just the needed symbol file
  unzip dbg-symbols.zip -d "$SYMBOL_DIR"

  # Copy to package directory
  cp -r "$SYMBOL_DIR/dbg-symbols/$SYMBOL_FILENAME" "$WHEEL_DIR/hf_xet.pdb"
  echo "Installed dbg symbol $SYMBOL_FILENAME to $WHEEL_DIR/hf_xet.pdb"
fi

# --- launch target ---
(
  "${CMD[@]}" & echo $! > "$PID_FILE"
) 2>&1 | tee "$CONSOLE_LOG" &
LOGGER_BG=$!

# wait until PID file filled
for _ in {1..50}; do
  [[ -s "$PID_FILE" ]] && break
  sleep 0.1
done
if [[ ! -s "$PID_FILE" ]]; then
  echo "ERROR: Could not determine child PID." | tee -a "$CONSOLE_LOG"
  exit 1
fi
TARGET_PID="$(cat "$PID_FILE")"

# map Git-Bash PID to Windows PID
WINPID=$(ps | awk -v pid="$TARGET_PID" '$1 == pid {print $4}')
if [[ -z "$WINPID" ]]; then
  echo "ERROR: Could not map to Windows PID." | tee -a "$CONSOLE_LOG"
  exit 1
fi

echo "Started PID: $TARGET_PID (Windows PID=$WINPID)" | tee -a "$CONSOLE_LOG"

# --- periodic dump loop ---
LAST_SNAPSHOT_AT=0
while kill -0 "$TARGET_PID" 2>/dev/null; do
  now=$(date +%s)
  if (( now - LAST_SNAPSHOT_AT >= INTERVAL )); then
    ts="$(date +%Y%m%d%H%M%S)"
    dump_file="$OUTDIR/stacks/dump_${ts}.dmp"
    echo "$(date -Is) capturing dump to $dump_file" | tee -a "$CONSOLE_LOG"
    ./procdump.exe -accepteula -mp "$WINPID" "$dump_file" >> "$CONSOLE_LOG" 2>&1 || true
    LAST_SNAPSHOT_AT=$now
  fi
  sleep 1
done

echo "Process $TARGET_PID has exited at $(date -Is)." | tee -a "$CONSOLE_LOG"
echo "Logs and dumps are in: $OUTDIR"
disown "$LOGGER_BG" 2>/dev/null || true
