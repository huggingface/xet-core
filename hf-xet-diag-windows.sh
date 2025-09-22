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
  echo "python version:"; python --version || true
} > "$ENV_LOG" 2>&1 || true

# --- download hf-xet debug symbols (PDB) ---
WHEEL_VERSION=$(pip show hf-xet 2>/dev/null | awk '/Version:/{print $2}')
if [[ -n "$WHEEL_VERSION" ]]; then
  SYMBOL_DIR="symbols-$WHEEL_VERSION"
  LABEL="$(pip show hf-xet | awk '/Location:/{print $2}')/hf_xet"
  SYMBOL_FILENAME="hf_xet.pdb"

  if [[ -d "$SYMBOL_DIR" ]]; then
    echo "Existing symbols dir found, assuming previously installed."
  else
    echo "Downloading debug symbols for hf-xet v$WHEEL_VERSION"
    DOWNLOAD_URL="https://github.com/huggingface/xet-core/releases/download/v${WHEEL_VERSION}/dbg-symbols.zip"
    curl -L "$DOWNLOAD_URL" -o dbg-symbols.zip || {
      echo "Warning: failed to download debug symbols." | tee -a "$CONSOLE_LOG"
    }
    if [[ -f dbg-symbols.zip ]]; then
      unzip -o dbg-symbols.zip -d "$SYMBOL_DIR"
    else
      echo "Warning: dbg-symbols.zip not found, skipping unzip." | tee -a "$CONSOLE_LOG"
    fi
    if [[ -f "$SYMBOL_DIR/dbg-symbols/$SYMBOL_FILENAME" ]]; then
      cp "$SYMBOL_DIR/dbg-symbols/$SYMBOL_FILENAME" "$LABEL/" || true
      echo "Installed dbg symbol $SYMBOL_FILENAME to $LABEL"
    else
      echo "Warning: $SYMBOL_FILENAME not found in archive." | tee -a "$CONSOLE_LOG"
    fi
  fi
else
  echo "hf-xet not installed — skipping debug symbol installation." | tee -a "$CONSOLE_LOG"
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
