#!/usr/bin/env bash
# hf-xet-diag-analyze-latest.sh — Cross-platform dump analyzer
# Finds the latest diagnostics directory and opens the most recent dump
# in the appropriate debugger for your platform (gdb, lldb, or WinDbg).

set -Eeuo pipefail

print_usage() {
  cat <<'USAGE'
Usage: hf-xet-diag-analyze-latest.sh [diagnostics-directory]

Finds and analyzes the latest dump from a diagnostics collection.

Arguments:
  diagnostics-directory  Path to a specific diag_* directory
                        (default: latest diag_* in current directory)

Examples:
  ./hf-xet-diag-analyze-latest.sh
  ./hf-xet-diag-analyze-latest.sh diag_python_hfxet_test_20250127120000

This script will:
  - Auto-detect your OS (Linux, macOS, or Windows)
  - Find the most recent dump file
  - Launch the appropriate debugger:
    * Linux:   gdb with core dumps from dumps/
    * macOS:   lldb with .core files from dumps/
    * Windows: WinDbg with .dmp files from stacks/
USAGE
}

# --- option parsing ---
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  print_usage
  exit 0
fi

DIAG_DIR="${1:-}"

# --- find diagnostics directory ---
if [[ -z "$DIAG_DIR" ]]; then
  # Find the latest diag_* directory in current directory
  DIAG_DIR=$(find . -maxdepth 1 -type d -name "diag_*" -print0 2>/dev/null | \
             xargs -0 ls -dt 2>/dev/null | head -1 || true)
  
  if [[ -z "$DIAG_DIR" ]]; then
    echo "ERROR: No diag_* directories found in current directory."
    echo "Please specify a diagnostics directory or run from a directory containing diag_* folders."
    exit 1
  fi
  
  echo "Found latest diagnostics directory: $DIAG_DIR"
elif [[ ! -d "$DIAG_DIR" ]]; then
  echo "ERROR: Directory not found: $DIAG_DIR"
  exit 1
fi

# --- detect OS ---
OS_TYPE=""
case "${OSTYPE:-}" in
  linux*)   OS_TYPE="linux" ;;
  darwin*)  OS_TYPE="macos" ;;
  msys*|mingw*|cygwin*)  OS_TYPE="windows" ;;
  *)
    # Fallback: check uname
    UNAME=$(uname -s 2>/dev/null || echo "")
    case "$UNAME" in
      Linux*)   OS_TYPE="linux" ;;
      Darwin*)  OS_TYPE="macos" ;;
      MINGW*|MSYS*|CYGWIN*)  OS_TYPE="windows" ;;
      *)
        echo "ERROR: Unsupported OS: ${OSTYPE:-unknown} / ${UNAME:-unknown}"
        exit 1
        ;;
    esac
    ;;
esac

echo "Detected OS: $OS_TYPE"

# --- find latest dump file ---
DUMP_FILE=""

case "$OS_TYPE" in
  linux)
    # Linux: look for core dumps in dumps/ directory
    if [[ -d "$DIAG_DIR/dumps" ]]; then
      DUMP_FILE=$(find "$DIAG_DIR/dumps" -type f -name "core_*" -print0 2>/dev/null | \
                  xargs -0 ls -t 2>/dev/null | head -1 || true)
    fi
    
    if [[ -z "$DUMP_FILE" ]]; then
      echo "ERROR: No core dumps found in $DIAG_DIR/dumps/"
      echo "Core dumps should be named: core_<timestamp>.<pid>"
      exit 1
    fi
    ;;
    
  macos)
    # macOS: look for .core files in dumps/ directory
    if [[ -d "$DIAG_DIR/dumps" ]]; then
      DUMP_FILE=$(find "$DIAG_DIR/dumps" -type f -name "*.core" -print0 2>/dev/null | \
                  xargs -0 ls -t 2>/dev/null | head -1 || true)
    fi
    
    if [[ -z "$DUMP_FILE" ]]; then
      echo "ERROR: No core dumps found in $DIAG_DIR/dumps/"
      echo "Core dumps should be named: dump_<pid>_<timestamp>.core"
      exit 1
    fi
    ;;
    
  windows)
    # Windows: look for .dmp files in stacks/ directory
    if [[ -d "$DIAG_DIR/stacks" ]]; then
      DUMP_FILE=$(find "$DIAG_DIR/stacks" -type f -name "*.dmp" -print0 2>/dev/null | \
                  xargs -0 ls -t 2>/dev/null | head -1 || true)
    fi
    
    if [[ -z "$DUMP_FILE" ]]; then
      echo "ERROR: No dump files found in $DIAG_DIR/stacks/"
      echo "Dump files should be named: dump_<timestamp>.dmp"
      exit 1
    fi
    ;;
esac

echo "Found dump file: $DUMP_FILE"

# --- determine python executable ---
PYTHON_EXE=""
for py_candidate in python3 python; do
  if command -v "$py_candidate" >/dev/null 2>&1; then
    PYTHON_EXE=$(command -v "$py_candidate")
    break
  fi
done

if [[ -z "$PYTHON_EXE" ]]; then
  echo "WARNING: Could not find python executable. Using 'python' as fallback."
  PYTHON_EXE="python"
else
  echo "Using python executable: $PYTHON_EXE"
fi

# --- launch debugger ---
case "$OS_TYPE" in
  linux)
    if ! command -v gdb >/dev/null 2>&1; then
      echo "ERROR: gdb not found. Install with: sudo apt-get install gdb"
      exit 1
    fi
    
    echo ""
    echo "======================================"
    echo "Opening dump in GDB..."
    echo "======================================"
    echo "Useful commands:"
    echo "  (gdb) bt              # backtrace of current thread"
    echo "  (gdb) thread apply all bt  # backtrace of all threads"
    echo "  (gdb) info threads    # list all threads"
    echo "  (gdb) quit            # exit gdb"
    echo "======================================"
    echo ""
    
    exec gdb "$PYTHON_EXE" "$DUMP_FILE"
    ;;
    
  macos)
    if ! command -v lldb >/dev/null 2>&1; then
      echo "ERROR: lldb not found. Install with: xcode-select --install"
      exit 1
    fi
    
    echo ""
    echo "======================================"
    echo "Opening dump in LLDB..."
    echo "======================================"
    echo "Useful commands:"
    echo "  (lldb) bt             # backtrace of current thread"
    echo "  (lldb) thread backtrace all  # backtrace of all threads"
    echo "  (lldb) thread list    # list all threads"
    echo "  (lldb) quit           # exit lldb"
    echo "======================================"
    echo ""
    
    exec lldb -c "$DUMP_FILE" "$PYTHON_EXE"
    ;;
    
  windows)
    # Check for various WinDbg installations
    WINDBG_EXE=""
    
    # Check if windbg is on PATH
    if command -v windbg.exe >/dev/null 2>&1; then
      WINDBG_EXE="windbg.exe"
    elif command -v windbgx.exe >/dev/null 2>&1; then
      WINDBG_EXE="windbgx.exe"
    else
      # Common installation paths
      for dbg_path in \
        "/c/Program Files (x86)/Windows Kits/10/Debuggers/x64/windbg.exe" \
        "/c/Program Files (x86)/Windows Kits/10/Debuggers/x86/windbg.exe" \
        "$PROGRAMFILES/Windows Kits/10/Debuggers/x64/windbg.exe" \
        "${PROGRAMFILES_X86}/Windows Kits/10/Debuggers/x64/windbg.exe"
      do
        if [[ -f "$dbg_path" ]]; then
          WINDBG_EXE="$dbg_path"
          break
        fi
      done
    fi
    
    if [[ -z "$WINDBG_EXE" ]]; then
      echo "ERROR: WinDbg not found."
      echo ""
      echo "Please install WinDbg from:"
      echo "  https://developer.microsoft.com/en-us/windows/downloads/windows-sdk/"
      echo ""
      echo "Or add WinDbg to your PATH."
      echo ""
      echo "You can manually open the dump file:"
      echo "  windbg -z \"$DUMP_FILE\""
      exit 1
    fi
    
    echo ""
    echo "======================================"
    echo "Opening dump in WinDbg..."
    echo "======================================"
    echo "Useful commands:"
    echo "  !analyze -v          # automatic analysis"
    echo "  ~* kb                # backtrace of all threads"
    echo "  ~                    # list all threads"
    echo "  lm                   # list loaded modules"
    echo "  q                    # quit"
    echo "======================================"
    echo ""
    
    # Convert to Windows path format
    DUMP_FILE_WIN=$(cygpath -w "$DUMP_FILE" 2>/dev/null || echo "$DUMP_FILE")
    
    exec "$WINDBG_EXE" -z "$DUMP_FILE_WIN"
    ;;
esac

