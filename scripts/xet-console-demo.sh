#!/usr/bin/env bash
# Build hf_xet (release, `console` feature) into hf_xet/venv, rebuild the
# xet-console TUI (release), then open a tmux session:
#   window 0 ("download"): sources the venv and runs your transfer command
#   window 1 ("console"):  runs the xet-console TUI, retrying up to 5 times
#                          while the transfer process brings the server up
#
# Usage:
#   scripts/xet-console-demo.sh [command ...]
#     default command: hf download CohereLabs/North-Mini-Code-1.0
#
# Env:
#   XET_CONSOLE_PORT=NNNN    port for server + TUI (default 6660)
#   XET_DEMO_SKIP_BUILD=1    skip both builds (fast relaunch)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SELF="$ROOT/scripts/xet-console-demo.sh"
SESSION="xet-console-demo"
CONSOLE_BIN="$ROOT/target/release/xet-console"
PORT="${XET_CONSOLE_PORT:-6660}"

# Internal window-1 entrypoint: wait for the console server, then run the TUI.
if [[ "${1:-}" == "__console_wait" ]]; then
    for attempt in 1 2 3 4 5; do
        echo "[xet-console] attempt $attempt/5 — waiting for http://127.0.0.1:$PORT ..."
        if curl -sf --max-time 2 "http://127.0.0.1:$PORT/" >/dev/null 2>&1; then
            "$CONSOLE_BIN" "$PORT" && exit 0
            echo "[xet-console] exited unexpectedly; retrying..."
        fi
        sleep 2
    done
    echo "[xet-console] no server after 5 attempts; run manually: $CONSOLE_BIN $PORT"
    exit 1
fi

if [[ "${XET_DEMO_SKIP_BUILD:-0}" != "1" ]]; then
    echo "==> building xet-console TUI (release)"
    cargo build -p xet-console --release --manifest-path "$ROOT/Cargo.toml"
    echo "==> building hf_xet wheel (release, console feature) into hf_xet/venv"
    (cd "$ROOT/hf_xet" && VIRTUAL_ENV="$PWD/venv" PATH="$PWD/venv/bin:$PATH" \
        ./venv/bin/maturin develop -r --features console)
fi

CMD="hf download CohereLabs/North-Mini-Code-1.0"
if [[ $# -gt 0 ]]; then
    CMD="$(printf '%q ' "$@")"
fi

tmux kill-session -t "$SESSION" 2>/dev/null || true
# Window indexes are 0/1 under tmux's default base-index; windows are targeted
# by name below so a non-default base-index still works.
tmux new-session -d -s "$SESSION" -n download -c "$ROOT/hf_xet"
tmux send-keys -t "$SESSION:download" \
    "export XET_CONSOLE_PORT=$PORT && source venv/bin/activate && $CMD" C-m
tmux new-window -t "$SESSION" -n console -c "$ROOT"
tmux send-keys -t "$SESSION:console" \
    "XET_CONSOLE_PORT=$PORT $(printf '%q' "$SELF") __console_wait" C-m

if [[ -n "${TMUX:-}" ]]; then
    tmux switch-client -t "$SESSION"
elif [[ -t 0 ]]; then
    exec tmux attach -t "$SESSION"
else
    echo "session '$SESSION' ready: tmux attach -t $SESSION"
fi
