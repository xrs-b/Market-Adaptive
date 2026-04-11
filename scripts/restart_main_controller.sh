#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

CONFIG_PATH="${1:-config/config.yaml}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"
LOG_FILE="logs/main_controller.log"
CMD=("$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/run_main_controller.py" --config "$CONFIG_PATH" --log-level "$LOG_LEVEL")
MATCH="$ROOT_DIR/scripts/run_main_controller.py"

pids="$(pgrep -f "$MATCH" || true)"
if [[ -n "$pids" ]]; then
  echo "Stopping existing main controller: $pids"
  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    kill "$pid" || true
  done <<< "$pids"

  deadline=$((SECONDS + 15))
  while pgrep -f "$MATCH" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      remaining="$(pgrep -f "$MATCH" || true)"
      if [[ -n "$remaining" ]]; then
        echo "Force killing lingering main controller: $remaining"
        while IFS= read -r pid; do
          [[ -z "$pid" ]] && continue
          kill -9 "$pid" || true
        done <<< "$remaining"
      fi
      break
    fi
    sleep 1
  done
fi

mkdir -p logs
nohup "${CMD[@]}" > "$LOG_FILE" 2>&1 &
new_pid=$!
echo "Started main controller: $new_pid"
