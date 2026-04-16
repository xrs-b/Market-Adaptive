#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/Market-Adaptive/.venv/bin/python}"
CONFIG_PATH="${1:-$ROOT_DIR/Market-Adaptive/config/config.yaml}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/Market-Adaptive/logs}"
LOG_FILE="$LOG_DIR/main_controller.log"
ARCHIVE_DIR="$LOG_DIR/archive"
CMD=("$PYTHON_BIN" "$ROOT_DIR/scripts/run_main_controller.py" --config "$CONFIG_PATH" --log-level "$LOG_LEVEL")
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

mkdir -p "$LOG_DIR" "$ARCHIVE_DIR"
if [[ -f "$LOG_FILE" && -s "$LOG_FILE" ]]; then
  ts="$(date +"%Y%m%d-%H%M%S")"
  archive_file="$ARCHIVE_DIR/main_controller-$ts.log"
  mv "$LOG_FILE" "$archive_file"
  echo "Archived previous main controller log: $archive_file"
fi
nohup "${CMD[@]}" > "$LOG_FILE" 2>&1 &
new_pid=$!
echo "Started main controller: $new_pid"
