#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
CONFIG_PATH="${1:-$ROOT_DIR/config/config.yaml}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/logs}"
LOG_FILE="$LOG_DIR/main_controller.log"
ARCHIVE_DIR="$LOG_DIR/archive"
PLIST_NAME="com.market_adaptive.main_controller"
CMD=("$PYTHON_BIN" "$ROOT_DIR/scripts/run_main_controller.py" --config "$CONFIG_PATH" --log-level "$LOG_LEVEL")

# Use launchctl to restart the launchd-managed service, avoiding double-instance race
restart_via_launchd() {
  local uid
  uid=$(id -u)
  # Bootout stops the service and prevents launchd from auto-restarting it within the window
  if launchctl bootout "gui/${uid}/${PLIST_NAME}" 2>/dev/null; then
    echo "launchd: stopped ${PLIST_NAME}"
  else
    echo "launchd: ${PLIST_NAME} not loaded or already stopped"
  fi

  # Small settle time to ensure launchd has unregistered the job
  sleep 2

  # Bootstrap restarts the service under launchd management
  if launchctl bootstrap "gui/${uid}" ~/Library/LaunchAgents/"${PLIST_NAME}".plist 2>/dev/null; then
    echo "launchd: restarted ${PLIST_NAME}"
  else
    echo "launchd: bootstrap failed, starting manually"
    mkdir -p "$LOG_DIR" "$ARCHIVE_DIR"
    if [[ -f "$LOG_FILE" && -s "$LOG_FILE" ]]; then
      ts="$(date +"%Y%m%d-%H%M%S")"
      mv "$LOG_FILE" "$ARCHIVE_DIR/main_controller-$ts.log"
    fi
    nohup "${CMD[@]}" > "$LOG_FILE" 2>&1 &
    echo "Started main controller: $!"
  fi
}

restart_via_launchd
