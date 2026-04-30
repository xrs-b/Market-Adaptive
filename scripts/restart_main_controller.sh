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

# Prefer launchd supervision; only fall back to manual start as a last resort.
restart_via_launchd() {
  local uid plist_path
  uid=$(id -u)
  plist_path="$HOME/Library/LaunchAgents/${PLIST_NAME}.plist"

  if launchctl print "gui/${uid}/${PLIST_NAME}" >/dev/null 2>&1; then
    echo "launchd: kickstart ${PLIST_NAME}"
    if launchctl kickstart -k "gui/${uid}/${PLIST_NAME}" >/dev/null 2>&1; then
      echo "launchd: restarted ${PLIST_NAME}"
      return 0
    fi
    echo "launchd: kickstart failed, trying bootout/bootstrap"
  fi

  if launchctl bootout "gui/${uid}/${PLIST_NAME}" >/dev/null 2>&1; then
    echo "launchd: stopped ${PLIST_NAME}"
  else
    echo "launchd: ${PLIST_NAME} not loaded or already stopped"
  fi

  sleep 2

  if launchctl bootstrap "gui/${uid}" "$plist_path" >/dev/null 2>&1; then
    echo "launchd: restarted ${PLIST_NAME}"
    return 0
  fi

  if launchctl kickstart -k "gui/${uid}/${PLIST_NAME}" >/dev/null 2>&1; then
    echo "launchd: restarted ${PLIST_NAME} via kickstart after bootstrap miss"
    return 0
  fi

  echo "launchd: bootstrap/kickstart failed, starting manually"
  mkdir -p "$LOG_DIR" "$ARCHIVE_DIR"
  if [[ -f "$LOG_FILE" && -s "$LOG_FILE" ]]; then
    ts="$(date +"%Y%m%d-%H%M%S")"
    mv "$LOG_FILE" "$ARCHIVE_DIR/main_controller-$ts.log"
  fi
  nohup "${CMD[@]}" > "$LOG_FILE" 2>&1 &
  echo "Started main controller: $!"
}

restart_via_launchd
