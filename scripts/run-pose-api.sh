#!/usr/bin/env bash
# RunPod (or any GPU host) — pose API without a custom Docker image.
#
# Use a RunPod PyTorch GPU pod, SSH or web terminal, clone this repo, then:
#   cd agentic
#   bash scripts/run-pose-api.sh              # setup venv + deps + start :5060
#   bash scripts/run-pose-api.sh --setup-only
#   bash scripts/run-pose-api.sh --start-only
#   bash scripts/run-pose-api.sh --stop
#   bash scripts/run-pose-api.sh --status
#
# RunPod console: expose TCP port 5060 → proxy URL like https://POD_ID-5060.proxy.runpod.net
# Main VM feedback-agent (app/agents/.env):
#   POSE_API_BASE_URL=https://POD_ID-5060.proxy.runpod.net
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PORT="${POSE_API_PORT:-5060}"
PID_FILE="${ROOT}/.pose-api.pid"
LOG_FILE="${ROOT}/logs/pose-api.log"

START_ONLY=0
SETUP_ONLY=0
DO_STOP=0
DO_STATUS=0
SKIP_TORCH=0

usage() {
  sed -n '2,14p' "$0" | sed 's/^# \?//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-only) START_ONLY=1 ;;
    --setup-only) SETUP_ONLY=1 ;;
    --stop) DO_STOP=1 ;;
    --status) DO_STATUS=1 ;;
    --skip-torch) SKIP_TORCH=1 ;;
    -h|--help) usage 0 ;;
    *) echo "Unknown option: $1" >&2; usage 1 ;;
  esac
  shift
done

log() { echo "[pose-api] $*"; }

activate_venv() {
  if [[ -f venv/bin/activate ]]; then
    # shellcheck disable=SC1091
    source venv/bin/activate
  elif [[ -f .venv/bin/activate ]]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
  else
    return 1
  fi
}

load_env() {
  for f in app/pose_api/.env app/agents/.env app/backendapi/.env .env; do
    if [[ -f "$f" ]]; then
      set -a
      # shellcheck disable=SC1091
      source "$f"
      set +a
    fi
  done
}

setup_venv() {
  if [[ ! -f venv/bin/activate ]]; then
    log "Creating venv…"
    python3 -m venv venv
  fi
  activate_venv
}

setup_deps() {
  setup_venv
  log "Installing pose API Python deps…"
  pip install --upgrade pip
  pip install --no-cache-dir -r app/pose_api/requirements.txt
  if [[ "$SKIP_TORCH" -eq 0 ]]; then
    log "Installing PyTorch (CUDA if GPU present)…"
    bash scripts/install-torch.sh
  fi
  mkdir -p logs "${DATA_DIR:-$ROOT/data/pose_api}" \
    "${POSE_PIPELINE_OUTPUT_DIR:-$ROOT/app/yolo_model/artifacts/pose}"
}

stop_server() {
  if [[ -f "$PID_FILE" ]]; then
    pid="$(cat "$PID_FILE")"
    if kill -0 "$pid" 2>/dev/null; then
      log "Stopping pose-api pid $pid"
      kill "$pid" 2>/dev/null || true
      sleep 1
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
  pkill -f "uvicorn pose_api.main:app" 2>/dev/null || true
}

start_server() {
  activate_venv || { log "No venv — run without --start-only first"; exit 1; }
  load_env

  export PYTHONPATH="${PYTHONPATH:-$ROOT/app}"
  export YOLO_POSE_DEVICE="${YOLO_POSE_DEVICE:-cuda}"
  export YOLO_HIGHLIGHT_WEIGHTS="${YOLO_HIGHLIGHT_WEIGHTS:-$ROOT/app/yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt}"
  export DATA_DIR="${DATA_DIR:-$ROOT/data/pose_api}"
  export POSE_PIPELINE_OUTPUT_DIR="${POSE_PIPELINE_OUTPUT_DIR:-$ROOT/app/yolo_model/artifacts/pose}"
  export HOST="${HOST:-0.0.0.0}"

  if [[ ! -f "$YOLO_HIGHLIGHT_WEIGHTS" ]]; then
    log "ERROR: highlight weights not found: $YOLO_HIGHLIGHT_WEIGHTS"
    exit 1
  fi

  stop_server
  mkdir -p "$(dirname "$LOG_FILE")"

  log "Starting pose API on ${HOST}:${PORT} (device=$YOLO_POSE_DEVICE)"
  log "Logs: $LOG_FILE"
  nohup uvicorn pose_api.main:app --host "$HOST" --port "$PORT" >>"$LOG_FILE" 2>&1 &
  echo $! >"$PID_FILE"
  sleep 2
  if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    log "Failed to start — tail $LOG_FILE"
    tail -30 "$LOG_FILE" || true
    exit 1
  fi
  log "PID $(cat "$PID_FILE") — curl http://127.0.0.1:${PORT}/health"
}

print_status() {
  load_env
  if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    log "Running pid $(cat "$PID_FILE") port ${PORT:-5060}"
  else
    log "Not running"
  fi
  curl -sf "http://127.0.0.1:${PORT:-5060}/health" | python3 -m json.tool 2>/dev/null || log "Health check failed"
}

[[ "$DO_STOP" -eq 1 ]] && { stop_server; log "Stopped"; exit 0; }
[[ "$DO_STATUS" -eq 1 ]] && { print_status; exit 0; }

if [[ "$START_ONLY" -eq 0 ]]; then
  setup_deps
fi

[[ "$SETUP_ONLY" -eq 1 ]] && { log "Setup complete"; exit 0; }

start_server
print_status
