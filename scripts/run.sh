#!/usr/bin/env bash
# Universal bootstrap + run for the full coaching stack (API, worker, feedback agent, YOLO pose).
#
# Usage:
#   bash scripts/run.sh                 # setup (if needed) + stop old processes + start stack
#   bash scripts/run.sh --start-only    # start only (skip pip/torch setup)
#   bash scripts/run.sh --setup-only    # venv, deps, env files — do not start services
#   bash scripts/run.sh --stop          # stop API :8000, feedback :5055, worker
#   bash scripts/run.sh --status        # health + log paths
#
# Configure once in app/backendapi/.env (and app/agents/.env for OPENAI_API_KEY):
#   FEEDBACK_AGENT_BASE_URL=http://127.0.0.1:5055
#   FEEDBACK_PUBLIC_BASE_URL=https://YOUR-HOST-8000.proxy.runpod.net   # browser review links
#   YOLO_POSE_DEVICE=cuda
#   YOLO_HIGHLIGHT_WEIGHTS=app/yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt
#   FEEDBACK_USE_POSE_PIPELINE=false   # default: YOLO highlight + tactical vision (no pose)
#   OPENAI_API_KEY=sk-...
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

START_ONLY=0
SETUP_ONLY=0
DO_STOP=0
DO_STATUS=0
SKIP_TORCH=0
SKIP_PIP=0

usage() {
  sed -n '2,12p' "$0" | sed 's/^# \?//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-only) START_ONLY=1 ;;
    --setup-only) SETUP_ONLY=1 ;;
    --stop) DO_STOP=1 ;;
    --status) DO_STATUS=1 ;;
    --skip-torch) SKIP_TORCH=1 ;;
    --skip-pip) SKIP_PIP=1 ;;
    -h|--help) usage 0 ;;
    *) echo "Unknown option: $1" >&2; usage 1 ;;
  esac
  shift
done

log() { echo "[run] $*"; }

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
  if [[ -f app/backendapi/.env ]]; then
    set -a
    # shellcheck disable=SC1091
    source app/backendapi/.env
    set +a
  fi
  if [[ -f app/agents/.env ]]; then
    set -a
    # shellcheck disable=SC1091
    source app/agents/.env
    set +a
  fi
  if [[ -f .env ]]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
  fi
  export PYTHONPATH="${ROOT}/app"
}

apply_defaults() {
  export FEEDBACK_AGENT_BASE_URL="${FEEDBACK_AGENT_BASE_URL:-http://127.0.0.1:5055}"
  export FEEDBACK_USE_POSE_PIPELINE="${FEEDBACK_USE_POSE_PIPELINE:-false}"
  export POSE_PIPELINE_OUTPUT_DIR="${POSE_PIPELINE_OUTPUT_DIR:-app/yolo_model/artifacts/pose}"

  if [[ -z "${YOLO_HIGHLIGHT_WEIGHTS:-}" ]]; then
    local w="${ROOT}/app/yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt"
    if [[ -f "$w" ]]; then
      export YOLO_HIGHLIGHT_WEIGHTS="$w"
    fi
  fi

  if [[ -z "${YOLO_POSE_DEVICE:-}" ]]; then
    if command -v nvidia-smi >/dev/null 2>&1; then
      export YOLO_POSE_DEVICE=cuda
    else
      export YOLO_POSE_DEVICE=cpu
    fi
  fi

  if [[ -n "${FEEDBACK_PUBLIC_BASE_URL:-}" ]]; then
    export PUBLIC_BASE_URL="${FEEDBACK_PUBLIC_BASE_URL}"
  fi
}

ensure_env_files() {
  if [[ ! -f app/backendapi/.env ]]; then
    cp app/backendapi/.env.example app/backendapi/.env
    log "Created app/backendapi/.env from example — edit FEEDBACK_PUBLIC_BASE_URL, OPENAI_API_KEY, etc."
  fi
  if [[ ! -f app/agents/.env ]]; then
    cp app/agents/.env.example app/agents/.env
    log "Created app/agents/.env from example — set OPENAI_API_KEY if not in app/backendapi/.env"
  fi
}

ensure_venv() {
  if activate_venv; then
    return 0
  fi
  log "Creating Python venv at venv/"
  python3 -m venv venv
  activate_venv
}

ensure_system_tools() {
  local missing=()
  command -v ffmpeg >/dev/null 2>&1 || missing+=(ffmpeg)
  command -v ffprobe >/dev/null 2>&1 || missing+=(ffprobe)
  if [[ ${#missing[@]} -eq 0 ]]; then
    return 0
  fi

  log "Missing system tools: ${missing[*]}"
  if [[ "$(uname -s)" == "Linux" ]] && command -v apt-get >/dev/null 2>&1; then
    if [[ "$(id -u)" -eq 0 ]]; then
      log "Installing ffmpeg via apt-get..."
      apt-get update -qq && apt-get install -y -qq ffmpeg redis-server >/dev/null
      return 0
    fi
    log "Run as root or: sudo apt-get update && sudo apt-get install -y ffmpeg redis-server"
  elif [[ "$(uname -s)" == "Darwin" ]] && command -v brew >/dev/null 2>&1; then
    log "Install with: brew install ffmpeg redis"
  else
    log "Install ffmpeg before running video coaching jobs."
  fi
  return 0
}

ensure_redis() {
  if ! command -v redis-cli >/dev/null 2>&1; then
    log "WARNING: redis-cli not found — worker queue will fail until Redis is installed"
    return 0
  fi
  if redis-cli ping >/dev/null 2>&1; then
    return 0
  fi
  if command -v redis-server >/dev/null 2>&1; then
    log "Starting redis-server..."
    redis-server --daemonize yes 2>/dev/null || true
    sleep 1
  fi
  redis-cli ping >/dev/null 2>&1 || log "WARNING: Redis not responding"
}

ensure_pip_deps() {
  if [[ "$SKIP_PIP" -eq 1 ]]; then
    return 0
  fi
  log "Installing Python dependencies (requirements.txt)..."
  pip install -q -r requirements.txt
}

torch_ok() {
  python - <<'PY' 2>/dev/null
import torch
from ultralytics import YOLO
dev = "cuda" if torch.cuda.is_available() else "cpu"
torch.zeros(2, 2, device=dev)
PY
}

ensure_torch() {
  if [[ "$SKIP_TORCH" -eq 1 ]]; then
    return 0
  fi
  if torch_ok; then
    log "PyTorch + ultralytics OK"
    return 0
  fi
  log "PyTorch missing or broken — running scripts/install-torch.sh"
  bash "${ROOT}/scripts/install-torch.sh"
}

print_status() {
  echo ""
  echo "=== Agentic stack status ==="
  echo "Repo:     ${ROOT}"
  echo "Python:   $(python -V 2>&1)"
  curl -sf http://127.0.0.1:8000/health >/dev/null && echo "API :8000       OK" || echo "API :8000       not running"
  curl -sf -o /dev/null http://127.0.0.1:5055/health 2>/dev/null && echo "Feedback :5055  OK" || \
    curl -sf -o /dev/null http://127.0.0.1:5055/ 2>/dev/null && echo "Feedback :5055  OK" || echo "Feedback :5055  not running"
  command -v redis-cli >/dev/null 2>&1 && redis-cli ping >/dev/null 2>&1 && echo "Redis           OK" || echo "Redis           not running"
  echo ""
  echo "Config:"
  echo "  FEEDBACK_AGENT_BASE_URL=${FEEDBACK_AGENT_BASE_URL:-not set}"
  echo "  FEEDBACK_PUBLIC_BASE_URL=${FEEDBACK_PUBLIC_BASE_URL:-not set}"
  echo "  YOLO_POSE_DEVICE=${YOLO_POSE_DEVICE:-not set}"
  echo "  YOLO_HIGHLIGHT_WEIGHTS=${YOLO_HIGHLIGHT_WEIGHTS:-not set}"
  echo "  OPENAI_API_KEY=${OPENAI_API_KEY:+set}${OPENAI_API_KEY:-not set}"
  echo ""
  echo "Endpoints (when FEEDBACK_PUBLIC_BASE_URL is set, use that origin):"
  local pub="${FEEDBACK_PUBLIC_BASE_URL:-http://127.0.0.1:8000}"
  echo "  Admin Agents Lab:  ${pub}/admin/agents-lab"
  echo "  API health:        ${pub}/health"
  echo "  Review example:    ${pub}/review/{review_id}"
  echo ""
  echo "Logs:"
  echo "  tail -f /tmp/agentic-api.log /tmp/agentic-worker.log /tmp/agentic-feedback.log"
  echo ""
  echo "Pose JSON output:  ${POSE_PIPELINE_OUTPUT_DIR:-app/yolo_model/artifacts/pose}/job_<agent_job_id>/pose_results.json"
}

if [[ "$DO_STOP" -eq 1 ]]; then
  bash "${ROOT}/scripts/stop-stack.sh"
  exit 0
fi

if [[ "$DO_STATUS" -eq 1 ]]; then
  load_env
  apply_defaults
  activate_venv || true
  print_status
  exit 0
fi

if [[ "$START_ONLY" -eq 0 ]]; then
  ensure_env_files
  ensure_venv
  load_env
  apply_defaults
  ensure_system_tools
  ensure_pip_deps
  ensure_torch
fi

if [[ "$SETUP_ONLY" -eq 1 ]]; then
  log "Setup complete (services not started). Run: bash scripts/run.sh --start-only"
  exit 0
fi

load_env
apply_defaults
activate_venv

log "Stopping any existing stack..."
bash "${ROOT}/scripts/stop-stack.sh"

ensure_redis

log "Starting stack..."
bash "${ROOT}/scripts/start-stack.sh"

print_status
