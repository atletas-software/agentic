#!/usr/bin/env bash
# RunPod (or any GPU host) — pose API without a custom Docker image.
#
# RunPod PyTorch pods already include CUDA torch + often cv2/ultralytics.
# This script SKIPS pip entirely when imports already work (~0s setup).
#
#   cd agentic
#   bash scripts/run-pose-api.sh --use-system-python
#   bash scripts/run-pose-api.sh --start-only          # no pip at all
#   bash scripts/run-pose-api.sh --skip-pip              # same
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
DO_CHECK=0
SKIP_TORCH=0
SKIP_PIP=0
USE_VENV=-1

usage() {
  sed -n '2,14p' "$0" | sed 's/^# \?//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-only) START_ONLY=1; SKIP_PIP=1 ;;
    --setup-only) SETUP_ONLY=1 ;;
    --stop) DO_STOP=1 ;;
    --status) DO_STATUS=1 ;;
    --check) DO_CHECK=1 ;;
    --skip-torch) SKIP_TORCH=1 ;;
    --skip-pip) SKIP_PIP=1 ;;
    --use-system-python) USE_VENV=0 ;;
    --use-venv) USE_VENV=1 ;;
    -h|--help) usage 0 ;;
    *) echo "Unknown option: $1" >&2; usage 1 ;;
  esac
  shift
done

log() { echo "[pose-api] $*"; }

should_use_system_python() {
  if [[ "$USE_VENV" -eq 0 ]]; then return 0; fi
  if [[ "$USE_VENV" -eq 1 ]]; then return 1; fi
  if [[ "${POSE_API_USE_SYSTEM_PYTHON:-}" == "1" ]]; then return 0; fi
  if [[ -n "${RUNPOD_POD_ID:-}" ]] || [[ -d /runpod-volume ]] || [[ -d /workspace ]]; then
    return 0
  fi
  if python3 -c "import torch; assert torch.cuda.is_available()" 2>/dev/null; then
    return 0
  fi
  return 1
}

activate_python() {
  if should_use_system_python; then
    if [[ -d "$ROOT/venv" ]] || [[ -d "$ROOT/.venv" ]]; then
      log "WARNING: venv/ exists but using system Python. For faster setup: rm -rf venv .venv"
    fi
    log "Using system Python (RunPod / CUDA — no venv)"
    return 0
  fi
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

# Returns 0 if pose API can start without any pip install.
stack_ready() {
  PYTHONPATH="${ROOT}/app" python3 - <<'PY'
import sys
mods = ("torch", "cv2", "fastapi", "uvicorn", "httpx", "dotenv", "ultralytics")
missing = []
for m in mods:
    try:
        __import__(m)
        print("ok", m)
    except ImportError:
        missing.append(m)
        print("missing", m)
if missing:
    print("missing:", ",".join(sorted(set(missing))), file=sys.stderr)
    sys.exit(1)
import torch
print("cuda", torch.cuda.is_available(), "torch", torch.__version__)
PY
}

pip_install_if_missing() {
  local mod=$1
  shift
  if python3 -c "import ${mod}" 2>/dev/null; then
    log "  already have ${mod}"
    return 0
  fi
  log "  pip install $* …"
  pip install --no-cache-dir "$@"
}

install_ultralytics_stack() {
  export PYTHONPATH="${ROOT}/app"
  if python3 -c "from ultralytics import YOLO" 2>/dev/null; then
    log "  ultralytics import OK"
    return 0
  fi
  log "  installing ultralytics + runtime deps…"
  pip install --no-cache-dir ultralytics==8.3.27 --no-deps
  pip install --no-cache-dir -r "${ROOT}/app/pose_api/requirements-ultralytics-runtime.txt"
  python3 -c "from ultralytics import YOLO" || {
    log "ERROR: ultralytics still fails to import after runtime deps"
    exit 1
  }
}

setup_deps() {
  setup_venv_if_needed() {
    if should_use_system_python; then return 0; fi
    if [[ ! -f venv/bin/activate ]]; then
      log "Creating venv…"
      python3 -m venv venv
    fi
    # shellcheck disable=SC1091
    source venv/bin/activate
  }

  setup_venv_if_needed
  activate_python || true

  if [[ "$SKIP_PIP" -eq 1 ]]; then
    if stack_ready 2>/dev/null; then
      log "Skipping pip (--start-only / --skip-pip)"
    else
      log "Not ready — running minimal bootstrap (missing packages only)…"
      bash "${ROOT}/scripts/bootstrap-pose-api.sh"
    fi
  elif stack_ready 2>/dev/null; then
    log "All imports OK — skipping pip entirely (0 downloads)"
  else
    log "Installing only missing packages (not full requirements.txt)…"
    pip_install_if_missing cv2 opencv-python-headless==4.11.0.86
    install_ultralytics_stack
    pip_install_if_missing fastapi fastapi==0.115.12
    pip_install_if_missing uvicorn uvicorn==0.34.2
    pip_install_if_missing httpx httpx==0.28.1
    pip_install_if_missing dotenv python-dotenv==1.0.1
    pip_install_if_missing yaml pyyaml
    pip_install_if_missing PIL Pillow
    stack_ready || {
      if should_use_system_python; then
        log "ERROR: use RunPod **PyTorch** template (not bare Ubuntu)."
        exit 1
      elif [[ "$SKIP_TORCH" -eq 0 ]]; then
        bash scripts/install-torch.sh
        install_ultralytics_stack
      fi
      stack_ready || exit 1
    }
  fi

  mkdir -p logs "${DATA_DIR:-$ROOT/data/pose_api}" \
    "${POSE_PIPELINE_OUTPUT_DIR:-$ROOT/app/yolo_model/artifacts/pose}"
  log "Ready — $(python3 -c 'import torch; print(f"torch {torch.__version__} cuda={torch.cuda.is_available()}")')"
}

stop_server() {
  if [[ -f "$PID_FILE" ]]; then
    pid="$(cat "$PID_FILE")"
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      sleep 1
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
  pkill -f "uvicorn pose_api.main:app" 2>/dev/null || true
}

start_server() {
  activate_python || { log "Run setup first"; exit 1; }
  load_env

  export PYTHONPATH="${PYTHONPATH:-$ROOT/app}"
  export YOLO_POSE_DEVICE="${YOLO_POSE_DEVICE:-cuda}"
  export YOLO_HIGHLIGHT_WEIGHTS="${YOLO_HIGHLIGHT_WEIGHTS:-$ROOT/app/yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt}"
  export DATA_DIR="${DATA_DIR:-$ROOT/data/pose_api}"
  export POSE_PIPELINE_OUTPUT_DIR="${POSE_PIPELINE_OUTPUT_DIR:-$ROOT/app/yolo_model/artifacts/pose}"
  export HOST="${HOST:-0.0.0.0}"

  [[ -f "$YOLO_HIGHLIGHT_WEIGHTS" ]] || { log "ERROR: weights missing: $YOLO_HIGHLIGHT_WEIGHTS"; exit 1; }

  stop_server
  mkdir -p "$(dirname "$LOG_FILE")"
  log "Starting on ${HOST}:${PORT}"
  nohup python3 -m uvicorn pose_api.main:app --host "$HOST" --port "$PORT" >>"$LOG_FILE" 2>&1 &
  echo $! >"$PID_FILE"
  sleep 2
  kill -0 "$(cat "$PID_FILE")" 2>/dev/null || { tail -20 "$LOG_FILE"; exit 1; }
  log "PID $(cat "$PID_FILE")"
}

print_status() {
  curl -sf "http://127.0.0.1:${PORT:-5060}/health" | python3 -m json.tool 2>/dev/null || log "not running"
}

[[ "$DO_STOP" -eq 1 ]] && { stop_server; exit 0; }
[[ "$DO_STATUS" -eq 1 ]] && { print_status; exit 0; }

if [[ "$DO_CHECK" -eq 1 ]]; then
  activate_python || true
  export PYTHONPATH="${ROOT}/app"
  if stack_ready; then
    log "READY — no pip needed. Start with: bash scripts/start-pose-api.sh"
    exit 0
  fi
  log "NOT READY — run once: bash scripts/bootstrap-pose-api.sh"
  exit 1
fi

# Default: if stack already works, never run pip (avoids slow opencv/torch re-downloads).
if [[ "$START_ONLY" -eq 0 ]] && [[ "$SETUP_ONLY" -eq 0 ]] && stack_ready 2>/dev/null; then
  log "Imports OK — skipping setup (0 pip). Use --setup-only to force install."
  SKIP_PIP=1
fi

if [[ "$START_ONLY" -eq 0 ]] && [[ "$SETUP_ONLY" -eq 1 || "$SKIP_PIP" -eq 0 ]]; then
  setup_deps
fi
[[ "$SETUP_ONLY" -eq 1 ]] && exit 0

start_server
print_status
