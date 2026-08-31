#!/usr/bin/env bash
# Start Redis (if installed), API :8000, and RQ worker. Review UI is served from the API.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -f venv/bin/activate ]]; then
  # shellcheck disable=SC1091
  source venv/bin/activate
elif [[ -f .venv/bin/activate ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

# Load platform + agent secrets (OPENAI_API_KEY, YOLO_*, feedback URLs).
if [[ -f app/backendapi/.env ]]; then
  set -a
  # shellcheck disable=SC1091
  source app/backendapi/.env
  set +a
fi

export PYTHONPATH="${ROOT}/app"
export FEEDBACK_USE_POSE_PIPELINE="${FEEDBACK_USE_POSE_PIPELINE:-false}"
if [[ -n "${FEEDBACK_PUBLIC_BASE_URL:-}" ]]; then
  export PUBLIC_BASE_URL="${FEEDBACK_PUBLIC_BASE_URL}"
fi
if [[ -z "${YOLO_POSE_DEVICE:-}" ]]; then
  if command -v nvidia-smi >/dev/null 2>&1; then
    export YOLO_POSE_DEVICE=cuda
  else
    export YOLO_POSE_DEVICE=cpu
  fi
fi

echo "Checking ffmpeg..."
if ! command -v ffmpeg >/dev/null 2>&1; then
  echo "WARNING: ffmpeg not found — video coaching will fail. Run: bash scripts/run.sh (installs on Linux as root) or apt-get install ffmpeg"
else
  ffmpeg -version | head -1
fi

echo "Checking PyTorch + ultralytics..."
if ! python - <<'PY'
import torch
from ultralytics import YOLO

dev = "cuda" if torch.cuda.is_available() else "cpu"
x = torch.zeros(2, 2, device=dev)
print("torch", torch.__version__, "device", x.device, "ultralytics OK")
PY
then
  echo "PyTorch/ultralytics check failed. Run: bash scripts/install-torch.sh"
  exit 1
fi

if command -v redis-cli >/dev/null 2>&1; then
  if ! redis-cli ping >/dev/null 2>&1; then
    echo "Starting redis-server..."
    redis-server --daemonize yes 2>/dev/null || true
    sleep 1
  fi
  redis-cli ping || echo "WARNING: Redis not responding — worker will fail"
else
  echo "WARNING: redis-cli not found — install redis-server or start Redis manually"
fi

echo "Starting API on :8000..."
nohup uvicorn backendapi.main:app --host 0.0.0.0 --port 8000 > /tmp/agentic-api.log 2>&1 &
echo "  PID $!  log: /tmp/agentic-api.log"

sleep 1

echo "Starting RQ worker..."
nohup python -m backendapi.workers.run_worker > /tmp/agentic-worker.log 2>&1 &
echo "  PID $!  log: /tmp/agentic-worker.log"

sleep 2
echo ""
echo "Health:"
curl -sf http://127.0.0.1:8000/health && echo "  API OK" || echo "  API not ready"
curl -sf -o /dev/null http://127.0.0.1:8000/feedback-agent/health 2>/dev/null && echo "  Feedback routes OK" || true
echo ""
echo "Review pages: FRONTEND_BASE_URL/review/{id} (proxied to API :8000)"
echo "YOLO_POSE_DEVICE=${YOLO_POSE_DEVICE:-not set}"
echo "YOLO_HIGHLIGHT_WEIGHTS=${YOLO_HIGHLIGHT_WEIGHTS:-not set}"
echo "FEEDBACK_PUBLIC_BASE_URL=${FEEDBACK_PUBLIC_BASE_URL:-not set}"
echo "Logs: tail -f /tmp/agentic-api.log /tmp/agentic-worker.log"
