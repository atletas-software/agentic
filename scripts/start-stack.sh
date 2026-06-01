#!/usr/bin/env bash
# Start Redis (if installed), API :8000, RQ worker, feedback agent :5055.
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

export PYTHONPATH="${ROOT}"

if ! python -c "import torch; torch._C._dlpack_exchange_api()" 2>/dev/null; then
  echo "PyTorch missing or broken. Run: bash scripts/install-torch.sh"
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
nohup uvicorn app.main:app --host 0.0.0.0 --port 8000 > /tmp/agentic-api.log 2>&1 &
echo "  PID $!  log: /tmp/agentic-api.log"

sleep 1

echo "Starting RQ worker..."
nohup python -m app.workers.run_worker > /tmp/agentic-worker.log 2>&1 &
echo "  PID $!  log: /tmp/agentic-worker.log"

sleep 1

echo "Starting feedback agent on :5055..."
nohup uvicorn agents.feedback.main:app --host 0.0.0.0 --port 5055 > /tmp/agentic-feedback.log 2>&1 &
echo "  PID $!  log: /tmp/agentic-feedback.log"

sleep 2
echo ""
echo "Health:"
curl -sf http://127.0.0.1:8000/health && echo "  API OK" || echo "  API not ready"
curl -sf -o /dev/null http://127.0.0.1:5055/ && echo "  Feedback agent OK" || echo "  Feedback agent not ready"
echo ""
echo "Logs: tail -f /tmp/agentic-api.log /tmp/agentic-worker.log /tmp/agentic-feedback.log"
