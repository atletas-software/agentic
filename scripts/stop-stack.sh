#!/usr/bin/env bash
# Stop API (:8000) and RQ worker for this repo (optional legacy feedback agent :5055).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

kill_port() {
  local port="$1"
  local pids=""
  if command -v lsof >/dev/null 2>&1; then
    pids="$(lsof -ti ":${port}" 2>/dev/null || true)"
  elif command -v fuser >/dev/null 2>&1; then
    fuser -k "${port}/tcp" 2>/dev/null && return 0
  fi
  if [[ -n "${pids}" ]]; then
    echo "Killing port ${port}: ${pids}"
    kill -TERM ${pids} 2>/dev/null || true
    sleep 1
    kill -KILL ${pids} 2>/dev/null || true
  else
    echo "Port ${port}: nothing listening (or use: fuser -k ${port}/tcp)"
  fi
}

echo "Stopping services..."
kill_port 8000
kill_port 5055

# RQ worker (python -m backendapi.workers.run_worker)
pkill -f "backendapi.workers.run_worker" 2>/dev/null && echo "Stopped RQ worker" || echo "No RQ worker process"

# Stray uvicorn for this project
pkill -f "uvicorn backendapi.main:app" 2>/dev/null || true
pkill -f "uvicorn agents.feedback.main:app" 2>/dev/null || true

sleep 1
echo "Done. Verify:"
if command -v lsof >/dev/null 2>&1; then
  lsof -i :8000 2>/dev/null || echo "  :8000 free"
  lsof -i :5055 2>/dev/null || echo "  :5055 free"
fi
