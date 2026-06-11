#!/usr/bin/env bash
# One-time minimal install on a new RunPod pod (~3–5 min; no torch download).
# Run once when: bash scripts/run-pose-api.sh --check  shows missing packages.
#
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "[bootstrap] Installing small API packages…"
pip install --no-cache-dir \
  fastapi==0.115.12 \
  uvicorn==0.34.2 \
  httpx==0.28.1 \
  python-dotenv==1.0.1

echo "[bootstrap] Installing opencv-headless (largest download ~20MB)…"
pip install --no-cache-dir opencv-python-headless==4.11.0.86

echo "[bootstrap] Installing ultralytics without heavy deps…"
pip install --no-cache-dir ultralytics==8.3.27 --no-deps

export PYTHONPATH="${ROOT}/app"
python3 - <<'PY'
import torch, cv2, fastapi, uvicorn, httpx
from ultralytics import YOLO
print("OK torch", torch.__version__, "cuda", torch.cuda.is_available())
PY

echo "[bootstrap] Done. Start with: bash scripts/start-pose-api.sh"
