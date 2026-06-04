#!/usr/bin/env bash
# Verify repo layout, PYTHONPATH imports, and optional dependency install.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RED=0
warn() { echo "[verify] WARN: $*"; }
fail() { echo "[verify] FAIL: $*"; RED=1; }
ok() { echo "[verify] OK: $*"; }

echo "=== Layout ==="
for d in app/backendapi app/agents app/yolo_model; do
  if [[ -d "$d" ]]; then ok "directory $d"; else fail "missing $d"; fi
done

for stale in app/main.py agents/feedback yolo_model/pipeline app/.env; do
  if [[ -e "$stale" ]]; then fail "stale path still exists: $stale"; fi
done

echo ""
echo "=== Stale import references ==="
if rg -q 'from app\.|import app\.|uvicorn app\.main|python -m app\.workers' \
  --glob '*.py' --glob '*.sh' --glob 'Makefile' --glob 'docker-compose.yml' 2>/dev/null; then
  fail "found old 'app.' Python module references (platform is now backendapi)"
  rg 'from app\.|import app\.|uvicorn app\.main|python -m app\.workers' \
    --glob '*.py' --glob '*.sh' --glob 'Makefile' --glob 'docker-compose.yml' || true
else
  ok "no stale app.* module references in code/config"
fi

echo ""
echo "=== PYTHONPATH import smoke test ==="
export PYTHONPATH="${ROOT}/app"
python3 <<'PY'
import importlib
import sys

# Pure-Python modules (no heavy deps required)
light = [
    "yolo_model.config.paths",
    "yolo_model.pose_feedback.engine",
    "yolo_model.pipeline.runner",
]

heavy = [
    "backendapi.core.env_loader",
    "backendapi.services.orchestrator",
    "backendapi.workers.run_worker",
    "agents.feedback.storage",
    "agents.feedback.review_agent",
    "agents.feedback.highlight.pipeline",
    "backendapi.main",
    "agents.feedback.main",
]

failed = []
for name in light:
    try:
        importlib.import_module(name)
        print("OK (light)", name)
    except Exception as exc:
        print("FAIL (light)", name, exc)
        failed.append(name)

missing_deps = []
OPTIONAL_PREFIXES = (
    "fastapi", "sqlalchemy", "rq", "dotenv", "pydantic", "cv2", "torch", "ultralytics",
    "openai", "redis", "psycopg", "httpx", "PIL", "jinja2", "google", "google.cloud.firestore",
    "passlib", "tiktoken", "cryptography", "ffmpeg",
)

for name in heavy:
    try:
        importlib.import_module(name)
        print("OK (heavy)", name)
    except ModuleNotFoundError as exc:
        mod = getattr(exc, "name", "") or str(exc)
        if any(x in mod for x in OPTIONAL_PREFIXES):
            missing_deps.append((name, mod))
            print("SKIP (deps)", name, "->", exc)
        else:
            print("FAIL (heavy)", name, exc)
            failed.append(name)
    except ImportError as exc:
        mod = str(exc)
        if any(x in mod for x in OPTIONAL_PREFIXES):
            missing_deps.append((name, mod))
            print("SKIP (deps)", name, "->", exc)
        else:
            print("FAIL (heavy)", name, exc)
            failed.append(name)
    except Exception as exc:
        print("FAIL (heavy)", name, exc)
        failed.append(name)

if missing_deps:
    print("\nNote: install deps with: pip install -r requirements.txt (Python 3.11–3.12 recommended)")
if failed:
    sys.exit(1)
PY

IMPORT_RC=$?
if [[ "$IMPORT_RC" -ne 0 ]]; then fail "import smoke test failed"; fi

echo ""
echo "=== Syntax compile ==="
python3 -m compileall -q app/backendapi app/agents app/yolo_model
ok "compileall passed"

echo ""
if [[ "$RED" -eq 0 ]]; then
  echo "=== All structural checks passed ==="
else
  echo "=== Some checks failed ==="
  exit 1
fi
