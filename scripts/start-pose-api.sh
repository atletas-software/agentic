#!/usr/bin/env bash
# Start pose API only — ZERO pip. Use this on RunPod after first successful /health.
#
#   cd /workspace/agentic && bash scripts/start-pose-api.sh
#
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
exec bash scripts/run-pose-api.sh --start-only "$@"
