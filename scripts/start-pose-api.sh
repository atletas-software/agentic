#!/usr/bin/env bash
# Start pose API on RunPod: installs ONLY missing packages (once per pod), then starts.
# Safe to run on every pod boot — skips pip when imports already work.
#
#   cd /workspace/agentic && bash scripts/start-pose-api.sh
#
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
exec bash scripts/run-pose-api.sh --use-system-python "$@"
