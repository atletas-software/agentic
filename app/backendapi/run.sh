#!/usr/bin/env bash
# Run the platform API from app/backendapi/ (sets PYTHONPATH to parent app/).
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$DIR/../.." && pwd)"
export PYTHONPATH="$(cd "$DIR/.." && pwd)"
export ENV_FILE="$DIR/.env"

if [[ -f "$DIR/venv/bin/activate" ]]; then
  # shellcheck source=/dev/null
  source "$DIR/venv/bin/activate"
elif [[ -f "$REPO_ROOT/.venv-app/bin/activate" ]]; then
  # shellcheck source=/dev/null
  source "$REPO_ROOT/.venv-app/bin/activate"
fi

exec uvicorn backendapi.main:app --reload --host 0.0.0.0 --port 8000 "$@"
