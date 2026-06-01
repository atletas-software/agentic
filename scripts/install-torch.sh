#!/usr/bin/env bash
# Reinstall matching torch + torchvision (fixes broken/mixed PyPI + CUDA installs).
# Run from repo root with your venv activated:
#   bash scripts/install-torch.sh
set -euo pipefail

TORCH_VERSION="${TORCH_VERSION:-2.4.1}"
TV_VERSION="${TV_VERSION:-0.19.1}"

if command -v nvidia-smi >/dev/null 2>&1; then
  INDEX="${TORCH_INDEX_URL:-https://download.pytorch.org/whl/cu124}"
  echo "GPU detected — installing torch ${TORCH_VERSION} + torchvision ${TV_VERSION} from ${INDEX}"
else
  INDEX="${TORCH_INDEX_URL:-https://download.pytorch.org/whl/cpu}"
  echo "No GPU — installing CPU torch ${TORCH_VERSION} + torchvision ${TV_VERSION} from ${INDEX}"
fi

pip uninstall -y torch torchvision torchaudio 2>/dev/null || true
pip install --no-cache-dir "torch==${TORCH_VERSION}" "torchvision==${TV_VERSION}" --index-url "${INDEX}"

python - <<'PY'
import torch
import torchvision

print("torch:", torch.__version__)
print("torchvision:", torchvision.__version__)
print("cuda available:", torch.cuda.is_available())
# Smoke test that broke in the user's error:
_ = torch._C._dlpack_exchange_api()
print("torch._C OK")
PY

echo "PyTorch install verified."
