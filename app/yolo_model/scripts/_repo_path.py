"""Ensure the repository root is on ``sys.path`` for ``agents.*`` imports.

Training scripts live under ``yolo_model/scripts/`` but reuse ffmpeg helpers from
``agents.feedback.video_utils``. When you run::

    cd yolo_model
    python -m scripts.prepare_dataset ...

Python's working directory is ``yolo_model/``, so ``agents`` is not importable
unless we add the parent ``app/`` directory (contains ``agents/`` and ``yolo_model/``)
"""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_repo_root_on_path() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "agents" / "feedback").is_dir():
            root_s = str(parent)
            if root_s not in sys.path:
                sys.path.insert(0, root_s)
            return parent
    raise RuntimeError(
        "Could not find app bundle (expected a parent of this file to contain "
        "agents/feedback/). Set PYTHONPATH to the repo's app/ directory."
    )
