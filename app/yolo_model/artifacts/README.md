# artifacts/

Generated outputs (git-ignored except this file).

| Path | Purpose |
|------|---------|
| `train/highlight_v1.1.0/` | Latest highlight-detector training run (`weights/best.pt`, `args.yaml`, `val_report.json`) |
| `pose/` | Runtime pose JSON: `job_<id>/pose_results.json` |

Training writes here when `--project` points at `artifacts/` (see `scripts/train.py`).
