# Dataset layout

| Path | Purpose |
|------|---------|
| `sources/positives/` | Staging copy of your positive frames (from Roboflow / manual export) |
| `sources/negatives/` | Hard-negative frames (no overlay) — optional |
| `raw/` | Frames sampled from videos via `scripts/prepare_dataset.py` (pre-labeling) |
| `v1.0.0/` | Frozen YOLOv8 training set (images + labels + `data.yaml`) |

## Import positives from outside the repo

```bash
cd yolo_model
python -m scripts.import_dataset \
  --positives-dir /path/to/positive-image \
  --negatives-dir /path/to/negative-image \
  --version v1.0.0
```

Then train:

```bash
python -m scripts.train --data dataset/v1.0.0/data.yaml --device 0
```

See `TRAINING_GUIDE.md` for the full pipeline.
