# datasets/

| Path | Purpose |
|------|---------|
| `athlete_focus/v1.1.0/` | Production Roboflow YOLOv8 export (`data.yaml`, train/valid/test) |
| `sources/positives`, `sources/negatives` | Raw labeled frames for `scripts/import_dataset.py` |
| `raw/` | Frame samples from `scripts/prepare_dataset.py` |

Train with:

```bash
python -m yolo_model.scripts.train --data yolo_model/datasets/athlete_focus/v1.1.0/data.yaml
```
