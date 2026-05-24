# dataset v1.0.0

- Created: auto-import via `scripts/import_dataset.py`
- Positives source: `/Users/mac/Documents/projects/yuri/positive-image`
- Negatives source: `none`
- Train: 22 positive, 0 negative images
- Val: 5 positive, 0 negative images
- Labels: HSV bootstrap
- Bootstrap misses (no box written): 1
- Class: `highlight_overlay` (single class)
- Label rules: see `DATA_STRATEGY.md`

## Notes

- Replace bootstrap labels with a Roboflow **YOLOv8** export when available.
- Val images are a random split; for production, split by **video**, not frame.
