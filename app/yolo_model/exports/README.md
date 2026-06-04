# exports/

Stage promoted weights before copying to `agents/feedback/models/highlight_yolo_v1.pt`.

```bash
cp artifacts/train/highlight_v1.1.0/weights/best.pt exports/highlight_yolo_v1.1.0.pt
cp exports/highlight_yolo_v1.1.0.pt agents/feedback/models/highlight_yolo_v1.pt
```

Binary weights are git-ignored (see `.gitignore` in this folder).
