# Training guide

Concrete, reproducible steps to train a production-ready highlight overlay detector.

Most commands below assume your shell is in **`yolo_model/`** (same folder as `manifest.txt`). The training scripts automatically add the repository root to Python's path so `agents.feedback` imports work. You can also run them from the repo root with `python -m yolo_model.scripts.<script> ...` — see each script's docstring.

## 0. Prerequisites

```bash
# From repo root, in your training environment (Colab Pro A100 recommended).
pip install ultralytics torch
```

You also need the deployed agent's dependencies if you'll run the pipeline gate (`evaluate_pipeline.py`):

```bash
pip install -r agents/feedback/requirements.txt
```

System-side: `ffmpeg` must be on PATH. On macOS: `brew install ffmpeg`. On Linux: `apt-get install ffmpeg libgl1`.

## 1a. Import already-labeled positive images (optional)

If you have a folder of positive frames (e.g. Roboflow annotated exports under `positive-image/`):

```bash
cd yolo_model
.venv/bin/python -m scripts.import_dataset \
  --positives-dir /path/to/positive-image \
  --negatives-dir /path/to/negative-image \
  --version v1.0.0 \
  --val-ratio 0.2
```

This builds `dataset/v1.0.0/` (YOLO layout + `data.yaml`). Bootstrap labels use the legacy HSV detector unless you pass `--labels-dir` with a Roboflow YOLO export. Prefer replacing bootstrap labels with a real Roboflow **YOLOv8** export before production training.

## 1. Sample frames from raw videos

Collect 5–10 source videos covering every platform you support (Veo, Hudl, Trace, custom). Put them in `~/highlight_yolo/sources/` (anywhere works — that path is just a convention).

```bash
cd yolo_model   # if not already there

python -m scripts.prepare_dataset \
  --videos-dir ~/highlight_yolo/sources \
  --output ./dataset/raw \
  --interval-sec 0.5 \
  --frame-width 640
```

Output:

- `~/highlight_yolo/dataset/raw/<video_id>/frame_<ts*100>.jpg`
- `~/highlight_yolo/dataset/raw/<video_id>/suggestions.csv` — the legacy HSV detector's guesses, **for labeling speed only**, never as ground truth.

For "hard negative" videos (no overlay anywhere — used to teach the model what's NOT an overlay), point a separate run at:

```bash
python -m scripts.prepare_dataset \
  --negatives-dir ~/highlight_yolo/sources_negatives \
  --output ./dataset/raw \
  --interval-sec 1.0 \
  --no-suggest
```

## 2. Label

Use [Roboflow](https://roboflow.com) (recommended) or [CVAT](https://github.com/opencv/cvat).

Single class: **`highlight_overlay`**. Labeling rules (from `DATA_STRATEGY.md`):

- Box the overlay shape itself (the red ring, the Veo pointer), not the player.
- Tight boxes — about 2 px padding around the ring's outer edge.
- Box partially-occluded overlays as long as ≥ 60% of the ring is visible.
- Drop overlays smaller than 12 px on the long edge.
- Drop frames where you cannot decide with confidence.

Export as **YOLOv8** format. Unzip into a versioned folder:

```
~/highlight_yolo/dataset/v1.0.0/
  data.yaml
  images/train/*.jpg
  images/val/*.jpg
  labels/train/*.txt
  labels/val/*.txt
```

Verify `data.yaml` looks like this:

```yaml
path: /Users/you/highlight_yolo/dataset/v1.0.0
train: images/train
val: images/val
names:
  0: highlight_overlay
```

## 3. Train

The hyperparameters baked into `scripts/train.py` are the production recipe. Override only if you have a reason.

```bash
python -m scripts.train \
  --data ~/highlight_yolo/dataset/v1.0.0/data.yaml \
  --weights yolov8n.pt \
  --epochs 80 \
  --imgsz 640 \
  --batch 16 \
  --device 0 \
  --project ~/highlight_yolo/runs \
  --name v1.0.0
```

Notes:

- `--device 0` = first GPU (Colab). Use `--device cpu` only for smoke tests; do not train production weights on CPU.
- `--device mps` works on Apple Silicon but the MPS backend has subtle numerical differences vs CUDA in some Ultralytics ops. Acceptable for prototyping, not for the final production checkpoint.
- Early stopping kicks in after 20 epochs without improvement (configurable via `--patience`).
- Best weights land at `~/highlight_yolo/runs/v1.0.0/weights/best.pt`.

Expected wall time on Colab Pro A100 for ~500 images: about 10 minutes.

## 4. Gate G1 — model-level metrics

```bash
python -m scripts.evaluate \
  --weights ~/highlight_yolo/runs/v1.0.0/weights/best.pt \
  --data ~/highlight_yolo/dataset/v1.0.0/data.yaml \
  --report ~/highlight_yolo/runs/v1.0.0/val_report.json
```

Pass criteria for v1 launch:

- precision ≥ 0.85
- recall ≥ 0.90
- mAP@0.5 ≥ 0.85

If recall is below target → label more **positives**, especially on the platforms that miss.
If precision is below target → add more **hard negatives** (red jerseys, ads, sunset frames).
Do not advance until G1 passes.

## 5. Stage the weights

```bash
mkdir -p yolo_model/weights
cp ~/highlight_yolo/runs/v1.0.0/weights/best.pt \
   yolo_model/weights/highlight_yolo_v1.0.0.pt
```

Copy the same file to the runtime location the deployed agent reads from:

```bash
mkdir -p agents/feedback/models
cp yolo_model/weights/highlight_yolo_v1.0.0.pt \
   agents/feedback/models/highlight_yolo_v1.pt
```

Then write a model card next to the weights — template lives in `PRODUCTION_PLAYBOOK.md` (section "Model card").

## 6. Gate G2 — pipeline-level metrics on real videos

Build a fixture manifest at `yolo_model/fixtures/<name>.json` (see `yolo_model/fixtures/README.md` for the schema) with 3–10 short videos where you have manually annotated the true event spans.

```bash
python -m scripts.evaluate_pipeline \
  --fixtures fixtures/launch_v1.json \
  --output ~/highlight_yolo/runs/v1.0.0/pipeline_eval \
  --report ~/highlight_yolo/runs/v1.0.0/pipeline_report.json
```

Pass criteria:

- event recall ≥ 0.90 across all fixtures
- false-positive event rate ≤ 0.05

`evaluate_pipeline.py` exits with code 0 if both pass, 1 otherwise. Wire that into CI when you're ready.

## 7. Gate G3 — shadow deploy

Run the new YOLO model alongside the legacy HSV detector for one week (see `PRODUCTION_PLAYBOOK.md` "Shadow deployment" for how). If diffs are within tolerance, flip `VIDEO_HIGHLIGHT_DETECTOR=yolo` to default in `app/.env.example` and ship.

## 8. Save everything

Before walking away from a run, ensure these artifacts are committed or uploaded to your model registry:

- `yolo_model/weights/highlight_yolo_v<ver>.pt` (binary — git-ignored; upload to S3 / model registry)
- `yolo_model/weights/highlight_yolo_v<ver>.card.md` (model card — commit to git)
- `yolo_model/fixtures/launch_v<ver>.json` (fixture manifest — commit to git)
- `~/highlight_yolo/runs/v<ver>/val_report.json` and `pipeline_report.json` (metrics — upload to registry)
- W&B / TensorBoard run URL (linked in the model card)

The runtime weights symlink:

```bash
ln -sf highlight_yolo_v1.0.0.pt agents/feedback/models/highlight_yolo_v1.pt
```

(This is what `VIDEO_HIGHLIGHT_YOLO_WEIGHTS` resolves to by default. Rolling back is a one-line symlink change — see `PRODUCTION_PLAYBOOK.md`.)

## Common pitfalls

| Symptom | Likely cause | Fix |
|---|---|---|
| Loss diverges in first epoch | imgsz too small or `lr0` too high for a tiny dataset | Keep imgsz at 640; halve `lr0`. |
| High train mAP, low val mAP | Train / val frames from the same videos (memorization) | Re-split: val frames must come from videos NOT in train. |
| Model misses one platform entirely | Platform under-represented in dataset | Sample 50–100 more frames of that platform; retrain. |
| Many false positives on red jerseys | Hard negatives under-represented | Add 100+ jersey-heavy negative frames; retrain. |
| Recall stuck at 0.90 plateau | Dataset too small | Scale data before changing architecture. Push to 1500+ labeled frames before considering yolov8s. |
