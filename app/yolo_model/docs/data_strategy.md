# Data strategy

The single biggest production lever for this model is **dataset composition**. Architecture is interchangeable; data is not. This doc captures what to collect, how to label, and how to version the dataset across releases.

## Target dataset composition (v1)

Build the dataset as an explicit grid, not opportunistically.

| Source / condition | Positive frames | Negative frames |
|---|---|---|
| Veo (red ring with notches) | 120 | 30 |
| Hudl (marker) | 80 | 20 |
| Trace (white vertical pointer) | 80 | 20 |
| Custom uploads (red ring no notches) | 80 | 30 |
| Indoor / artificial light | 40 | 20 |
| Sunset / golden hour | 40 | 30 |
| Rain / fog / snow | 30 | 20 |
| Low-res / heavy compression | 30 | 30 |
| **Subtotal** | **500** | **200** |

Negative ratio target: 25–30% of the total dataset.

## Hard negatives are mandatory

The legacy HSV detector fails on these. Your YOLO model **must** see them with no labels during training, otherwise it will repeat the same false positives:

- Red kits (whole teams in red)
- Red sideline ads, banners, sponsor boards
- Red corner flags, red water bottles
- Red scoreboard graphics, red text overlays
- Sunset frames (everything tinted red)
- Red stadium seats, fans wearing red

Roboflow lets you upload these as "null images" (no bounding boxes). They contribute to the classification loss but never the box-regression loss — exactly what we want for negatives.

## Label quality rules

Consistency between labelers matters more than absolute correctness:

1. Box the **overlay shape**, not the player it surrounds. The model is learning the overlay, not the athlete.
2. Tight boxes — about 2 px padding around the ring's outer edge.
3. Box partially-occluded overlays as long as ≥ 60% of the ring is visible.
4. Drop overlays smaller than 12 px on the long edge — they are too noisy.
5. If two overlays appear in the same frame (rare but happens), label both.
6. Drop frames where you cannot decide with confidence. Ambiguity poisons training.

## Train / val / test split

- **Train: 70%**
- **Val: 20%** — used by Ultralytics during training (early-stopping signal)
- **Test (held-out): 10%** — never shown during training, used only for the launch gate

**Critical rule**: frames in val and test must come from **videos that are not in train**, not just different frames of the same video. Frame-level random splits leak across the boundary (consecutive frames are near-identical) and inflate metrics.

Suggested workflow in Roboflow:

1. Group your source videos into train / val / test partitions before any labeling.
2. Upload them as separate "batches" so Roboflow respects the grouping in its split.
3. Verify by checking `images/val/*.jpg` filenames map to your val-partition videos.

## Dataset versioning

Treat datasets like code — frozen snapshots, never edited in place.

```
~/highlight_yolo/dataset/
  v1.0.0/      # frozen, never edit
    data.yaml
    images/{train,val}/*.jpg
    labels/{train,val}/*.txt
    DATASET_CARD.md
  v1.1.0/      # added 200 hard negatives
    ...
  v2.0.0/      # added Pixellot platform support
    ...
```

Each dataset version maps to a model version. **Never train across versioned datasets without bumping the model version.** Reproducibility depends on this.

### Dataset card template

Drop one of these into every versioned dataset folder:

```markdown
# dataset v1.0.0

- Created: 2026-MM-DD
- Total frames: 700 (500 positive + 200 negative)
- Source videos:
  - veo_match_01.mp4 (train) — 80 positives, 20 negatives
  - veo_match_02.mp4 (val) — 40 positives, 10 negatives
  - hudl_clip_01.mp4 (train) — 80 positives, 20 negatives
  - trace_clip_01.mp4 (test, held-out) — 30 positives, 10 negatives
  - ...
- Label rules version: 1.0 (see docs/data_strategy.md)
- Labeled by: <name(s)>
- Labeling QA: spot-checked 50 random frames, 2 corrections
- Known issues: Trace platform under-represented (only one clip)
```

## Growing the dataset

The data flywheel (described in `docs/production.md` section "Continuous improvement") drives v1.1+:

- Every month, sample ~200 frames where the production model was uncertain (0.30 ≤ conf ≤ 0.60) and label them.
- Add the new frames to a new dataset version (never overwrite a frozen version).
- Retrain → publish new model patch version.

Steady-state target: 3000–5000 labeled frames by month 6. After that, returns diminish; focus on hard negatives instead of raw count.

## When to add a new class

Stay single-class (`highlight_overlay`) unless one of these is true:

1. You need to distinguish the overlay style downstream (e.g. different coaching prompts for Veo vs Hudl). In that case split into `veo_ring`, `hudl_marker`, etc.
2. The model is mis-classifying one platform as another (rare with one class, but possible if styles overlap).

Splitting classes requires at least 200 labels **per class** and a model version bump (major version).

## Things explicitly NOT in the dataset

The model learns overlays, nothing else. Do **not** label:

- The player inside the ring (separate problem — player ID is out of scope).
- The ball.
- Scoreboard text.
- Audio events.

If you need any of these downstream, build a separate model. Mixing concerns into one detector hurts every concern.
