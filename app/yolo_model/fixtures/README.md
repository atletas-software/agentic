# End-to-end pipeline fixtures

This folder holds **annotated test clips** that gate promoting the YOLO detector to the default (gate G2 in `docs/production.md`). The companion script `yolo_model/scripts/evaluate_pipeline.py` reads a JSON manifest from here and compares the pipeline's detected events to your hand-labeled ground truth.

## Recommended composition

Aim for 5–10 short videos *per platform*:

- Veo (red ring with arrow notches)
- Hudl (their marker style)
- Trace (athlete-focus pointer)
- Custom uploads (assorted red rings + hard negatives)

Each clip should be **30–120 seconds** with 1–3 highlight events. Mix in a couple of clips with zero events (red jerseys, advertisements, sunset matches) to measure false-positive rate.

## Manifest format

```json
{
  "fixtures": [
    {
      "name": "veo_match_abc_clip01",
      "video": "https://example.com/veo_clip01.mp4",
      "events": [
        {"t_on": 4.20, "t_off": 6.10},
        {"t_on": 18.50, "t_off": 21.30}
      ]
    },
    {
      "name": "negative_red_jerseys",
      "video": "./samples/negatives/red_jerseys_01.mp4",
      "events": []
    }
  ]
}
```

- `video` can be a local path or HTTPS URL.
- `events` is a list of ground-truth visibility spans (seconds).

## Metrics reported

For every fixture:

- `precision`, `recall`, `f1` at the event level (one prediction matches one ground-truth event if their time spans overlap with temporal IoU ≥ 0.5).
- `mean_iou_temporal` — how tightly predicted spans match ground truth.
- Per-event timing (probe extract + YOLO inference + asset build).

Aggregate gate G2 (from `docs/production.md`): promote `VIDEO_HIGHLIGHT_DETECTOR=yolo` to default only once the fixture suite shows **recall ≥ 0.90** and **false-positive-event rate ≤ 0.05**.

## Versioning

Commit one manifest per major dataset version (e.g. `launch_v1.json`, `launch_v1.1.json`). Each manifest is paired with a frozen dataset, so adding/removing fixtures means a new file, not editing in place.
