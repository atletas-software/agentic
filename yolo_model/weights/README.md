# yolo_model/weights — training-output staging

This folder is the **staging area for trained YOLO weights** during experimentation. It is *not* where the deployed agent reads weights from at runtime.

## The two locations, and why

| Location | Purpose | Committed to git? | Shipped in Docker? |
|---|---|---|---|
| `yolo_model/weights/highlight_yolo_v<ver>.pt` | Staging during experimentation; one file per trained version. | No (`.gitignore`) | No |
| `agents/feedback/models/highlight_yolo_v1.pt` | What the deployed agent loads at runtime (env: `VIDEO_HIGHLIGHT_YOLO_WEIGHTS`). | No (`.gitignore`) | Yes (the Dockerfile copies `agents/`) |

Keeping the two separate means you can train, evaluate, and version many candidates here without altering production, then promote one explicitly:

```bash
# Promote v1.0.0 to runtime:
cp yolo_model/weights/highlight_yolo_v1.0.0.pt \
   agents/feedback/models/highlight_yolo_v1.pt
```

A symlink also works (preferred for rollback agility):

```bash
ln -sf highlight_yolo_v1.0.0.pt agents/feedback/models/highlight_yolo_v1.pt
```

## Always commit the model card

While the `.pt` binary is git-ignored, the matching **model card** must be committed:

```
yolo_model/weights/
  highlight_yolo_v1.0.0.pt        # binary — git-ignored
  highlight_yolo_v1.0.0.card.md   # model card — commit
```

Template for the card lives in `yolo_model/PRODUCTION_PLAYBOOK.md` (section "Model card"). The card captures dataset version, training hyperparameters, val/pipeline metrics, known failure modes, and the W&B run URL — everything you need to reproduce or audit the model later.

## Rollback

Keep at least the previous two versions of the `.pt` here (and on the deploy host or model registry). To roll back a misbehaving production model:

```bash
ln -sf highlight_yolo_v0.9.0.pt agents/feedback/models/highlight_yolo_v1.pt
# Restart the feedback agent. No image rebuild needed.
```

See `PRODUCTION_PLAYBOOK.md` section "Rollback" for the full procedure.
