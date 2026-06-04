"""CLI: pose JSON → per-highlight feedback (rule-based or via feedback agent).

Rule-based only:

python -m yolo_model.scripts.pose_feedback \\
    --pose-json results_yolov8.json \\
    --output yolo_model/artifacts/feedback/results.json

Full agent review (needs video + OPENAI_API_KEY, writes under agents/feedback/data):

PYTHONPATH=app python -m yolo_model.scripts.pose_feedback \\
    --pose-json results_yolov8.json \\
    --use-agent \\
    --video-url /path/to/match.mp4 \\
    --review-id my_pose_review \\
    --player-focus "Player #14"
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from pathlib import Path

LOG = logging.getLogger("highlight.pose_feedback")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--pose-json", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--kb", type=Path, default=None, help="Posture KB YAML (default: yolo_model/config/posture_guidelines.yaml)")
    parser.add_argument("--use-agent", action="store_true", help="Run agents.feedback review agent (vision per highlight)")
    parser.add_argument("--video-url", default="", help="Required with --use-agent")
    parser.add_argument("--review-id", default="", help="Review id (generated if omitted)")
    parser.add_argument("--sport", default="Soccer")
    parser.add_argument("--player-focus", default="highlighted player")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not args.pose_json.is_file():
        LOG.error("Pose JSON not found: %s", args.pose_json)
        return 2

    from yolo_model.config.paths import posture_kb_path

    kb = args.kb or posture_kb_path()

    if args.use_agent:
        from agents.feedback.review_agent import build_review_from_pose_json
        from yolo_model.pose_feedback.engine import load_pose_json

        review_id = args.review_id or uuid.uuid4().hex[:16]
        if not args.video_url:
            print("error: --video-url required with --use-agent", file=sys.stderr)
            return 2
        review = build_review_from_pose_json(
            review_id=review_id,
            video_url=args.video_url,
            pose_data=load_pose_json(args.pose_json),
            sport=args.sport,
            player_focus=args.player_focus,
            kb_path=kb,
        )
        payload = {"review_id": review_id, "review": review}
    else:
        from yolo_model.pose_feedback import generate_feedback_payload, load_posture_kb
        from yolo_model.pose_feedback.engine import load_pose_json

        if not kb.is_file():
            LOG.error("KB not found: %s", kb)
            return 2
        pose_data = load_pose_json(args.pose_json)
        payload = generate_feedback_payload(
            pose_data,
            load_posture_kb(kb),
            pose_json_path=str(args.pose_json),
            kb_path=str(kb),
        )
        payload["used_agent"] = False

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    LOG.info("Wrote %s", args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
