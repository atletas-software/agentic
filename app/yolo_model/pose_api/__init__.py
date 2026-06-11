"""HTTP client for remote GPU pose API (RunPod)."""

from yolo_model.pose_api.client import (
    pose_api_base_url,
    pose_api_configured,
    resolve_pose_data_for_video,
)

__all__ = [
    "pose_api_base_url",
    "pose_api_configured",
    "resolve_pose_data_for_video",
]
