# GPU pose API for RunPod. Build from repo root:
#   docker build -f app/pose_api/Dockerfile -t athlete-pose-api .
#
# RunPod: expose port 5060, set YOLO_POSE_DEVICE=cuda, mount DATA_DIR volume.
FROM pytorch/pytorch:2.4.1-cuda12.4-cudnn9-runtime

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
        fastapi==0.115.12 "uvicorn[standard]==0.34.2" httpx==0.28.1 python-dotenv==1.0.1 \
        pydantic==2.11.4 Pillow==11.2.1 opencv-python-headless==4.11.0.86 \
        "pyyaml>=6.0" matplotlib scipy pandas psutil py-cpuinfo requests tqdm ultralytics-thop \
    && pip install --no-cache-dir ultralytics==8.3.27 --no-deps

COPY app/pose_api /app/pose_api
COPY app/yolo_model /app/yolo_model
RUN mkdir -p /app/data/pose_api \
    && test -f /app/yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt

ENV YOLO_HIGHLIGHT_WEIGHTS=/app/yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt
ENV YOLO_POSE_DEVICE=cuda
ENV YOLO_POSE_WEIGHTS=yolov8n-pose.pt
ENV POSE_PIPELINE_OUTPUT_DIR=/app/data/pose
ENV DATA_DIR=/app/data/pose_api
ENV HOST=0.0.0.0
ENV PORT=5060

EXPOSE 5060

CMD ["sh", "-c", "uvicorn pose_api.main:app --host 0.0.0.0 --port ${PORT:-5060}"]
