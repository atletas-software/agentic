FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/app

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg tesseract-ocr libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY app/backendapi/requirements.txt /app/app/backendapi/requirements.txt
RUN pip install --no-cache-dir -r /app/app/backendapi/requirements.txt

COPY app/backendapi/requirements-yolo.txt /app/app/backendapi/requirements-yolo.txt
RUN pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cpu \
        "torch==2.4.1" "torchvision==0.19.1" \
    && pip install --no-cache-dir -r /app/app/backendapi/requirements-yolo.txt \
    && pip install --no-cache-dir ultralytics==8.3.27 --no-deps

COPY app/backendapi /app/app/backendapi
COPY app/agents /app/app/agents
COPY app/yolo_model /app/app/yolo_model

# Runtime default when bind-mount is not used (file may be replaced by compose volume).
RUN mkdir -p /app/app/agents/feedback/models

ENV YOLO_HIGHLIGHT_WEIGHTS=/app/app/agents/feedback/models/highlight_yolo_v1.pt \
    YOLO_POSE_DEVICE=cpu \
    YOLO_POSE_WEIGHTS=yolov8n-pose.pt \
    VIDEO_HIGHLIGHT_DETECTOR=yolo \
    POSE_PIPELINE_OUTPUT_DIR=/app/data/pose

RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /app/data/pose \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

CMD ["uvicorn", "backendapi.main:app", "--host", "0.0.0.0", "--port", "8000"]
