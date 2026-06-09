# Build from repository root: docker build -f app/agents/feedback/Dockerfile .
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg tesseract-ocr libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY app/agents/requirements.txt /tmp/agents/requirements.txt
# Install CPU-only torch wheel first so ultralytics doesn't pull the CUDA build.
RUN pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cpu \
        torch==2.4.1 \
    && pip install --no-cache-dir -r /tmp/agents/requirements.txt

# Copy the feedback agent source AND any pre-baked highlight YOLO weights
# (agents/feedback/models/highlight_yolo_v1.pt). The weights file is optional —
# the runtime gracefully falls back to the legacy HSV detector if it is absent
# or VIDEO_HIGHLIGHT_DETECTOR is left unset.
COPY app/agents /app/agents
COPY app/yolo_model /app/yolo_model
RUN mkdir -p /app/agents/feedback/models /app/yolo_model/artifacts/pose

ENV HOST=0.0.0.0
ENV PORT=5055
ENV DATA_DIR=/app/data

EXPOSE 5055

CMD ["sh", "-c", "uvicorn agents.feedback.main:app --host 0.0.0.0 --port ${PORT:-5055}"]
