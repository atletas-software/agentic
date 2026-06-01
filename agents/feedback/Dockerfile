# Build from repository root: docker build -f agents/feedback/Dockerfile .
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg tesseract-ocr libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY agents/requirements.txt /tmp/agents/requirements.txt
COPY agents/feedback/requirements.txt /tmp/agents/feedback/requirements.txt
# Install CPU-only torch wheel first so ultralytics doesn't pull the CUDA build.
RUN pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cpu \
        torch==2.4.1 \
    && pip install --no-cache-dir -r /tmp/agents/feedback/requirements.txt

# Copy the feedback agent source AND any pre-baked highlight YOLO weights
# (agents/feedback/models/highlight_yolo_v1.pt). The weights file is optional —
# the runtime gracefully falls back to the legacy HSV detector if it is absent
# or VIDEO_HIGHLIGHT_DETECTOR is left unset.
COPY agents /app/agents
COPY yolo_model /app/yolo_model

ENV HOST=0.0.0.0
ENV PORT=5055
ENV DATA_DIR=/app/data

EXPOSE 5055

CMD ["sh", "-c", "uvicorn agents.feedback.main:app --host 0.0.0.0 --port ${PORT:-5055}"]
