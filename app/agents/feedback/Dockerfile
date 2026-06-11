# Build from repository root: docker build -f app/agents/feedback/Dockerfile .
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg tesseract-ocr libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Install torch+torchvision (CPU) first, then app deps without opencv-python duplicate.
RUN pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cpu \
        "torch==2.4.1" "torchvision==0.19.1" \
    && pip install --no-cache-dir \
        fastapi==0.115.12 "uvicorn[standard]==0.34.2" httpx==0.28.1 python-dotenv==1.0.1 \
        pydantic==2.11.4 openai==1.76.0 Pillow==11.2.1 opencv-python-headless==4.11.0.86 \
        jinja2==3.1.6 python-multipart==0.0.20 pytesseract==0.3.13 "pyyaml>=6.0" \
        matplotlib scipy pandas seaborn psutil py-cpuinfo requests tqdm ultralytics-thop \
    && pip install --no-cache-dir ultralytics==8.3.27 --no-deps

COPY app/agents /app/agents
COPY app/yolo_model /app/yolo_model
# Explicit copy so build fails loudly if best.pt missing from context (expect ~6MB in build log).
COPY app/yolo_model/artifacts/train/highlight_v1.1.0/weights/best.pt \
    /app/agents/feedback/models/highlight_yolo_v1.pt
RUN mkdir -p /app/yolo_model/artifacts/pose \
    && ls -lh /app/agents/feedback/models/highlight_yolo_v1.pt

ENV YOLO_HIGHLIGHT_WEIGHTS=/app/agents/feedback/models/highlight_yolo_v1.pt
ENV HOST=0.0.0.0
ENV PORT=5055
ENV DATA_DIR=/app/data

EXPOSE 5055

CMD ["sh", "-c", "uvicorn agents.feedback.main:app --host 0.0.0.0 --port ${PORT:-5055}"]
