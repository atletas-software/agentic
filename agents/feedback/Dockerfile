# Build from repository root: docker build -f agents/feedback/Dockerfile .
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

COPY agents/requirements.txt /tmp/agents/requirements.txt
COPY agents/feedback/requirements.txt /tmp/agents/feedback/requirements.txt
RUN pip install --no-cache-dir -r /tmp/agents/feedback/requirements.txt

COPY agents /app/agents

ENV HOST=0.0.0.0
ENV PORT=5055
ENV DATA_DIR=/app/data

EXPOSE 5055

CMD ["sh", "-c", "uvicorn agents.feedback.main:app --host 0.0.0.0 --port ${PORT:-5055}"]
