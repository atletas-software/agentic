FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/app

WORKDIR /app

COPY app/backendapi/requirements.txt /app/backendapi/requirements.txt
RUN pip install --no-cache-dir -r /app/backendapi/requirements.txt

COPY . /app

RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

CMD ["uvicorn", "backendapi.main:app", "--host", "0.0.0.0", "--port", "8000"]
