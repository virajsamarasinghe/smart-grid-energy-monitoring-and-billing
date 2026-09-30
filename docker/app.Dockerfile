FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app TZ=UTC
WORKDIR /app
COPY requirements/app.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY smartgrid ./smartgrid
COPY simulator ./simulator
COPY dashboard ./dashboard
COPY api ./api
COPY scripts ./scripts
COPY sql ./sql
