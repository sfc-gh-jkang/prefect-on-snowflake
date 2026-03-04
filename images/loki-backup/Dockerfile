FROM python:3.12-slim

LABEL maintainer="prefect-spcs"
LABEL description="Loki data backup sidecar for SPCS"

RUN pip install --no-cache-dir requests==2.32.*

# Build context must be images/ directory:
#   docker buildx build --platform linux/amd64 -f loki-backup/Dockerfile -t loki-backup .
COPY stage-backup/backup_lib.py /opt/backup/backup_lib.py
COPY loki-backup/backup.py /opt/backup/backup.py

ENV PYTHONPATH="/opt/backup"

CMD ["python", "/opt/backup/backup.py"]
