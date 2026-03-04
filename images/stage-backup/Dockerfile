FROM python:3.12-slim

LABEL maintainer="prefect-spcs"
LABEL description="Base image for SPCS stage backup operations"

RUN pip install --no-cache-dir requests==2.32.*

COPY backup_lib.py /opt/backup/backup_lib.py
ENV PYTHONPATH="/opt/backup"
