FROM python:3.12-slim

# Lightweight Prometheus exporter that polls the Prefect REST API
# and exposes application-level metrics (flow runs, deployments, work pools).

RUN pip install --no-cache-dir prometheus_client==0.21.1 requests==2.32.3

COPY exporter.py /app/exporter.py

EXPOSE 9394

CMD ["python", "/app/exporter.py"]
