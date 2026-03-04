FROM python:3.12-slim

# SPCS Log Poller — queries snowflake.telemetry.events for container logs
# and pushes them to Loki via HTTP push API.

RUN pip install --no-cache-dir \
    snowflake-connector-python==3.12.3 \
    requests==2.32.3

COPY poller.py /app/poller.py

CMD ["python", "/app/poller.py"]
