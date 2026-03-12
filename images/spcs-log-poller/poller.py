"""SPCS Log Poller — bridges Snowflake event table logs to Loki.

Queries snowflake.telemetry.events for SPCS container logs and pushes
them to a local Loki instance via the HTTP push API.

Designed to run as a sidecar in PF_MONITOR on SPCS. Uses the built-in
OAuth token at /snowflake/session/token for Snowflake auth.
"""

import contextlib
import logging
import os
import time
from datetime import UTC, datetime

import requests
import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("spcs-log-poller")

LOKI_PUSH_URL = os.environ.get("LOKI_PUSH_URL", "http://localhost:3100/loki/api/v1/push")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
LOOKBACK_SECONDS = int(os.environ.get("LOOKBACK_SECONDS", "60"))
# On cold start, use a larger lookback window to backfill logs after restore.
# After the first successful poll, reverts to LOOKBACK_SECONDS.
INITIAL_LOOKBACK_SECONDS = int(os.environ.get("INITIAL_LOOKBACK_SECONDS", str(LOOKBACK_SECONDS)))
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_HOST = os.environ.get("SNOWFLAKE_HOST", "")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "PREFECT_DB")
SERVICES_FILTER = os.environ.get(
    "SPCS_SERVICES", "PF_SERVER,PF_WORKER,PF_REDIS,PF_SERVICES,PF_MONITOR"
).split(",")

TOKEN_PATH = "/snowflake/session/token"

# Track the last timestamp we've seen to avoid duplicates
_last_timestamp: str | None = None
# True until the first successful poll completes — uses INITIAL_LOOKBACK_SECONDS
_is_first_poll: bool = True


def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Create a Snowflake connection using SPCS OAuth token."""
    with open(TOKEN_PATH) as f:
        token = f.read().strip()
    return snowflake.connector.connect(
        host=SNOWFLAKE_HOST,
        account=SNOWFLAKE_ACCOUNT,
        authenticator="oauth",
        token=token,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        session_parameters={"QUERY_TAG": "monitoring/spcs-log-poller"},
    )


def fetch_logs(conn: snowflake.connector.SnowflakeConnection) -> list[dict]:
    """Query recent logs from the Snowflake event table."""
    global _last_timestamp
    services_list = ",".join(f"'{s}'" for s in SERVICES_FILTER)

    query = f"""
        SELECT
            TIMESTAMP::VARCHAR AS ts,
            SERVICE_NAME,
            CONTAINER_NAME,
            SEVERITY,
            MESSAGE
        FROM PREFECT_DB.PREFECT_SCHEMA.SPCS_EVENT_LOGS_TABLE
        WHERE SERVICE_NAME IN ({services_list})
    """
    if _last_timestamp:
        query += f"  AND TIMESTAMP > '{_last_timestamp}'::TIMESTAMP_LTZ\n"
    else:
        # First poll uses INITIAL_LOOKBACK_SECONDS for cold-start backfill
        lookback = INITIAL_LOOKBACK_SECONDS if _is_first_poll else LOOKBACK_SECONDS
        query += f"  AND TIMESTAMP > DATEADD(second, -{lookback}, CURRENT_TIMESTAMP())\n"
    query += "    ORDER BY TIMESTAMP ASC\n    LIMIT 1000\n"

    cur = conn.cursor()
    try:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [d[0] for d in cur.description]
        results = [dict(zip(columns, row, strict=False)) for row in rows]
        if results:
            _last_timestamp = results[-1]["TS"]
        return results
    finally:
        cur.close()


def push_to_loki(logs: list[dict]) -> None:
    """Push log entries to Loki via HTTP push API."""
    if not logs:
        return

    # Group by service+container (Loki stream labels)
    streams: dict[str, list] = {}
    for entry in logs:
        service = entry.get("SERVICE_NAME", "unknown")
        container = entry.get("CONTAINER_NAME", "unknown")
        severity = entry.get("SEVERITY", "INFO")
        key = f"{service}|{container}"
        if key not in streams:
            streams[key] = {
                "labels": {
                    "job": "spcs-logs",
                    "service": service.lower().replace("_", "-"),
                    "container": container,
                    "severity": severity,
                    "source": "event-table",
                },
                "values": [],
            }
        # Loki expects [timestamp_ns_string, log_line]
        ts_str = entry.get("TS", "")
        try:
            dt = datetime.fromisoformat(ts_str.replace(" ", "T").removesuffix("+00:00"))
            dt = dt.replace(tzinfo=UTC)
            ts_ns = str(int(dt.timestamp() * 1_000_000_000))
        except (ValueError, AttributeError):
            ts_ns = str(int(time.time() * 1_000_000_000))

        message = entry.get("MESSAGE", "")
        log_line = f"[{severity}] [{service}/{container}] {message}"
        streams[key]["values"].append([ts_ns, log_line])

    # Build Loki push payload
    payload = {
        "streams": [
            {
                "stream": data["labels"],
                "values": data["values"],
            }
            for data in streams.values()
        ]
    }

    try:
        resp = requests.post(
            LOKI_PUSH_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code not in (200, 204):
            log.warning("Loki push failed: %d %s", resp.status_code, resp.text[:200])
        else:
            log.info("Pushed %d log entries (%d streams) to Loki", len(logs), len(streams))
    except Exception as exc:
        log.warning("Loki push error: %s", exc)


def poll_once(conn: snowflake.connector.SnowflakeConnection) -> int:
    """Run one poll cycle. Returns number of logs pushed."""
    global _is_first_poll
    logs = fetch_logs(conn)
    push_to_loki(logs)
    if _is_first_poll:
        _is_first_poll = False
        log.info(
            "First poll complete (lookback=%ds), switching to normal lookback=%ds",
            INITIAL_LOOKBACK_SECONDS,
            LOOKBACK_SECONDS,
        )
    return len(logs)


def main() -> None:
    log.info(
        "Starting SPCS log poller: polling every %ds, pushing to %s",
        POLL_INTERVAL,
        LOKI_PUSH_URL,
    )
    log.info("Monitoring services: %s", ", ".join(SERVICES_FILTER))

    conn = get_connection()
    consecutive_errors = 0

    while True:
        try:
            count = poll_once(conn)
            consecutive_errors = 0
            if count > 0:
                log.info("Polled %d new log entries", count)
        except snowflake.connector.errors.ProgrammingError as exc:
            consecutive_errors += 1
            log.warning("Snowflake query error (%d): %s", consecutive_errors, exc)
            if consecutive_errors > 5:
                log.info("Reconnecting after %d consecutive errors", consecutive_errors)
                with contextlib.suppress(Exception):
                    conn.close()
                conn = get_connection()
                consecutive_errors = 0
        except Exception as exc:
            consecutive_errors += 1
            log.error("Unexpected error (%d): %s", consecutive_errors, exc)
            if consecutive_errors > 10:
                log.info("Reconnecting after %d consecutive errors", consecutive_errors)
                with contextlib.suppress(Exception):
                    conn.close()
                conn = get_connection()
                consecutive_errors = 0
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
