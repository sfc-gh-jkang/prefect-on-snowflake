"""Prefect Application Prometheus Exporter.

Polls the Prefect REST API and exposes application-level metrics
that the built-in /api/metrics endpoint does not provide:

- Flow run counts by state
- Deployment counts by status
- Work pool status and worker counts

Designed to run as a sidecar in PF_MONITOR on SPCS.
"""

import logging
import os
import statistics
import time
from datetime import UTC, datetime, timedelta

import requests
from prometheus_client import Gauge, start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("prefect-exporter")

PREFECT_API_URL = os.environ.get("PREFECT_API_URL", "http://pf-server:4200/api")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
EXPORTER_PORT = int(os.environ.get("EXPORTER_PORT", "9394"))

# ── Metrics ──────────────────────────────────────────────────────
flow_runs_by_state = Gauge("prefect_flow_runs", "Current flow run count by state", ["state"])
deployments_total = Gauge("prefect_deployments_total", "Total deployment count")
deployments_by_status = Gauge("prefect_deployments", "Deployment count by status", ["status"])
work_pools_total = Gauge("prefect_work_pools_total", "Total work pool count")
work_pools_by_status = Gauge("prefect_work_pools", "Work pool count by status", ["status"])
work_pool_workers = Gauge(
    "prefect_work_pool_workers", "Worker count per work pool", ["pool_name", "status"]
)
api_poll_errors = Gauge("prefect_exporter_poll_errors_total", "Total API polling errors")
api_poll_duration = Gauge("prefect_exporter_poll_duration_seconds", "Duration of last poll cycle")
flow_run_duration_p50 = Gauge(
    "prefect_flow_run_duration_p50_seconds", "P50 flow run duration (completed, last 1h)"
)
flow_run_duration_p95 = Gauge(
    "prefect_flow_run_duration_p95_seconds", "P95 flow run duration (completed, last 1h)"
)
flow_run_duration_max = Gauge(
    "prefect_flow_run_duration_max_seconds", "Max flow run duration (completed, last 1h)"
)
flow_run_duration_avg = Gauge(
    "prefect_flow_run_duration_avg_seconds", "Avg flow run duration (completed, last 1h)"
)

FLOW_RUN_STATES = [
    "COMPLETED",
    "FAILED",
    "RUNNING",
    "PENDING",
    "SCHEDULED",
    "CANCELLING",
    "CANCELLED",
    "CRASHED",
    "PAUSED",
]

_error_count = 0


def _post(endpoint: str, payload: dict | None = None) -> list | dict | None:
    """POST to the Prefect API. Returns parsed JSON or None on error."""
    global _error_count
    try:
        resp = requests.post(
            f"{PREFECT_API_URL}{endpoint}",
            json=payload if payload is not None else {},
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        if not resp.ok:
            _error_count += 1
            api_poll_errors.set(_error_count)
            log.warning(
                "API error on %s: %d %s — %s",
                endpoint,
                resp.status_code,
                resp.reason,
                resp.text[:500],
            )
            return None
        return resp.json()
    except Exception as exc:
        _error_count += 1
        api_poll_errors.set(_error_count)
        log.warning("API error on %s: %s", endpoint, exc)
        return None


def _get(endpoint: str) -> list | dict | None:
    """GET from the Prefect API. Returns parsed JSON or None on error."""
    global _error_count
    try:
        resp = requests.get(
            f"{PREFECT_API_URL}{endpoint}",
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        _error_count += 1
        api_poll_errors.set(_error_count)
        log.warning("API error on %s: %s", endpoint, exc)
        return None


def collect_flow_runs() -> None:
    """Count flow runs in each state (last 24 hours)."""
    for state in FLOW_RUN_STATES:
        data = _post(
            "/flow_runs/count",
            {
                "flow_runs": {
                    "operator": "and_",
                    "state": {"type": {"any_": [state]}},
                },
            },
        )
        if data is not None:
            flow_runs_by_state.labels(state=state).set(data)


def collect_deployments() -> None:
    """Count deployments, split by is_schedule_active."""
    all_deps = _post("/deployments/filter", {"limit": 200})
    if all_deps is None:
        return
    total = len(all_deps)
    active = sum(1 for d in all_deps if d.get("is_schedule_active"))
    inactive = total - active
    deployments_total.set(total)
    deployments_by_status.labels(status="active").set(active)
    deployments_by_status.labels(status="inactive").set(inactive)


def collect_work_pools() -> None:
    """Count work pools and workers per pool."""
    pools = _post("/work_pools/filter", {})
    if pools is None:
        return
    total = len(pools)
    by_status: dict[str, int] = {}
    for pool in pools:
        status = pool.get("status", "UNKNOWN")
        by_status[status] = by_status.get(status, 0) + 1

    work_pools_total.set(total)
    for status, count in by_status.items():
        work_pools_by_status.labels(status=status).set(count)

    # Workers per pool
    for pool in pools:
        pool_name = pool.get("name", "unknown")
        workers = _post(f"/work_pools/{pool_name}/workers/filter", {})
        if workers is not None:
            by_status: dict[str, int] = {}
            for w in workers:
                ws = w.get("status", "UNKNOWN").upper()
                by_status[ws] = by_status.get(ws, 0) + 1
            for ws, count in by_status.items():
                work_pool_workers.labels(pool_name=pool_name, status=ws).set(count)
            if "ONLINE" not in by_status:
                work_pool_workers.labels(pool_name=pool_name, status="ONLINE").set(0)
            if "OFFLINE" not in by_status:
                work_pool_workers.labels(pool_name=pool_name, status="OFFLINE").set(0)


def collect_flow_run_durations() -> None:
    """Compute duration percentiles for COMPLETED flow runs in the last hour."""
    since = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    runs = _post(
        "/flow_runs/filter",
        {
            "flow_runs": {
                "operator": "and_",
                "state": {"type": {"any_": ["COMPLETED"]}},
                "start_time": {"after_": since},
            },
            "limit": 200,
        },
    )
    if not runs:
        return

    durations: list[float] = []
    for run in runs:
        total_run_time = run.get("total_run_time")
        # total_run_time is a float (seconds) in Prefect 3.x
        if total_run_time is not None and isinstance(total_run_time, int | float):
            durations.append(float(total_run_time))

    if not durations:
        return

    durations.sort()
    flow_run_duration_avg.set(statistics.mean(durations))
    flow_run_duration_max.set(durations[-1])
    # percentiles
    n = len(durations)
    flow_run_duration_p50.set(durations[n // 2])
    p95_idx = min(int(n * 0.95), n - 1)
    flow_run_duration_p95.set(durations[p95_idx])


def poll_once() -> None:
    """Run one full collection cycle."""
    start = time.monotonic()
    collect_flow_runs()
    collect_deployments()
    collect_work_pools()
    collect_flow_run_durations()
    duration = time.monotonic() - start
    api_poll_duration.set(duration)
    log.info("Poll completed in %.2fs", duration)


def main() -> None:
    log.info(
        "Starting Prefect exporter on :%d, polling %s every %ds",
        EXPORTER_PORT,
        PREFECT_API_URL,
        POLL_INTERVAL,
    )
    start_http_server(EXPORTER_PORT)
    while True:
        poll_once()
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
