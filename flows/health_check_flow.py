"""Health check: verify SPCS services, workers, and key infrastructure.

Scheduled every 15 minutes — lightweight probes to detect outages fast.
Fires on_flow_failure hook for immediate Slack/webhook notification.
"""

from __future__ import annotations

import os

from hooks import on_flow_failure
from prefect import flow, get_run_logger, task
from prefect.tasks import exponential_backoff
from shared_utils import execute_query

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
EXPECTED_SERVICES = ["PF_SERVER", "PF_WORKER", "PF_REDIS", "PF_SERVICES", "PF_MONITOR"]
DB = os.environ.get("SNOWFLAKE_DATABASE", "PREFECT_DB")
SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "PREFECT_SCHEMA")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(
    retries=2, retry_delay_seconds=exponential_backoff(backoff_factor=10), retry_jitter_factor=0.5
)
def check_snowflake_connectivity() -> bool:
    """Verify basic Snowflake connectivity with a simple query."""
    logger = get_run_logger()
    _, rows = execute_query("SELECT CURRENT_TIMESTAMP(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
    ts, wh, role = rows[0]
    logger.info("Snowflake OK — time: %s, warehouse: %s, role: %s", ts, wh, role)
    return True


@task(
    retries=2, retry_delay_seconds=exponential_backoff(backoff_factor=10), retry_jitter_factor=0.5
)
def check_services_running() -> dict:
    """Check that all expected SPCS services are running.

    Uses SHOW SERVICES to enumerate current service status.
    Filters to RUNNING services only (ignores DONE jobs).
    """
    logger = get_run_logger()
    query = f"SHOW SERVICES IN SCHEMA {DB}.{SCHEMA}"
    columns, rows = execute_query(query)

    # Use column names for robust parsing (index varies across connector versions)
    col_map = {c.upper(): i for i, c in enumerate(columns)}
    name_idx = col_map.get("NAME", col_map.get("name", 1))
    status_idx = col_map.get("STATUS", col_map.get("status"))

    running_services = set()
    for row in rows:
        svc_name = str(row[name_idx]).upper()
        # Only count RUNNING services (skip DONE jobs, SUSPENDED, etc.)
        if status_idx is not None:
            status = str(row[status_idx]).upper()
            if status != "RUNNING":
                continue
        running_services.add(svc_name)

    missing = [s for s in EXPECTED_SERVICES if s not in running_services]
    found = [s for s in EXPECTED_SERVICES if s in running_services]

    logger.info("Services running: %s", ", ".join(found) if found else "NONE")
    if missing:
        logger.warning("Services MISSING: %s", ", ".join(missing))

    return {
        "expected": len(EXPECTED_SERVICES),
        "found": len(found),
        "missing": missing,
        "passed": len(missing) == 0,
    }


@task(
    retries=2, retry_delay_seconds=exponential_backoff(backoff_factor=10), retry_jitter_factor=0.5
)
def check_compute_pools() -> dict:
    """Check that compute pools are active/idle (not suspended or failed)."""
    logger = get_run_logger()
    query = "SHOW COMPUTE POOLS LIKE 'PREFECT_%'"
    columns, rows = execute_query(query)

    # Use column names for robust parsing
    col_map = {c.upper(): i for i, c in enumerate(columns)}
    name_idx = col_map.get("NAME", col_map.get("name", 1))
    state_idx = col_map.get("STATE", col_map.get("state", 2))

    pool_status = {}
    unhealthy = []
    for row in rows:
        name = str(row[name_idx]).upper()
        state = str(row[state_idx]).upper()
        pool_status[name] = state
        # SUSPENDED pools with auto_resume are expected (e.g., DASHBOARD_POOL)
        if state not in ("ACTIVE", "IDLE", "STARTING", "SUSPENDED"):
            unhealthy.append(f"{name}={state}")

    logger.info("Compute pools: %s", pool_status)
    if unhealthy:
        logger.warning("Unhealthy pools: %s", ", ".join(unhealthy))

    return {
        "pools": pool_status,
        "unhealthy": unhealthy,
        "passed": len(unhealthy) == 0,
    }


@task(
    retries=2, retry_delay_seconds=exponential_backoff(backoff_factor=10), retry_jitter_factor=0.5
)
def check_database_objects() -> dict:
    """Verify that key database objects (stages, secrets) exist."""
    logger = get_run_logger()
    checks = {
        "PREFECT_FLOWS_STAGE": f"SHOW STAGES LIKE 'PREFECT_FLOWS' IN SCHEMA {DB}.{SCHEMA}",
        "MONITOR_STAGE": f"SHOW STAGES LIKE 'MONITOR_STAGE' IN SCHEMA {DB}.{SCHEMA}",
    }

    results = {}
    missing = []
    for obj_name, query in checks.items():
        try:
            _, rows = execute_query(query)
            exists = len(rows) > 0
        except Exception:
            exists = False
        results[obj_name] = exists
        if not exists:
            missing.append(obj_name)

    logger.info("Database objects: %s", results)
    if missing:
        logger.warning("Missing objects: %s", ", ".join(missing))

    return {"objects": results, "missing": missing, "passed": len(missing) == 0}


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(
    name="health-check",
    log_prints=True,
    retries=1,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
    on_failure=[on_flow_failure],
)
def health_check():
    """Run infrastructure health checks.

    1. Snowflake connectivity
    2. SPCS service status
    3. Compute pool health
    4. Database object existence
    """
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("HEALTH CHECK — START")
    logger.info("=" * 60)

    failures: list[str] = []

    # 1. Connectivity
    check_snowflake_connectivity()

    # 2. Services
    svc = check_services_running()
    if not svc["passed"]:
        failures.append(f"Missing services: {', '.join(svc['missing'])}")

    # 3. Compute pools
    pools = check_compute_pools()
    if not pools["passed"]:
        failures.append(f"Unhealthy pools: {', '.join(pools['unhealthy'])}")

    # 4. Database objects
    objs = check_database_objects()
    if not objs["passed"]:
        failures.append(f"Missing objects: {', '.join(objs['missing'])}")

    logger.info("=" * 60)
    logger.info("HEALTH CHECK — RESULTS")
    logger.info("  Services:  %d/%d running", svc["found"], svc["expected"])
    logger.info("  Pools:     %d unhealthy", len(pools["unhealthy"]))
    logger.info("  Objects:   %d missing", len(objs["missing"]))
    logger.info("  Failures:  %d", len(failures))
    logger.info("=" * 60)

    if failures:
        raise RuntimeError(f"Health check failed: {'; '.join(failures)}")

    return {
        "services": f"{svc['found']}/{svc['expected']}",
        "pools_healthy": pools["passed"],
        "objects_ok": objs["passed"],
        "status": "HEALTHY",
    }


if __name__ == "__main__":
    health_check()
