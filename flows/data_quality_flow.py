"""Data quality checks: table freshness, row counts, and schema drift detection.

Scheduled daily — runs e2e-pipeline-test as a prerequisite subflow to ensure
tables exist, then validates row counts and freshness thresholds.
Reports issues via flow hooks.
"""

from __future__ import annotations

import time

from e2e_test_flow import cleanup as e2e_cleanup
from e2e_test_flow import e2e_pipeline_test
from hooks import on_flow_failure
from prefect import flow, get_run_logger, task
from shared_utils import execute_query, table_name

# ---------------------------------------------------------------------------
# Configuration — tables to check and their expectations.
# Add entries here to extend monitoring coverage.
# ---------------------------------------------------------------------------
TABLE_CHECKS: list[dict] = [
    {
        "table": "E2E_ORDERS",
        "min_rows": 1,
        "freshness_hours": 168,  # 7 days — E2E test runs weekly
    },
    {
        "table": "E2E_ORDERS_SUMMARY",
        "min_rows": 1,
        "freshness_hours": 168,
    },
]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(retries=3, retry_delay_seconds=30)
def check_table_exists(tbl: str) -> bool:
    """Verify the table exists via direct query (not INFORMATION_SCHEMA)."""
    logger = get_run_logger()
    fqn = table_name(tbl)
    try:
        execute_query(f"SELECT 1 FROM {fqn} LIMIT 0")
        logger.info("Table %s exists: True", fqn)
        return True
    except Exception:
        logger.info("Table %s exists: False", fqn)
        return False


@task(retries=3, retry_delay_seconds=30)
def check_row_count(tbl: str, min_rows: int) -> dict:
    """Check that a table has at least min_rows rows."""
    logger = get_run_logger()
    fqn = table_name(tbl)
    _, rows = execute_query(f"SELECT COUNT(*) FROM {fqn}")
    count = rows[0][0]
    passed = count >= min_rows
    logger.info(
        "Row count for %s: %d (min: %d) — %s", fqn, count, min_rows, "PASS" if passed else "FAIL"
    )
    return {"table": tbl, "count": count, "min_rows": min_rows, "passed": passed}


@task(retries=3, retry_delay_seconds=30)
def check_freshness(tbl: str, max_hours: int) -> dict:
    """Check that a table was modified within the last max_hours hours.

    Uses INFORMATION_SCHEMA.TABLES.LAST_ALTERED as a proxy for freshness.
    """
    logger = get_run_logger()
    query = f"""
        SELECT TIMESTAMPDIFF(HOUR, LAST_ALTERED, CURRENT_TIMESTAMP())
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_CATALOG  = CURRENT_DATABASE()
          AND TABLE_SCHEMA   = CURRENT_SCHEMA()
          AND TABLE_NAME     = '{tbl}'
    """
    _, rows = execute_query(query)
    if not rows or rows[0][0] is None:
        logger.warning("Freshness check: table %s not found", tbl)
        return {"table": tbl, "hours_since_update": None, "max_hours": max_hours, "passed": False}

    hours_old = rows[0][0]
    passed = hours_old <= max_hours
    logger.info(
        "Freshness for %s: %d hours old (max: %d) — %s",
        tbl,
        hours_old,
        max_hours,
        "PASS" if passed else "FAIL",
    )
    return {"table": tbl, "hours_since_update": hours_old, "max_hours": max_hours, "passed": passed}


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(
    name="data-quality-check",
    log_prints=True,
    retries=1,
    retry_delay_seconds=120,
    on_failure=[on_flow_failure],
)
def data_quality_check():
    """Run data quality checks across configured tables.

    0. Run e2e-pipeline-test subflow (creates/refreshes E2E tables)
    1. Verify each table exists
    2. Check row counts meet minimums
    3. Verify data freshness thresholds
    4. Summarize results
    """
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("DATA QUALITY CHECK — START")
    logger.info("=" * 60)

    logger.info("Running e2e-pipeline-test as prerequisite subflow ...")
    e2e_pipeline_test(skip_cleanup=True)
    logger.info("e2e-pipeline-test completed — waiting for table visibility ...")
    time.sleep(5)

    results: list[dict] = []
    failures: list[str] = []

    try:
        for check in TABLE_CHECKS:
            tbl = check["table"]

            if not check_table_exists(tbl):
                failures.append(f"{tbl}: table does not exist")
                continue

            rc = check_row_count(tbl, check["min_rows"])
            results.append(rc)
            if not rc["passed"]:
                failures.append(f"{tbl}: row count {rc['count']} < {rc['min_rows']}")

            if "freshness_hours" in check:
                fr = check_freshness(tbl, check["freshness_hours"])
                results.append(fr)
                if not fr["passed"]:
                    failures.append(f"{tbl}: {fr['hours_since_update']}h old > {fr['max_hours']}h")
    finally:
        e2e_cleanup()
        logger.info("E2E tables cleaned up.")

    logger.info("=" * 60)
    logger.info("DATA QUALITY CHECK — RESULTS")
    logger.info("  Tables checked: %d", len(TABLE_CHECKS))
    logger.info("  Checks run:     %d", len(results))
    logger.info("  Failures:       %d", len(failures))
    for f in failures:
        logger.warning("  FAIL: %s", f)
    logger.info("=" * 60)

    if failures:
        raise RuntimeError(
            f"Data quality check failed: {len(failures)} issue(s): {'; '.join(failures)}"
        )

    return {
        "tables_checked": len(TABLE_CHECKS),
        "checks_passed": len(results),
        "status": "ALL PASSED",
    }


if __name__ == "__main__":
    data_quality_check()
