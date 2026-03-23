"""Stage cleanup: remove old files from Snowflake internal stages.

Scheduled daily — scans PREFECT_FLOWS and MONITOR_STAGE for files older
than a configurable retention period and removes them to save storage.
"""

from __future__ import annotations

import os

from hooks import on_flow_failure
from prefect import flow, get_run_logger, task
from prefect.tasks import exponential_backoff
from shared_utils import execute_ddl, execute_query

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEFAULT_RETENTION_DAYS = int(os.environ.get("STAGE_RETENTION_DAYS", "30"))
STAGES_TO_CLEAN = [
    "PREFECT_DB.PREFECT_SCHEMA.PREFECT_FLOWS",
    "PREFECT_DB.PREFECT_SCHEMA.MONITOR_STAGE/backups",
]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(
    retries=2, retry_delay_seconds=exponential_backoff(backoff_factor=10), retry_jitter_factor=0.5
)
def list_stage_files(stage: str) -> list[dict]:
    """List all files in a stage with their last-modified timestamps."""
    logger = get_run_logger()
    query = f"LIST @{stage}"
    try:
        columns, rows = execute_query(query)
    except Exception as e:
        logger.warning("Failed to list stage %s: %s", stage, e)
        return []

    # LIST returns: name, size, md5, last_modified
    files = []
    for row in rows:
        files.append(
            {
                "name": row[0],
                "size": row[1],
                "last_modified": row[3] if len(row) > 3 else None,
            }
        )
    logger.info("Stage %s: %d files found", stage, len(files))
    return files


@task(
    retries=1, retry_delay_seconds=exponential_backoff(backoff_factor=10), retry_jitter_factor=0.5
)
def identify_stale_files(files: list[dict], retention_days: int) -> list[str]:
    """Identify files older than retention_days.

    Uses Snowflake SQL to compute the age of each file. Files without
    timestamps are kept (not cleaned).
    """
    logger = get_run_logger()
    stale = []
    for f in files:
        if f["last_modified"] is None:
            continue
        # LIST @stage last_modified is a string like 'Thu, 01 Jan 2025 00:00:00 GMT'
        # We compare via SQL for timezone correctness
        query = f"""
            SELECT CASE
                WHEN DATEDIFF(DAY, TRY_TO_TIMESTAMP('{f["last_modified"]}',
                    'DY, DD MON YYYY HH24:MI:SS TZH'), CURRENT_TIMESTAMP()) > {retention_days}
                THEN 1 ELSE 0
            END
        """
        try:
            _, rows = execute_query(query)
            if rows and rows[0][0] == 1:
                stale.append(f["name"])
        except Exception:
            pass  # Keep files we can't parse

    logger.info("Found %d stale files (> %d days old)", len(stale), retention_days)
    return stale


@task(
    retries=1, retry_delay_seconds=exponential_backoff(backoff_factor=10), retry_jitter_factor=0.5
)
def remove_files(stage: str, file_names: list[str]) -> int:
    """Remove stale files from the stage."""
    logger = get_run_logger()
    removed = 0
    for name in file_names:
        try:
            execute_ddl(f"REMOVE @{stage}/{name}")
            removed += 1
        except Exception as e:
            logger.warning("Failed to remove %s: %s", name, e)
    logger.info("Removed %d/%d stale files from %s", removed, len(file_names), stage)
    return removed


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(
    name="stage-cleanup",
    log_prints=True,
    on_failure=[on_flow_failure],
)
def stage_cleanup(retention_days: int = DEFAULT_RETENTION_DAYS):
    """Clean up old files from configured Snowflake stages.

    1. List files in each stage
    2. Identify files older than retention_days
    3. Remove stale files
    4. Report summary
    """
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("STAGE CLEANUP — START (retention: %d days)", retention_days)
    logger.info("=" * 60)

    total_scanned = 0
    total_removed = 0

    for stage in STAGES_TO_CLEAN:
        files = list_stage_files(stage)
        total_scanned += len(files)

        stale = identify_stale_files(files, retention_days)
        if stale:
            removed = remove_files(stage, stale)
            total_removed += removed
        else:
            logger.info("Stage %s: no stale files", stage)

    logger.info("=" * 60)
    logger.info("STAGE CLEANUP — COMPLETE")
    logger.info("  Stages scanned:  %d", len(STAGES_TO_CLEAN))
    logger.info("  Files scanned:   %d", total_scanned)
    logger.info("  Files removed:   %d", total_removed)
    logger.info("=" * 60)

    return {
        "stages_scanned": len(STAGES_TO_CLEAN),
        "files_scanned": total_scanned,
        "files_removed": total_removed,
        "status": "COMPLETE",
    }


if __name__ == "__main__":
    stage_cleanup()
