"""Prometheus TSDB backup to Snowflake stage.

Runs as a sidecar in PF_MONITOR. Periodically:
1. Triggers a TSDB snapshot via the Prometheus admin API
2. Tars the snapshot directory
3. Uploads to @MONITOR_STAGE/backups/prometheus/
4. Prunes old backups (keeps last N)

On startup, if the TSDB data directory is empty (cold start), attempts
to restore from the latest stage backup before Prometheus reads the TSDB.

Environment variables:
    BACKUP_INTERVAL_SECONDS  — time between backups (default: 21600 = 6h)
    BACKUP_KEEP_COUNT        — number of backups to retain (default: 3)
    PROMETHEUS_URL           — Prometheus base URL (default: http://localhost:9090)
    PROMETHEUS_DATA_DIR      — TSDB data directory (default: /prometheus)
    STAGE_BACKUP_PREFIX      — stage path prefix (default: backups/prometheus)
    RESTORE_ON_COLD_START    — restore from stage if TSDB is empty (default: true)
"""

import logging
import os
import shutil
import tempfile
import time
from pathlib import Path

import requests
from backup_lib import (
    SPCSStageClient,
    create_tarball,
    extract_tarball,
    get_timestamp,
    wait_for_service,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [prom-backup] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)

# Configuration from environment
BACKUP_INTERVAL = int(os.environ.get("BACKUP_INTERVAL_SECONDS", "21600"))
KEEP_COUNT = int(os.environ.get("BACKUP_KEEP_COUNT", "3"))
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")
PROMETHEUS_DATA_DIR = os.environ.get("PROMETHEUS_DATA_DIR", "/prometheus")
STAGE_PREFIX = os.environ.get("STAGE_BACKUP_PREFIX", "backups/prometheus")
RESTORE_ON_COLD_START = os.environ.get("RESTORE_ON_COLD_START", "true").lower() == "true"


def is_cold_start(data_dir: str) -> bool:
    """Check if the TSDB data directory is empty (no WAL or blocks).

    A fresh Prometheus creates a wal/ directory on first startup.
    If there's no wal/ and no block directories, this is a cold start.
    """
    data_path = Path(data_dir)
    if not data_path.exists():
        return True
    # Check for WAL directory or any block directories (ULID-named dirs)
    contents = list(data_path.iterdir())
    # Filter out snapshots directory and lock file
    meaningful = [c for c in contents if c.name not in ("snapshots", "lock", "queries.active")]
    return len(meaningful) == 0


def restore_from_stage(client: SPCSStageClient, data_dir: str) -> bool:
    """Download and extract the latest backup from stage.

    Returns True if restore succeeded, False otherwise.
    """
    logger.info("Attempting restore from stage prefix: %s", STAGE_PREFIX)
    files = client.list_stage_files(STAGE_PREFIX)
    if not files:
        logger.info("No backups found on stage — starting fresh")
        return False

    # Take the last file (newest by lexicographic timestamp ordering)
    latest = files[-1]
    logger.info("Found %d backups, restoring from: %s", len(files), latest)

    with tempfile.TemporaryDirectory() as tmpdir:
        # Strip stage name prefix from the path for download
        rel_path = latest
        stage_name_lower = client.stage.lower()
        if "/" in rel_path:
            parts = rel_path.split("/", 1)
            if parts[0].lower() == stage_name_lower:
                rel_path = parts[1]

        local_file = client.download_file(rel_path, tmpdir)
        if not local_file:
            logger.error("Failed to download %s", latest)
            return False

        # Extract to the data directory
        logger.info("Extracting backup to %s", data_dir)
        extract_tarball(local_file, data_dir)
        logger.info("Restore complete from %s", latest)
        return True


def create_snapshot(prometheus_url: str) -> str | None:
    """Trigger a TSDB snapshot via the Prometheus admin API.

    Returns the snapshot directory name or None on failure.
    """
    url = f"{prometheus_url}/api/v1/admin/tsdb/snapshot"
    try:
        resp = requests.post(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") == "success":
            snap_name = data["data"]["name"]
            logger.info("Created TSDB snapshot: %s", snap_name)
            return snap_name
        logger.error("Snapshot API returned non-success: %s", data)
        return None
    except Exception:
        logger.exception("Failed to create TSDB snapshot")
        return None


def backup_to_stage(client: SPCSStageClient, data_dir: str) -> bool:
    """Create a TSDB snapshot, tar it, and upload to stage."""
    # Step 1: Create snapshot
    snap_name = create_snapshot(PROMETHEUS_URL)
    if not snap_name:
        return False

    snap_dir = os.path.join(data_dir, "snapshots", snap_name)
    if not os.path.isdir(snap_dir):
        logger.error("Snapshot directory not found: %s", snap_dir)
        return False

    # Step 2: Create tarball
    timestamp = get_timestamp()
    with tempfile.TemporaryDirectory() as tmpdir:
        tarball_name = f"prom-snapshot-{timestamp}.tar.gz"
        tarball_path = os.path.join(tmpdir, tarball_name)
        create_tarball(snap_dir, tarball_path)

        # Step 3: Upload to stage
        success = client.upload_file(tarball_path, STAGE_PREFIX)

    # Step 4: Clean up the snapshot directory to save disk space
    try:
        shutil.rmtree(snap_dir)
        logger.info("Cleaned up snapshot directory: %s", snap_dir)
    except Exception:
        logger.warning("Failed to clean up snapshot dir: %s", snap_dir)

    # Step 5: Prune old backups
    if success:
        client.prune_old_backups(STAGE_PREFIX, keep=KEEP_COUNT)

    return success


def main():
    """Main backup loop."""
    logger.info("Prometheus backup sidecar starting")
    logger.info(
        "Config: interval=%ds, keep=%d, data_dir=%s, stage_prefix=%s",
        BACKUP_INTERVAL,
        KEEP_COUNT,
        PROMETHEUS_DATA_DIR,
        STAGE_PREFIX,
    )

    client = SPCSStageClient()

    # Cold start restore
    if RESTORE_ON_COLD_START and is_cold_start(PROMETHEUS_DATA_DIR):
        logger.info("Cold start detected — attempting restore from stage")
        restore_from_stage(client, PROMETHEUS_DATA_DIR)
    else:
        logger.info("Existing TSDB data found — skipping restore")

    # Wait for Prometheus to be ready before starting backup loop
    logger.info("Waiting for Prometheus to be ready...")
    if not wait_for_service(f"{PROMETHEUS_URL}/-/ready", timeout=180):
        logger.error("Prometheus did not become ready — starting backup loop anyway")

    logger.info("Starting backup loop (interval=%ds)", BACKUP_INTERVAL)
    while True:
        time.sleep(BACKUP_INTERVAL)
        try:
            success = backup_to_stage(client, PROMETHEUS_DATA_DIR)
            if success:
                logger.info("Backup completed successfully")
            else:
                logger.error("Backup failed")
        except Exception:
            logger.exception("Unexpected error during backup")


if __name__ == "__main__":
    main()
