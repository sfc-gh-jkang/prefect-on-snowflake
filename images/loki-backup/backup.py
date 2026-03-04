"""Loki data backup to Snowflake stage.

Runs as a sidecar in PF_MONITOR. Periodically:
1. Tars the Loki chunks, index, and compactor data directories
2. Uploads to @MONITOR_STAGE/backups/loki/
3. Prunes old backups (keeps last N)

On startup, if the Loki data directory is empty (cold start), attempts
to restore from the latest stage backup. The event-log-poller will also
backfill any remaining gap via INITIAL_LOOKBACK_SECONDS.

Environment variables:
    BACKUP_INTERVAL_SECONDS  — time between backups (default: 21600 = 6h)
    BACKUP_KEEP_COUNT        — number of backups to retain (default: 3)
    LOKI_DATA_DIR            — Loki data directory (default: /loki)
    STAGE_BACKUP_PREFIX      — stage path prefix (default: backups/loki)
    RESTORE_ON_COLD_START    — restore from stage if data is empty (default: true)
"""

import logging
import os
import tempfile
import time
from pathlib import Path

from backup_lib import (
    SPCSStageClient,
    create_tarball,
    extract_tarball,
    get_timestamp,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [loki-backup] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)

# Configuration from environment
BACKUP_INTERVAL = int(os.environ.get("BACKUP_INTERVAL_SECONDS", "21600"))
KEEP_COUNT = int(os.environ.get("BACKUP_KEEP_COUNT", "3"))
LOKI_DATA_DIR = os.environ.get("LOKI_DATA_DIR", "/loki")
STAGE_PREFIX = os.environ.get("STAGE_BACKUP_PREFIX", "backups/loki")
RESTORE_ON_COLD_START = os.environ.get("RESTORE_ON_COLD_START", "true").lower() == "true"

# Subdirectories to back up — these are the ones that hold state
BACKUP_SUBDIRS = [
    "chunks",
    "compactor",
    "rules-tmp",
    "index",
    "boltdb-shipper-active",
    "boltdb-shipper-cache",
    "tsdb-shipper-active",
    "tsdb-shipper-cache",
]


def is_cold_start(data_dir: str) -> bool:
    """Check if the Loki data directory has no meaningful data.

    Loki creates chunks/ and compactor/ directories as it ingests data.
    An empty or nonexistent data dir means cold start.
    """
    data_path = Path(data_dir)
    if not data_path.exists():
        return True
    # Check if any of the known data subdirectories exist and are non-empty
    for subdir in BACKUP_SUBDIRS:
        sub = data_path / subdir
        if sub.exists() and any(sub.iterdir()):
            return False
    return True


def restore_from_stage(client: SPCSStageClient, data_dir: str) -> bool:
    """Download and extract the latest backup from stage."""
    logger.info("Attempting restore from stage prefix: %s", STAGE_PREFIX)
    files = client.list_stage_files(STAGE_PREFIX)
    if not files:
        logger.info("No backups found on stage — starting fresh")
        return False

    latest = files[-1]
    logger.info("Found %d backups, restoring from: %s", len(files), latest)

    with tempfile.TemporaryDirectory() as tmpdir:
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

        logger.info("Extracting backup to %s", data_dir)
        extract_tarball(local_file, data_dir)
        logger.info("Restore complete from %s", latest)
        return True


def backup_to_stage(client: SPCSStageClient, data_dir: str) -> bool:
    """Tar the Loki data directories and upload to stage."""
    data_path = Path(data_dir)
    if not data_path.exists():
        logger.warning("Loki data directory does not exist: %s", data_dir)
        return False

    # Check there's actual data to back up
    has_data = False
    for subdir in BACKUP_SUBDIRS:
        sub = data_path / subdir
        if sub.exists() and any(sub.iterdir()):
            has_data = True
            break

    if not has_data:
        logger.info("No Loki data to back up yet — skipping")
        return True  # not an error, just nothing to do

    timestamp = get_timestamp()
    with tempfile.TemporaryDirectory() as tmpdir:
        tarball_name = f"loki-snapshot-{timestamp}.tar.gz"
        tarball_path = os.path.join(tmpdir, tarball_name)

        # Tar the entire data directory, excluding write-ahead logs that
        # may be actively written (Loki handles WAL recovery on startup)
        create_tarball(data_dir, tarball_path, exclude_patterns=["wal"])

        success = client.upload_file(tarball_path, STAGE_PREFIX)

    if success:
        client.prune_old_backups(STAGE_PREFIX, keep=KEEP_COUNT)

    return success


def main():
    """Main backup loop."""
    logger.info("Loki backup sidecar starting")
    logger.info(
        "Config: interval=%ds, keep=%d, data_dir=%s, stage_prefix=%s",
        BACKUP_INTERVAL,
        KEEP_COUNT,
        LOKI_DATA_DIR,
        STAGE_PREFIX,
    )

    client = SPCSStageClient()

    # Cold start restore
    if RESTORE_ON_COLD_START and is_cold_start(LOKI_DATA_DIR):
        logger.info("Cold start detected — attempting restore from stage")
        restore_from_stage(client, LOKI_DATA_DIR)
    else:
        logger.info("Existing Loki data found — skipping restore")

    # Wait a bit for Loki to start ingesting before first backup
    logger.info("Waiting 60s for Loki to start ingesting...")
    time.sleep(60)

    logger.info("Starting backup loop (interval=%ds)", BACKUP_INTERVAL)
    while True:
        time.sleep(BACKUP_INTERVAL)
        try:
            success = backup_to_stage(client, LOKI_DATA_DIR)
            if success:
                logger.info("Backup completed successfully")
            else:
                logger.error("Backup failed")
        except Exception:
            logger.exception("Unexpected error during backup")


if __name__ == "__main__":
    main()
