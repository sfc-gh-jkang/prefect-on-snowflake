"""Shared library for SPCS stage backup/restore operations.

Provides authenticated access to Snowflake stages using the SPCS OAuth token
at /snowflake/session/token. Supports PUT, GET, LIST, and DELETE operations
for periodic backup of Prometheus TSDB and Loki data to @MONITOR_STAGE.

No external secrets or EAIs required — uses the built-in SPCS session token.
"""

import logging
import os
import tarfile
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# SPCS session token path — mounted automatically in every SPCS container
SPCS_TOKEN_PATH = "/snowflake/session/token"

# Snowflake stage SQL endpoint — uses the SPCS internal token endpoint
SNOWFLAKE_TOKEN_REFRESH_INTERVAL = 300  # refresh token every 5 min


class SPCSStageClient:
    """Client for Snowflake stage operations using SPCS OAuth token."""

    def __init__(
        self,
        database: str = "PREFECT_DB",
        schema: str = "PREFECT_SCHEMA",
        stage: str = "MONITOR_STAGE",
        warehouse: str = "COMPUTE_WH",
    ):
        self.database = database
        self.schema = schema
        self.stage = stage
        self.warehouse = warehouse
        self._token: str | None = None
        self._token_time: float = 0

    @property
    def token(self) -> str:
        """Read the SPCS OAuth token, refreshing if stale."""
        now = time.time()
        if self._token is None or (now - self._token_time) > SNOWFLAKE_TOKEN_REFRESH_INTERVAL:
            token_path = Path(SPCS_TOKEN_PATH)
            if not token_path.exists():
                raise FileNotFoundError(
                    f"SPCS token not found at {SPCS_TOKEN_PATH}. "
                    "This library must run inside an SPCS container."
                )
            self._token = token_path.read_text().strip()
            self._token_time = now
            logger.debug("Refreshed SPCS OAuth token")
        return self._token

    @property
    def _account_url(self) -> str:
        """Derive the Snowflake account URL from SPCS environment."""
        # SPCS containers have SNOWFLAKE_HOST set automatically
        host = os.environ.get("SNOWFLAKE_HOST", "")
        if not host:
            raise ValueError("SNOWFLAKE_HOST env var not set — not running in SPCS?")
        return f"https://{host}"

    @property
    def _headers(self) -> dict:
        return {
            "Authorization": f'Snowflake Token="{self.token}"',
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Snowflake-Authorization-Token-Type": "OAUTH",
        }

    def _execute_sql(self, sql: str, timeout: int = 120) -> dict:
        """Execute a SQL statement via the Snowflake REST API."""
        url = f"{self._account_url}/api/v2/statements"
        payload = {
            "statement": sql,
            "timeout": timeout,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
        }
        resp = requests.post(url, headers=self._headers, json=payload, timeout=timeout + 30)
        resp.raise_for_status()
        return resp.json()

    def list_stage_files(self, prefix: str = "") -> list[str]:
        """List files in the stage under the given prefix.

        Returns a list of file paths relative to the stage root.
        """
        stage_path = f"@{self.database}.{self.schema}.{self.stage}"
        if prefix:
            stage_path += f"/{prefix}"
        sql = f"LIST {stage_path};"
        result = self._execute_sql(sql)
        files = []
        for row in result.get("data", []):
            # LIST returns: name, size, md5, last_modified
            if row and len(row) >= 1:
                # Strip the stage prefix from the path
                name = row[0]
                # name format: "stage_name/path/to/file"
                files.append(name)
        return sorted(files)

    def upload_file(self, local_path: str, stage_prefix: str, overwrite: bool = True) -> bool:
        """Upload a local file to the stage using PUT.

        Uses the SQL PUT command via the REST API.
        """
        stage_path = f"@{self.database}.{self.schema}.{self.stage}/{stage_prefix}"
        overwrite_str = "OVERWRITE = TRUE" if overwrite else ""
        sql = f"PUT 'file://{local_path}' '{stage_path}' {overwrite_str} AUTO_COMPRESS = FALSE;"
        try:
            self._execute_sql(sql, timeout=300)
            logger.info("Uploaded %s to %s", local_path, stage_path)
            return True
        except Exception:
            logger.exception("Failed to upload %s to %s", local_path, stage_path)
            return False

    def download_file(self, stage_file: str, local_dir: str) -> str | None:
        """Download a file from the stage using GET.

        Returns the local file path or None on failure.
        """
        stage_path = f"@{self.database}.{self.schema}.{self.stage}/{stage_file}"
        sql = f"GET '{stage_path}' 'file://{local_dir}';"
        try:
            self._execute_sql(sql, timeout=300)
            # GET downloads to local_dir with the same filename
            filename = Path(stage_file).name
            local_path = os.path.join(local_dir, filename)
            if os.path.exists(local_path):
                logger.info("Downloaded %s to %s", stage_path, local_path)
                return local_path
            logger.warning("GET succeeded but file not found at %s", local_path)
            return None
        except Exception:
            logger.exception("Failed to download %s", stage_path)
            return None

    def delete_file(self, stage_file: str) -> bool:
        """Delete a file from the stage using REMOVE."""
        stage_path = f"@{self.database}.{self.schema}.{self.stage}/{stage_file}"
        sql = f"REMOVE '{stage_path}';"
        try:
            self._execute_sql(sql)
            logger.info("Deleted %s", stage_path)
            return True
        except Exception:
            logger.exception("Failed to delete %s", stage_path)
            return False

    def prune_old_backups(self, prefix: str, keep: int = 3) -> int:
        """Delete old backups, keeping only the most recent N files.

        Files are sorted by name (which includes timestamp) so lexicographic
        ordering gives chronological ordering.

        Returns the number of files deleted.
        """
        files = self.list_stage_files(prefix)
        if len(files) <= keep:
            logger.info("Found %d backups (keep=%d), nothing to prune", len(files), keep)
            return 0

        # Sort ascending — oldest first
        files.sort()
        to_delete = files[:-keep]  # everything except the newest `keep`
        deleted = 0
        for f in to_delete:
            # Strip the stage prefix that LIST returns (e.g., "monitor_stage/backups/...")
            # We need the path relative to the stage root
            rel_path = f
            # LIST returns paths like "monitor_stage/backups/prom/file.tar.gz"
            # We need just "backups/prom/file.tar.gz"
            stage_name_lower = self.stage.lower()
            if "/" in rel_path:
                parts = rel_path.split("/", 1)
                if parts[0].lower() == stage_name_lower:
                    rel_path = parts[1]
            if self.delete_file(rel_path):
                deleted += 1
        logger.info("Pruned %d old backups (kept %d)", deleted, keep)
        return deleted


def create_tarball(
    source_dir: str, output_path: str, exclude_patterns: list[str] | None = None
) -> str:
    """Create a gzipped tarball of a directory.

    Args:
        source_dir: Directory to archive
        output_path: Full path for the output .tar.gz file
        exclude_patterns: Optional list of glob patterns to exclude

    Returns:
        The output path
    """
    exclude = set(exclude_patterns or [])

    def _filter(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo | None:
        for pattern in exclude:
            if pattern in tarinfo.name:
                return None
        return tarinfo

    with tarfile.open(output_path, "w:gz") as tar:
        tar.add(source_dir, arcname=".", filter=_filter)

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    logger.info("Created tarball %s (%.1f MB)", output_path, size_mb)
    return output_path


def extract_tarball(tarball_path: str, target_dir: str) -> None:
    """Extract a gzipped tarball to a directory.

    Args:
        tarball_path: Path to the .tar.gz file
        target_dir: Directory to extract into
    """
    with tarfile.open(tarball_path, "r:gz") as tar:
        tar.extractall(path=target_dir)
    logger.info("Extracted %s to %s", tarball_path, target_dir)


def get_timestamp() -> str:
    """Return a UTC timestamp string suitable for filenames."""
    return time.strftime("%Y%m%d-%H%M%S", time.gmtime())


def wait_for_service(url: str, timeout: int = 120, interval: int = 5) -> bool:
    """Wait for an HTTP service to become ready.

    Args:
        url: URL to poll (expects 2xx response)
        timeout: Maximum time to wait in seconds
        interval: Polling interval in seconds

    Returns:
        True if service is ready, False if timeout reached
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(url, timeout=5)
            if resp.ok:
                logger.info("Service ready at %s", url)
                return True
        except requests.RequestException:
            pass
        time.sleep(interval)
    logger.warning("Timed out waiting for %s after %ds", url, timeout)
    return False
