"""PAT auto-rotation flow — two-layer secret architecture.

Rotates the Snowflake Programmatic Access Token (PAT) across ALL consumers:

Layer 1 — Prefect Secret block (flow code reads at runtime, no restart needed):
  - "snowflake-pat" block → used by shared_utils, dashboard, hooks

Layer 2 — Infrastructure sources (require container restart to pick up):
  - .env file (SNOWFLAKE_PAT=...) → read by docker-compose envsubst at start
  - Snowflake secret PREFECT_SVC_PAT → read by SPCS dashboard UDF
  - GitLab CI variable SNOWFLAKE_PAT → read by CI pipeline

After updating sources, restarts infrastructure consumers:
  - GCP auth-proxy (docker compose up -d)
  - AWS auth-proxy (docker compose up -d)
  - Local auth-proxy (docker compose up -d)
  - Monitoring auth-proxy (docker compose up -d)

Does NOT restart SPCS services (that would regenerate URLs).
SPCS workers read from .env via envsubst at boot — they pick up the new PAT
on next natural restart or scale event.

Usage:
  # Scheduled or manual trigger:
  python pat_rotation_flow.py

  # Or via Prefect deployment:
  prefect deployment run 'pat-rotation/spcs-pool'
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import subprocess
from datetime import UTC, datetime
from typing import Any

from hooks import on_flow_failure
from prefect import flow, get_run_logger, task

logger = logging.getLogger("prefect.pat_rotation")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# All PAT consumer locations — used by tests to verify completeness
PAT_CONSUMERS = [
    "prefect_secret_block",  # Layer 1: Prefect Secret block "snowflake-pat"
    "dotenv_file",  # Layer 2: .env SNOWFLAKE_PAT=...
    "snowflake_secret",  # Layer 2: ALTER SECRET PREFECT_SVC_PAT
    "gitlab_ci_variable",  # Layer 2: GitLab project variable
    "gcp_auth_proxy",  # Restart: docker compose up -d (reads .env)
    "aws_auth_proxy",  # Restart: docker compose up -d (reads .env)
    "local_auth_proxy",  # Restart: docker compose up -d (reads .env)
    "monitoring_auth_proxy",  # Restart: docker compose up -d (reads .env)
]

# Infrastructure consumers that need restart after .env update
INFRA_RESTART_CONSUMERS = [
    "gcp_auth_proxy",
    "aws_auth_proxy",
    "local_auth_proxy",
    "monitoring_auth_proxy",
]

ENV_FILE_DEFAULT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")


# ---------------------------------------------------------------------------
# PAT decoding utilities
# ---------------------------------------------------------------------------


def decode_pat_expiry(pat: str) -> datetime | None:
    """Decode the 'exp' claim from a Snowflake PAT JWT.

    Returns a timezone-aware UTC datetime, or None if decoding fails.
    """
    try:
        parts = pat.split(".")
        if len(parts) != 3:
            return None
        payload_b64 = parts[1]
        # Fix base64url padding
        padding = 4 - len(payload_b64) % 4
        if padding < 4:
            payload_b64 += "=" * padding
        payload_b64 = payload_b64.replace("-", "+").replace("_", "/")
        payload_json = base64.b64decode(payload_b64)
        payload = json.loads(payload_json)
        exp = payload.get("exp")
        if exp is None:
            return None
        return datetime.fromtimestamp(exp, tz=UTC)
    except Exception:
        return None


def pat_days_remaining(pat: str) -> int | None:
    """Return number of days until PAT expires, or None if cannot decode."""
    expiry = decode_pat_expiry(pat)
    if expiry is None:
        return None
    now = datetime.now(tz=UTC)
    return (expiry - now).days


# ---------------------------------------------------------------------------
# Layer 1: Prefect Secret block update
# ---------------------------------------------------------------------------


@task(name="update-prefect-secret-block")
def update_prefect_secret_block(new_pat: str) -> bool:
    """Create or update the 'snowflake-pat' Prefect Secret block.

    This is the primary secret source for all flow code. Flows call
    Secret.load("snowflake-pat") at runtime — no restart needed.
    """
    log = get_run_logger()
    try:
        from prefect.blocks.system import Secret

        try:
            block = Secret.load("snowflake-pat")
            block.value = new_pat
            block.save("snowflake-pat", overwrite=True)
            log.info("Updated existing Prefect Secret block 'snowflake-pat'")
        except ValueError:
            # Block doesn't exist yet — create it
            block = Secret(value=new_pat)
            block.save("snowflake-pat")
            log.info("Created new Prefect Secret block 'snowflake-pat'")
        return True
    except Exception as exc:
        log.error("Failed to update Prefect Secret block: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Layer 2: .env file update
# ---------------------------------------------------------------------------


def update_dotenv_file(new_pat: str, env_file: str = ENV_FILE_DEFAULT) -> bool:
    """Replace SNOWFLAKE_PAT=... line in .env file.

    Returns True on success, False on failure.
    """
    try:
        if not os.path.isfile(env_file):
            logger.error(".env file not found: %s", env_file)
            return False

        content = _read_file(env_file)
        pattern = r"^SNOWFLAKE_PAT=.*$"
        if not re.search(pattern, content, re.MULTILINE):
            logger.error("SNOWFLAKE_PAT= line not found in %s", env_file)
            return False

        new_content = re.sub(pattern, f"SNOWFLAKE_PAT={new_pat}", content, flags=re.MULTILINE)
        _write_file(env_file, new_content)
        logger.info("Updated SNOWFLAKE_PAT in %s", env_file)
        return True
    except Exception as exc:
        logger.error("Failed to update .env: %s", exc)
        return False


def _read_file(path: str) -> str:
    """Read file contents. Separated for testability."""
    with open(path) as f:
        return f.read()


def _write_file(path: str, content: str) -> None:
    """Write file contents. Separated for testability."""
    with open(path, "w") as f:
        f.write(content)


@task(name="update-dotenv")
def update_dotenv_task(new_pat: str, env_file: str = ENV_FILE_DEFAULT) -> bool:
    """Task wrapper for .env update."""
    return update_dotenv_file(new_pat, env_file)


# ---------------------------------------------------------------------------
# Layer 2: Snowflake secret update
# ---------------------------------------------------------------------------


def build_alter_secret_sql(new_pat: str) -> str:
    """Build the ALTER SECRET SQL for PREFECT_SVC_PAT."""
    # Escape single quotes for safe SQL interpolation
    escaped = new_pat.replace("'", "''")
    return (
        "USE ROLE PREFECT_ROLE; "
        "USE DATABASE PREFECT_DB; "
        "USE SCHEMA PREFECT_SCHEMA; "
        f"ALTER SECRET PREFECT_SVC_PAT SET SECRET_STRING = '{escaped}';"
    )


@task(name="update-snowflake-secret")
def update_snowflake_secret(new_pat: str, connection: str = "aws_spcs") -> bool:
    """Update the Snowflake secret PREFECT_SVC_PAT via snow CLI."""
    log = get_run_logger()
    sql = build_alter_secret_sql(new_pat)
    try:
        result = _run_command(["snow", "sql", "-q", sql, "--connection", connection])
        log.info("Updated Snowflake secret PREFECT_SVC_PAT")
        return result.returncode == 0
    except Exception as exc:
        log.error("Failed to update Snowflake secret: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Layer 2: GitLab CI variable update
# ---------------------------------------------------------------------------


def build_gitlab_api_url(project_id: str, variable_key: str = "SNOWFLAKE_PAT") -> str:
    """Build the GitLab API URL for updating a project variable."""
    gitlab_host = os.environ.get("GITLAB_HOST", "gitlab.com")
    return f"https://{gitlab_host}/api/v4/projects/{project_id}/variables/{variable_key}"


@task(name="update-gitlab-ci-variable")
def update_gitlab_ci_variable(
    new_pat: str,
    project_id: str | None = None,
    gitlab_token: str | None = None,
) -> bool:
    """Update SNOWFLAKE_PAT variable in GitLab CI/CD via API.

    Reads GIT_ACCESS_TOKEN from env if gitlab_token not provided.
    Reads GITLAB_PROJECT_ID from env if project_id not provided.
    """
    log = get_run_logger()
    proj_id = project_id or os.environ.get("GITLAB_PROJECT_ID", "")
    token = gitlab_token or os.environ.get("GIT_ACCESS_TOKEN", "")

    if not proj_id:
        log.warning("GITLAB_PROJECT_ID not set — skipping GitLab CI variable update")
        return False
    if not token:
        log.warning("GIT_ACCESS_TOKEN not set — skipping GitLab CI variable update")
        return False

    url = build_gitlab_api_url(proj_id)
    try:
        import urllib.request

        data = json.dumps({"value": new_pat}).encode()
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "PRIVATE-TOKEN": token,
            },
            method="PUT",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status in (200, 201):
                log.info("Updated GitLab CI variable SNOWFLAKE_PAT (project %s)", proj_id)
                return True
            else:
                log.error("GitLab API returned status %d", resp.status)
                return False
    except Exception as exc:
        log.error("Failed to update GitLab CI variable: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Infrastructure restarts (re-read .env via docker compose)
# ---------------------------------------------------------------------------

# Compose file paths relative to project root
COMPOSE_FILES = {
    "gcp_auth_proxy": "workers/gcp/docker-compose.gcp.yaml",
    "aws_auth_proxy": "workers/aws/docker-compose.aws.yaml",
    "local_auth_proxy": "docker-compose.auth-proxy.yaml",
    "monitoring_auth_proxy": "monitoring/docker-compose.monitoring.yml",
}


def _run_command(cmd: list[str], cwd: str | None = None, timeout: int = 60) -> Any:
    """Run a subprocess command. Separated for testability."""
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=cwd,
    )


@task(name="restart-infra-consumer")
def restart_infra_consumer(
    consumer_key: str,
    project_dir: str | None = None,
) -> bool:
    """Restart a docker-compose infrastructure consumer to pick up new .env.

    Only restarts if the compose file exists AND the container is currently running.
    Returns True if restart succeeded or was skipped (not running).
    """
    log = get_run_logger()
    compose_file = COMPOSE_FILES.get(consumer_key)
    if not compose_file:
        log.warning("Unknown consumer: %s", consumer_key)
        return False

    proj = project_dir or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    full_path = os.path.join(proj, compose_file)

    if not os.path.isfile(full_path):
        log.info("Compose file not found, skipping: %s", compose_file)
        return True  # Not an error — consumer may not be deployed

    # Check if any containers are running for this compose file
    try:
        ps_result = _run_command(
            ["docker", "compose", "-f", full_path, "ps", "--quiet"],
            cwd=proj,
        )
        if not ps_result.stdout.strip():
            log.info("No running containers for %s — skipping restart", consumer_key)
            return True
    except Exception:
        log.info("Could not check container status for %s — skipping", consumer_key)
        return True

    # Restart: up -d recreates with new env
    try:
        result = _run_command(
            ["docker", "compose", "--env-file", ".env", "-f", full_path, "up", "-d"],
            cwd=proj,
        )
        if result.returncode == 0:
            log.info("Restarted %s", consumer_key)
            return True
        else:
            log.error("Failed to restart %s: %s", consumer_key, result.stderr)
            return False
    except Exception as exc:
        log.error("Exception restarting %s: %s", consumer_key, exc)
        return False


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


@task(name="validate-pat-propagation")
def validate_pat_propagation(new_pat: str) -> dict[str, bool]:
    """Validate that the new PAT is accepted by key endpoints.

    Checks:
    1. Prefect Secret block has the new value
    2. .env file has the new value
    """
    log = get_run_logger()
    results: dict[str, bool] = {}

    # Check Prefect Secret block
    try:
        from prefect.blocks.system import Secret

        block = Secret.load("snowflake-pat")
        results["prefect_secret_block"] = block.get() == new_pat
    except Exception:
        results["prefect_secret_block"] = False

    # Check .env file
    try:
        content = _read_file(ENV_FILE_DEFAULT)
        results["dotenv_file"] = f"SNOWFLAKE_PAT={new_pat}" in content
    except Exception:
        results["dotenv_file"] = False

    for key, ok in results.items():
        if ok:
            log.info("  %s: OK", key)
        else:
            log.warning("  %s: FAILED", key)

    return results


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------


@flow(
    name="pat-rotation",
    log_prints=True,
    on_failure=[on_flow_failure],
)
def pat_rotation(
    new_pat: str | None = None,
    connection: str = "aws_spcs",
    skip_gitlab: bool = False,
    skip_infra_restart: bool = False,
    env_file: str = ENV_FILE_DEFAULT,
) -> dict:
    """Rotate PAT across all consumers.

    Args:
        new_pat: The new PAT value. If None, reads from NEW_SNOWFLAKE_PAT env var.
        connection: Snow CLI connection name for Snowflake secret update.
        skip_gitlab: Skip GitLab CI variable update.
        skip_infra_restart: Skip docker-compose restarts (useful in CI).
        env_file: Path to .env file.

    Returns:
        Dict with results per consumer.
    """
    log = get_run_logger()
    pat = new_pat or os.environ.get("NEW_SNOWFLAKE_PAT", "")
    if not pat:
        raise ValueError(
            "No new PAT provided. Pass new_pat parameter or set NEW_SNOWFLAKE_PAT env var."
        )

    # Check new PAT is valid (decodable, not expired)
    days = pat_days_remaining(pat)
    if days is not None and days < 0:
        raise ValueError(f"New PAT is already expired ({days} days ago)")
    if days is not None:
        log.info("New PAT expires in %d days", days)

    results: dict[str, bool] = {}

    # Layer 1: Prefect Secret block
    log.info("=== Layer 1: Prefect Secret Block ===")
    results["prefect_secret_block"] = update_prefect_secret_block(pat)

    # Layer 2: .env file
    log.info("=== Layer 2: .env file ===")
    results["dotenv_file"] = update_dotenv_task(pat, env_file)

    # Layer 2: Snowflake secret
    log.info("=== Layer 2: Snowflake secret PREFECT_SVC_PAT ===")
    results["snowflake_secret"] = update_snowflake_secret(pat, connection)

    # Layer 2: GitLab CI variable
    if not skip_gitlab:
        log.info("=== Layer 2: GitLab CI variable ===")
        results["gitlab_ci_variable"] = update_gitlab_ci_variable(pat)
    else:
        log.info("=== Layer 2: GitLab CI variable (SKIPPED) ===")
        results["gitlab_ci_variable"] = True

    # Infrastructure restarts
    if not skip_infra_restart:
        log.info("=== Infrastructure Restarts ===")
        for consumer in INFRA_RESTART_CONSUMERS:
            results[consumer] = restart_infra_consumer(consumer)
    else:
        log.info("=== Infrastructure Restarts (SKIPPED) ===")
        for consumer in INFRA_RESTART_CONSUMERS:
            results[consumer] = True

    # Validation
    log.info("=== Validation ===")
    validation = validate_pat_propagation(pat)
    results["validation"] = all(validation.values())

    # Summary
    log.info("=" * 60)
    log.info("PAT ROTATION — RESULTS")
    failed = [k for k, v in results.items() if not v]
    for key, ok in results.items():
        log.info("  %s: %s", key, "OK" if ok else "FAILED")
    log.info("  Total: %d/%d succeeded", sum(results.values()), len(results))
    log.info("=" * 60)

    if failed:
        raise RuntimeError(f"PAT rotation partially failed: {', '.join(failed)}")

    return results


if __name__ == "__main__":
    pat_rotation()
