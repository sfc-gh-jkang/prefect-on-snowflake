"""Shared utilities available to all flows.

Lives at the flows root (/opt/prefect/flows/shared_utils.py) so any flow
at any nesting depth can import it via Prefect's working_directory on sys.path.

Provides:
  - Constants: SCHEMA, APP_VERSION
  - table_name() for qualified table references
  - get_secret_value() — reads Prefect Secret block with env var fallback
  - get_snowflake_connection() with 3-tier auth and QUERY_TAG injection
  - execute_query() / execute_ddl() convenience wrappers
"""

import os

# Environment name: "dev", "staging", "prod", etc.  Drives deployment naming
# and can be used in flow logic.  Defaults to "dev" for safety.
PREFECT_ENV = os.environ.get("PREFECT_ENV", "dev")

# Fully-qualified schema derived from env vars (no hardcoding).
_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "PREFECT_DB")
_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "PREFECT_SCHEMA")
SCHEMA = f"{_DATABASE}.{_SCHEMA}"
APP_VERSION = "1.0.0"


def get_git_sha(short: bool = True) -> str:
    """Return the current git commit SHA, or 'unknown' if not in a repo.

    Args:
        short: If True (default), return the 7-char short SHA.
    """
    import subprocess

    cmd = ["git", "rev-parse"]
    if short:
        cmd.append("--short")
    cmd.append("HEAD")
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return "unknown"


def table_name(name: str) -> str:
    """Return a fully-qualified table name in the default schema."""
    return f"{SCHEMA}.{name}"


def get_secret_value(block_name: str, env_var: str, default: str = "") -> str:
    """Read a secret from Prefect Secret block, falling back to env var.

    Lookup order:
      1. Prefect Secret block (by block_name) — works in flow context
      2. Environment variable (env_var) — works everywhere
      3. Default value

    This enables the two-layer secret architecture: rotation updates the
    Prefect block once, and all flows pick up the new value on next run
    without restarts. The env var fallback ensures compatibility outside
    Prefect context (CLI scripts, local dev, CI).
    """
    # Try Prefect Secret block first
    try:
        from prefect.blocks.system import Secret

        block = Secret.load(block_name)
        value = block.get()
        if value:
            return value
    except Exception:
        pass

    # Fallback to environment variable
    return os.environ.get(env_var, default)


def _resolve_query_tag(query_tag: str | None = None) -> str:
    """Build a QUERY_TAG string for Snowflake session attribution.

    Format: prefect/<flow_name>  (or custom override).
    Falls back to prefect/unknown if no flow context is available.
    """
    if query_tag:
        return query_tag
    # Try to get flow name from Prefect runtime context
    try:
        from prefect.runtime import flow_run

        name = flow_run.flow_name
        if name:
            return f"prefect/{name}"
    except Exception:
        pass
    return "prefect/unknown"


def get_snowflake_connection(query_tag: str | None = None):
    """Return a snowflake.connector connection using the best available auth.

    Auth strategy (checked in order):
      1. Inside SPCS: uses OAuth token from /snowflake/session/token
      2. SNOWFLAKE_PAT set: uses programmatic access token
      3. Fallback: uses SNOWFLAKE_USER + SNOWFLAKE_PASSWORD env vars

    All connections set QUERY_TAG for cost attribution in Snowflake.
    """
    import snowflake.connector

    account = os.environ.get("SNOWFLAKE_ACCOUNT", "")
    tag = _resolve_query_tag(query_tag)
    common = dict(
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "PREFECT_DB"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "PREFECT_SCHEMA"),
        session_parameters={"QUERY_TAG": tag},
    )

    token_path = "/snowflake/session/token"
    pat = get_secret_value("snowflake-pat", "SNOWFLAKE_PAT")

    if os.path.isfile(token_path):
        with open(token_path) as f:
            token = f.read().strip()
        return snowflake.connector.connect(
            host=os.environ.get("SNOWFLAKE_HOST", ""),
            account=account,
            authenticator="oauth",
            token=token,
            **common,
        )
    elif pat:
        return snowflake.connector.connect(
            account=account,
            user=os.environ.get("SNOWFLAKE_USER", ""),
            password=pat,
            **common,
        )
    else:
        return snowflake.connector.connect(
            account=account,
            user=os.environ.get("SNOWFLAKE_USER", ""),
            password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
            **common,
        )


def execute_query(sql: str, query_tag: str | None = None) -> tuple:
    """Execute a query and return (columns, rows)."""
    conn = get_snowflake_connection(query_tag)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [d[0] for d in cur.description]
        return columns, rows
    finally:
        conn.close()


def execute_ddl(sql: str, query_tag: str | None = None) -> str:
    """Execute a DDL statement and return the status message."""
    conn = get_snowflake_connection(query_tag)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchone()[0] if cur.description else "OK"
    finally:
        conn.close()
