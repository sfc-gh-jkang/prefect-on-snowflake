"""Shared fixtures for the Prefect SPCS test suite."""

import os
import re
from pathlib import Path

import pytest
import requests
import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent.parent
SPECS_DIR = PROJECT_DIR / "specs"
SQL_DIR = PROJECT_DIR / "sql"
FLOWS_DIR = PROJECT_DIR / "flows"
SCRIPTS_DIR = PROJECT_DIR / "scripts"
IMAGES_DIR = PROJECT_DIR / "images"
WORKERS_DIR = PROJECT_DIR / "workers"
MONITORING_DIR = PROJECT_DIR / "monitoring"
COMPOSE_FILE = PROJECT_DIR / "docker-compose.yaml"
POOLS_FILE = PROJECT_DIR / "pools.yaml"

# Backwards-compatible alias
GCP_DIR = WORKERS_DIR / "gcp"


# ---------------------------------------------------------------------------
# Pool configuration — loaded from pools.yaml
# ---------------------------------------------------------------------------
def _load_pools() -> dict[str, dict]:
    """Load pool configuration from pools.yaml."""
    with open(POOLS_FILE) as f:
        data = yaml.safe_load(f)
    return data.get("pools", {})


POOLS = _load_pools()
POOL_NAMES = {key: cfg["pool_name"] for key, cfg in POOLS.items()}
EXTERNAL_POOLS = {key: cfg for key, cfg in POOLS.items() if cfg.get("type") == "external"}


def resolve_compose_path(pool_key: str) -> Path:
    """Resolve the docker-compose file for an external pool.

    Tries docker-compose.{pool_key}.yaml first, then falls back to
    docker-compose.{dir_name}.yaml for pools that share a worker directory
    (e.g., aws-backup shares workers/aws/docker-compose.aws.yaml).
    """
    cfg = EXTERNAL_POOLS[pool_key]
    dir_name = cfg.get("worker_dir", f"workers/{pool_key}").split("/")[-1]
    worker_dir = WORKERS_DIR / dir_name

    pool_specific = worker_dir / f"docker-compose.{pool_key}.yaml"
    if pool_specific.exists():
        return pool_specific

    dir_default = worker_dir / f"docker-compose.{dir_name}.yaml"
    if dir_default.exists():
        return dir_default

    # Return pool-specific path so the test gets a clear FileNotFoundError
    return pool_specific


# ---------------------------------------------------------------------------
# Canonical versions — single source of truth for all version assertions
# ---------------------------------------------------------------------------
def _read_from_line(path: Path, pattern: str) -> str:
    """Extract first capture group from a file line matching pattern."""
    for line in path.read_text().splitlines():
        m = re.search(pattern, line)
        if m:
            return m.group(1)
    raise ValueError(f"Pattern {pattern!r} not found in {path}")


# Read once at import time (all tests in a session share these)
PREFECT_TAG = _read_from_line(SPECS_DIR / "pf_server.yaml", r"prefect:([^\s\"]+)")
REDIS_TAG = _read_from_line(IMAGES_DIR / "redis" / "Dockerfile", r"^FROM redis:(\S+)")
POSTGRES_TAG = _read_from_line(IMAGES_DIR / "postgres" / "Dockerfile", r"^FROM postgres:(\S+)")
PYTHON_TAG = _read_from_line(IMAGES_DIR / "prefect" / "Dockerfile", r"^FROM python:(\S+)")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def compose_config():
    """Parse docker-compose.yaml once per session."""
    return yaml.safe_load(COMPOSE_FILE.read_text())


@pytest.fixture(scope="session")
def spec_files():
    """Return dict of spec name -> parsed YAML."""
    specs = {}
    for f in sorted(SPECS_DIR.glob("*.yaml")):
        specs[f.stem] = yaml.safe_load(f.read_text())
    return specs


@pytest.fixture(scope="session")
def sql_files():
    """Return dict of SQL filename -> content."""
    sqls = {}
    for f in sorted(SQL_DIR.glob("*.sql")) + sorted(SQL_DIR.glob("*.sql.template")):
        sqls[f.name] = f.read_text()
    return sqls


@pytest.fixture(scope="session")
def flow_files():
    """Return dict of flow filename -> content."""
    flows = {}
    for f in sorted(FLOWS_DIR.glob("*.py")):
        flows[f.name] = f.read_text()
    return flows


@pytest.fixture(scope="session")
def script_files():
    """Return dict of script filename -> content."""
    scripts = {}
    for d in [SCRIPTS_DIR]:
        for f in sorted(d.glob("*.sh")):
            scripts[f.name] = f.read_text()
    # Include worker scripts from all worker directories
    for worker_dir in sorted(WORKERS_DIR.iterdir()):
        if worker_dir.is_dir():
            for f in sorted(worker_dir.glob("*.sh")):
                scripts[f.name] = f.read_text()
    return scripts


@pytest.fixture(scope="session")
def dockerfile_contents():
    """Return dict of image name -> Dockerfile content."""
    dockerfiles = {}
    for img_dir in sorted(IMAGES_DIR.iterdir()):
        df = img_dir / "Dockerfile"
        if df.exists():
            dockerfiles[img_dir.name] = df.read_text()
    # External worker Dockerfiles (keyed as "<worker>-worker")
    for worker_dir in sorted(WORKERS_DIR.iterdir()):
        if worker_dir.is_dir():
            df = worker_dir / "Dockerfile.worker"
            if df.exists():
                dockerfiles[f"{worker_dir.name}-worker"] = df.read_text()
    return dockerfiles


# ---------------------------------------------------------------------------
# E2E — authenticated session for SPCS API requests
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spcs_session():
    """Return a requests.Session with SPCS auth headers.

    Reads SNOWFLAKE_PAT from the environment and injects the
    ``Authorization: Snowflake Token="<PAT>"`` header that the SPCS
    ingress endpoint requires.  If the PAT is not set the session is
    returned without auth headers (tests running inside the SPCS network
    may not need them).
    """
    session = requests.Session()
    pat = os.environ.get("SNOWFLAKE_PAT")
    if pat:
        session.headers["Authorization"] = f'Snowflake Token="{pat}"'
    return session
