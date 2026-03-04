"""Local integration tests using docker compose.

These tests bring up the local Prefect stack and validate
health, flow execution, and teardown. Run with: pytest -m local
"""

import subprocess

import pytest

pytestmark = pytest.mark.local

COMPOSE_CMD = ["docker", "compose", "-f", "docker-compose.yaml"]
BASE_URL = "http://localhost:4200/api"


@pytest.fixture(scope="module", autouse=True)
def compose_stack(tmp_path_factory):
    """Start compose stack, yield, then tear down."""
    import os

    cwd = os.path.join(os.path.dirname(__file__), "..")
    try:
        subprocess.run(
            COMPOSE_CMD + ["up", "-d", "--wait"],
            cwd=cwd,
            check=True,
            timeout=120,
        )
        yield
    finally:
        subprocess.run(
            COMPOSE_CMD + ["down", "-v"],
            cwd=cwd,
            check=False,
            timeout=60,
        )


class TestLocalStackHealth:
    def test_server_health_endpoint(self):
        import requests

        resp = requests.get(f"{BASE_URL}/health", timeout=10)
        assert resp.status_code == 200

    def test_server_version(self):
        import requests

        resp = requests.get(f"{BASE_URL}/admin/version", timeout=10)
        assert resp.status_code == 200


class TestLocalFlowRun:
    def test_can_create_work_pool(self):
        import requests

        resp = requests.post(
            f"{BASE_URL}/work_pools/",
            json={"name": "test-pool", "type": "process"},
            timeout=10,
        )
        assert resp.status_code in (200, 201, 409)  # 409 = already exists
