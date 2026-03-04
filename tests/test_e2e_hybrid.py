"""E2E tests for hybrid worker orchestration.

Tests that external workers can connect to the
SPCS-hosted Prefect server and execute flows.

Run with: pytest -m e2e tests/test_e2e_hybrid.py

Tests verify infrastructure health only (pools exist, workers
online, deployments registered per pool). No flows are executed.
"""

import os

import pytest
from conftest import EXTERNAL_POOLS, POOL_NAMES

pytestmark = pytest.mark.e2e


@pytest.fixture
def spcs_api_url():
    url = os.environ.get("PREFECT_SPCS_API_URL")
    if not url:
        pytest.skip("PREFECT_SPCS_API_URL not set")
    # Ensure the URL includes the /api path — SPCS ingress rejects POST
    # requests at root-level paths but allows them under /api/.
    if not url.rstrip("/").endswith("/api"):
        url = url.rstrip("/") + "/api"
    return url


def _get_online_workers(session, api_url, pool_name):
    """Fetch workers via POST filter and return only ONLINE ones."""
    resp = session.post(
        f"{api_url}/work_pools/{pool_name}/workers/filter",
        json={},
        timeout=10,
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return [w for w in resp.json() if w["status"] == "ONLINE"]


class TestHybridWorkerPoolExists:
    @pytest.mark.parametrize("pool_key", list(POOL_NAMES.keys()))
    def test_pool_exists(self, spcs_api_url, spcs_session, pool_key):
        pool_name = POOL_NAMES[pool_key]
        resp = spcs_session.get(f"{spcs_api_url}/work_pools/{pool_name}", timeout=10)
        if resp.status_code == 404:
            pytest.skip(f"{pool_name} not created yet")
        assert resp.status_code == 200


class TestHybridWorkerOnline:
    @pytest.mark.parametrize("pool_key", list(EXTERNAL_POOLS.keys()))
    def test_external_worker_polling(self, spcs_api_url, spcs_session, pool_key):
        """Check that an external worker is actively polling."""
        pool_name = POOL_NAMES[pool_key]
        online = _get_online_workers(spcs_session, spcs_api_url, pool_name)
        if online is None:
            pytest.skip(f"{pool_name} not found")
        assert len(online) >= 1, f"No {pool_key} workers online"

    def test_spcs_worker_polling(self, spcs_api_url, spcs_session):
        """Check that the SPCS worker is actively polling."""
        pool_name = POOL_NAMES["spcs"]
        online = _get_online_workers(spcs_session, spcs_api_url, pool_name)
        if online is None:
            pytest.skip(f"{pool_name} not found")
        assert len(online) >= 1, "No SPCS workers online"


class TestHybridDeploymentsRegistered:
    @pytest.mark.parametrize("pool_key", list(EXTERNAL_POOLS.keys()))
    def test_pool_has_deployments(self, spcs_api_url, spcs_session, pool_key):
        """Each external pool should have at least one deployment targeting it."""
        pool_name = POOL_NAMES[pool_key]
        resp = spcs_session.post(
            f"{spcs_api_url}/deployments/filter",
            json={"work_pool": {"name": {"any_": [pool_name]}}},
            timeout=10,
        )
        assert resp.status_code == 200
        deployments = resp.json()
        assert len(deployments) >= 1, f"No deployments targeting {pool_name}"
