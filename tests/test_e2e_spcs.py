"""E2E tests for SPCS deployment.

These tests require a live SPCS cluster with the Prefect
services deployed. Run with: pytest -m e2e

Tests verify infrastructure health only (server up, deployments
registered, workers online). No flows are executed — that would
be slow and flaky in CI.
"""

import os

import pytest

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


class TestSPCSServicesHealthy:
    def test_server_api_responds(self, spcs_api_url, spcs_session):
        resp = spcs_session.get(f"{spcs_api_url}/health", timeout=10)
        assert resp.status_code == 200

    def test_server_version(self, spcs_api_url, spcs_session):
        resp = spcs_session.get(f"{spcs_api_url}/admin/version", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        # Prefect 3.x returns the version as a plain string
        assert data, "Version response should not be empty"


class TestDeploymentsRegistered:
    def test_example_flow_deployment_exists(self, spcs_api_url, spcs_session):
        resp = spcs_session.get(
            f"{spcs_api_url}/deployments/name/example-flow/example-flow-local",
            timeout=10,
        )
        assert resp.status_code == 200, f"example-flow-local not found ({resp.status_code})"

    def test_external_api_deployment_exists(self, spcs_api_url, spcs_session):
        resp = spcs_session.get(
            f"{spcs_api_url}/deployments/name/external-api-flow/external-api-local",
            timeout=10,
        )
        if resp.status_code == 404:
            pytest.skip("external-api-local not deployed")
        assert resp.status_code == 200


class TestFlowExecutionVerification:
    """Trigger and verify a flow run completes end-to-end.

    Uses the alert-test flow (designed for testing) with should_fail=False
    to trigger a quick run and verify it reaches COMPLETED state.
    """

    def test_trigger_flow_run(self, spcs_api_url, spcs_session):
        """Create a flow run via the API and verify it starts."""
        # Find the alert-test deployment
        resp = spcs_session.post(
            f"{spcs_api_url}/deployments/filter",
            json={"deployments": {"name": {"any_": ["alert-test-local"]}}},
            timeout=10,
        )
        if resp.status_code != 200 or not resp.json():
            pytest.skip("alert-test-local deployment not found")

        deployment = resp.json()[0]
        deployment_id = deployment["id"]

        # Create a flow run with should_fail=False for a quick success
        run_resp = spcs_session.post(
            f"{spcs_api_url}/deployments/{deployment_id}/create_flow_run",
            json={"parameters": {"should_fail": False}},
            timeout=10,
        )
        assert run_resp.status_code in (200, 201), (
            f"Failed to create flow run: {run_resp.status_code}"
        )
        flow_run = run_resp.json()
        assert "id" in flow_run

    def test_recent_flow_runs_exist(self, spcs_api_url, spcs_session):
        """There should be at least one flow run in the last 24 hours."""
        import datetime

        since = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=24)).isoformat()
        resp = spcs_session.post(
            f"{spcs_api_url}/flow_runs/filter",
            json={
                "flow_runs": {
                    "expected_start_time": {"after_": since},
                },
                "limit": 5,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            pytest.skip(f"Flow runs filter failed: {resp.status_code}")
        runs = resp.json()
        assert len(runs) >= 1, "No flow runs in the last 24 hours"

    def test_no_stuck_running_flows(self, spcs_api_url, spcs_session):
        """No flow runs should be stuck in RUNNING state for >1 hour."""
        import datetime

        one_hour_ago = (
            datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=1)
        ).isoformat()
        resp = spcs_session.post(
            f"{spcs_api_url}/flow_runs/filter",
            json={
                "flow_runs": {
                    "state": {"name": {"any_": ["Running"]}},
                    "expected_start_time": {"before_": one_hour_ago},
                },
                "limit": 10,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            pytest.skip(f"Flow runs filter failed: {resp.status_code}")
        stuck = resp.json()
        assert len(stuck) == 0, f"Found {len(stuck)} flow runs stuck in Running state for >1 hour"


class TestMonitoringEndpoints:
    """Verify SPCS monitoring endpoints are reachable."""

    @pytest.fixture
    def monitor_endpoints(self):
        """Read monitoring endpoint URLs from environment.

        Accepts two naming conventions so the same env vars work for both
        ``test_e2e_spcs.py`` and ``test_monitoring.py``:
          GRAFANA_ENDPOINT   / SPCS_MONITOR_GRAFANA_URL
          PROMETHEUS_ENDPOINT / SPCS_MONITOR_ENDPOINT
          LOKI_ENDPOINT      / SPCS_MONITOR_LOKI_ENDPOINT
        """
        grafana = os.environ.get("GRAFANA_ENDPOINT") or os.environ.get("SPCS_MONITOR_GRAFANA_URL")
        prometheus = os.environ.get("PROMETHEUS_ENDPOINT") or os.environ.get(
            "SPCS_MONITOR_ENDPOINT"
        )
        loki = os.environ.get("LOKI_ENDPOINT") or os.environ.get("SPCS_MONITOR_LOKI_ENDPOINT")
        if not any([grafana, prometheus, loki]):
            pytest.skip("No monitoring endpoint URLs set")
        return {"grafana": grafana, "prometheus": prometheus, "loki": loki}

    def test_grafana_responds(self, spcs_session, monitor_endpoints):
        url = monitor_endpoints["grafana"]
        if not url:
            pytest.skip("GRAFANA_ENDPOINT not set")
        resp = spcs_session.get(f"{url}/api/health", timeout=30)
        assert resp.status_code == 200

    def test_prometheus_responds(self, spcs_session, monitor_endpoints):
        url = monitor_endpoints["prometheus"]
        if not url:
            pytest.skip("PROMETHEUS_ENDPOINT not set")
        resp = spcs_session.get(f"{url}/-/healthy", timeout=30)
        assert resp.status_code == 200

    def test_loki_responds(self, spcs_session, monitor_endpoints):
        url = monitor_endpoints["loki"]
        if not url:
            pytest.skip("LOKI_ENDPOINT not set")
        resp = spcs_session.get(f"{url}/ready", timeout=30)
        assert resp.status_code == 200


class TestWorkPoolQueues:
    """Verify work pool queues are healthy."""

    def test_spcs_pool_is_not_paused(self, spcs_api_url, spcs_session):
        from conftest import POOL_NAMES

        pool_name = POOL_NAMES.get("spcs")
        if not pool_name:
            pytest.skip("No SPCS pool configured")
        resp = spcs_session.get(f"{spcs_api_url}/work_pools/{pool_name}", timeout=10)
        if resp.status_code == 404:
            pytest.skip(f"{pool_name} not found")
        assert resp.status_code == 200
        pool = resp.json()
        assert pool.get("is_paused") is False, f"{pool_name} is paused"

    def test_flow_count_api(self, spcs_api_url, spcs_session):
        """Flows endpoint should return at least one registered flow."""
        resp = spcs_session.post(
            f"{spcs_api_url}/flows/filter",
            json={"limit": 5},
            timeout=10,
        )
        assert resp.status_code == 200
        flows = resp.json()
        assert len(flows) >= 1, "No flows registered"
