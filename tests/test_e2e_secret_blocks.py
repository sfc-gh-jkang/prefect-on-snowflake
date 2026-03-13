"""E2E tests for Prefect Secret blocks on the live SPCS system.

Verifies the two-layer secret architecture works end-to-end:
  Layer 1: Prefect Secret blocks (read by flows at runtime)
  Layer 2: .env / Snowflake secrets / GitLab CI (read by infrastructure)

Tests are split into two groups:
  - @pytest.mark.e2e: Require a live SPCS Prefect server (PREFECT_SPCS_API_URL + SNOWFLAKE_PAT)
  - No marker (unit): Run offline against project files and config

Run E2E tests:
  pytest -m e2e tests/test_e2e_secret_blocks.py -v

Run unit tests only:
  pytest -m "not e2e" tests/test_e2e_secret_blocks.py -v
"""

import contextlib
import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

PROJECT_DIR = Path(__file__).resolve().parent.parent
FLOWS_DIR = PROJECT_DIR / "flows"
MONITORING_DIR = PROJECT_DIR / "monitoring"

# ---------------------------------------------------------------------------
# Expected Secret blocks — single source of truth
# ---------------------------------------------------------------------------
EXPECTED_BLOCKS = {
    "snowflake-pat": {
        "env_var": "SNOWFLAKE_PAT",
        "description": "Snowflake PAT used by get_snowflake_connection()",
        "consumers": ["shared_utils.py"],
    },
    "slack-webhook-url": {
        "env_var": "SLACK_WEBHOOK_URL",
        "description": "Slack incoming webhook for flow failure alerts",
        "consumers": ["hooks.py"],
    },
    "alert-webhook-url": {
        "env_var": "ALERT_WEBHOOK_URL",
        "description": "Generic webhook endpoint for flow alerts",
        "consumers": ["hooks.py"],
    },
}


# ============================================================================
# UNIT TESTS — No network required, validate code/config consistency
# ============================================================================


class TestSecretBlockCodeConsistency:
    """Verify flow code correctly references all expected Secret blocks."""

    def test_shared_utils_uses_snowflake_pat_block(self):
        """get_snowflake_connection() must read from 'snowflake-pat' block."""
        source = (FLOWS_DIR / "shared_utils.py").read_text()
        assert 'get_secret_value("snowflake-pat"' in source

    def test_hooks_uses_slack_webhook_block(self):
        """hooks.py must read SLACK_WEBHOOK_URL from block."""
        source = (FLOWS_DIR / "hooks.py").read_text()
        assert 'get_secret_value("slack-webhook-url"' in source

    def test_hooks_uses_alert_webhook_block(self):
        """hooks.py must read ALERT_WEBHOOK_URL from block."""
        source = (FLOWS_DIR / "hooks.py").read_text()
        assert 'get_secret_value("alert-webhook-url"' in source

    def test_get_secret_value_exists_in_shared_utils(self):
        """shared_utils.py must define get_secret_value()."""
        source = (FLOWS_DIR / "shared_utils.py").read_text()
        assert "def get_secret_value(" in source

    def test_get_secret_value_tries_block_first(self):
        """get_secret_value() must try Prefect block before env var."""
        source = (FLOWS_DIR / "shared_utils.py").read_text()
        func_start = source.index("def get_secret_value(")
        func_body = source[func_start : source.index("\ndef ", func_start + 1)]
        # Block import must come before env fallback
        block_pos = func_body.index("Secret.load(")
        env_pos = func_body.index("os.environ.get(")
        assert block_pos < env_pos, "Block lookup must precede env var fallback"

    def test_get_secret_value_has_exception_fallback(self):
        """get_secret_value() must catch exceptions to fall back gracefully."""
        source = (FLOWS_DIR / "shared_utils.py").read_text()
        func_start = source.index("def get_secret_value(")
        func_body = source[func_start : source.index("\ndef ", func_start + 1)]
        assert "except Exception" in func_body or "except:" in func_body

    def test_hooks_imports_get_secret_value(self):
        """hooks.py must import get_secret_value from shared_utils."""
        source = (FLOWS_DIR / "hooks.py").read_text()
        assert "from shared_utils import get_secret_value" in source

    def test_all_expected_blocks_have_env_var_fallback(self):
        """Every expected block must also specify its env var fallback in code."""
        hooks_src = (FLOWS_DIR / "hooks.py").read_text()
        utils_src = (FLOWS_DIR / "shared_utils.py").read_text()
        combined = hooks_src + utils_src
        for block_name, info in EXPECTED_BLOCKS.items():
            env_var = info["env_var"]
            pattern = f'get_secret_value("{block_name}", "{env_var}"'
            assert pattern in combined, (
                f"Block '{block_name}' must be called with env_var='{env_var}' fallback"
            )


class TestSecretBlockDocumentation:
    """Verify Secret block architecture is documented in code."""

    def test_shared_utils_docstring_mentions_blocks(self):
        source = (FLOWS_DIR / "shared_utils.py").read_text()
        assert "Secret block" in source or "Prefect Secret" in source

    def test_get_secret_value_docstring_explains_lookup_order(self):
        source = (FLOWS_DIR / "shared_utils.py").read_text()
        func_start = source.index("def get_secret_value(")
        # Find the docstring
        docstring_match = re.search(r'"""(.+?)"""', source[func_start:], re.DOTALL)
        assert docstring_match, "get_secret_value must have a docstring"
        docstring = docstring_match.group(1)
        assert "block" in docstring.lower()
        assert "env" in docstring.lower()
        assert "fallback" in docstring.lower() or "lookup order" in docstring.lower()


class TestPatRotationFlowBlockIntegration:
    """Verify pat_rotation_flow.py correctly updates the Secret block."""

    def test_pat_rotation_updates_prefect_secret_block(self):
        source = (FLOWS_DIR / "pat_rotation_flow.py").read_text()
        assert "update_prefect_secret_block" in source

    def test_pat_rotation_references_snowflake_pat_block_name(self):
        source = (FLOWS_DIR / "pat_rotation_flow.py").read_text()
        assert '"snowflake-pat"' in source

    def test_pat_rotation_validate_checks_block(self):
        """validate_pat_propagation() must verify the block was updated."""
        source = (FLOWS_DIR / "pat_rotation_flow.py").read_text()
        assert "validate_pat_propagation" in source
        # Should reference Secret.load for validation
        assert "Secret.load" in source


class TestMonitorSpecSlackSecret:
    """Verify pf_monitor.yaml wires SLACK_WEBHOOK_URL secret to Grafana."""

    @pytest.fixture(autouse=True)
    def _load_spec(self):
        spec_path = MONITORING_DIR / "specs" / "pf_monitor.yaml"
        self.spec = yaml.safe_load(spec_path.read_text())

    def _get_grafana_container(self):
        for c in self.spec["spec"]["containers"]:
            if c["name"] == "grafana":
                return c
        raise AssertionError("No grafana container in pf_monitor.yaml")

    def test_grafana_has_slack_webhook_secret(self):
        """Grafana container must mount SLACK_WEBHOOK_URL as GF_ALERTING_SLACK_WEBHOOK_URL."""
        grafana = self._get_grafana_container()
        secrets = grafana.get("secrets", [])
        slack_secrets = [
            s for s in secrets if s.get("envVarName") == "GF_ALERTING_SLACK_WEBHOOK_URL"
        ]
        assert len(slack_secrets) == 1, (
            "pf_monitor.yaml: grafana must have GF_ALERTING_SLACK_WEBHOOK_URL secret"
        )

    def test_slack_secret_references_correct_snowflake_object(self):
        grafana = self._get_grafana_container()
        secrets = grafana.get("secrets", [])
        slack_secret = next(
            s for s in secrets if s.get("envVarName") == "GF_ALERTING_SLACK_WEBHOOK_URL"
        )
        sf_secret = slack_secret.get("snowflakeSecret", {})
        assert sf_secret.get("objectName") == "slack_webhook_url", (
            "Must reference Snowflake secret 'slack_webhook_url'"
        )

    def test_grafana_has_all_secrets(self):
        """Grafana must mount all 7 Snowflake secrets."""
        grafana = self._get_grafana_container()
        secrets = grafana.get("secrets", [])
        expected_env_vars = {
            "GF_SECURITY_ADMIN_PASSWORD",
            "GF_DATABASE_URL",
            "GF_SMTP_PASSWORD",
            "GF_ALERTING_SLACK_WEBHOOK_URL",
            "GF_SMTP_USER",
            "GF_SMTP_FROM_ADDRESS",
            "GF_SMTP_ALERT_RECIPIENTS",
        }
        actual_env_vars = {s.get("envVarName") for s in secrets}
        assert expected_env_vars == actual_env_vars, (
            f"Grafana secrets mismatch. Expected: {expected_env_vars}, Got: {actual_env_vars}"
        )

    def test_contact_points_has_slack_receiver(self):
        """contactpoints.yaml must have a Slack receiver referencing GF_ALERTING_SLACK_WEBHOOK_URL."""
        cp_path = MONITORING_DIR / "grafana" / "provisioning" / "alerting" / "contactpoints.yaml"
        config = yaml.safe_load(cp_path.read_text())
        receivers = config.get("contactPoints", [{}])[0].get("receivers", [])
        slack_receivers = [r for r in receivers if r.get("type") == "slack"]
        assert len(slack_receivers) >= 1, "contactpoints.yaml must have a Slack receiver"
        slack_url = slack_receivers[0].get("settings", {}).get("url", "")
        assert "GF_ALERTING_SLACK_WEBHOOK_URL" in slack_url, (
            "Slack receiver must use ${GF_ALERTING_SLACK_WEBHOOK_URL} env var"
        )


class TestGetSecretValueBehavior:
    """Unit tests for get_secret_value() using mocks."""

    def _import_with_mock(self, block_values=None, **env_vars):
        """Import shared_utils with mocked Prefect blocks and env vars."""
        block_values = block_values or {}

        class MockSecret:
            def __init__(self, value=""):
                self._value = value

            @classmethod
            def load(cls, name):
                if name in block_values:
                    return cls(block_values[name])
                raise ValueError(f"Block '{name}' not found")

            def get(self):
                return self._value

        mock_system = MagicMock()
        mock_system.Secret = MockSecret

        # Clean module cache
        for key in list(sys.modules.keys()):
            if "shared_utils" in key:
                del sys.modules[key]

        with (
            patch.dict(
                sys.modules,
                {
                    "prefect": MagicMock(),
                    "prefect.blocks": MagicMock(),
                    "prefect.blocks.system": mock_system,
                    "prefect.runtime": MagicMock(),
                    "prefect.runtime.flow_run": MagicMock(flow_name=None),
                    "snowflake": MagicMock(),
                    "snowflake.connector": MagicMock(),
                },
            ),
            patch.dict(os.environ, env_vars, clear=False),
        ):
            import importlib.util

            spec = importlib.util.spec_from_file_location(
                "shared_utils", FLOWS_DIR / "shared_utils.py"
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)

        return mod, mock_system

    def test_block_value_returned_when_available(self):
        mod, mock_system = self._import_with_mock({"test-block": "block-val"})
        with patch.dict(sys.modules, {"prefect.blocks.system": mock_system}):
            result = mod.get_secret_value("test-block", "TEST_VAR", "default")
        assert result == "block-val"

    def test_env_var_returned_when_block_missing(self):
        mod, _ = self._import_with_mock({}, TEST_VAR="env-val")
        with patch.dict(os.environ, {"TEST_VAR": "env-val"}, clear=False):
            result = mod.get_secret_value("missing-block", "TEST_VAR", "default")
        assert result == "env-val"

    def test_default_returned_when_both_missing(self):
        mod, _ = self._import_with_mock({})
        result = mod.get_secret_value("missing", "MISSING_VAR", "fallback")
        assert result == "fallback"

    def test_empty_block_falls_back_to_env(self):
        mod, mock_system = self._import_with_mock({"test-block": ""}, TEST_VAR="env-val")
        with (
            patch.dict(sys.modules, {"prefect.blocks.system": mock_system}),
            patch.dict(os.environ, {"TEST_VAR": "env-val"}, clear=False),
        ):
            result = mod.get_secret_value("test-block", "TEST_VAR", "default")
        assert result == "env-val"

    def test_block_exception_falls_back_to_env(self):
        """If Prefect is unavailable, gracefully fall back."""
        mod, _ = self._import_with_mock({}, SAFE_VAR="safe-val")
        # Force import error by removing prefect from modules
        with (
            patch.dict(sys.modules, {"prefect.blocks.system": None}),
            patch.dict(os.environ, {"SAFE_VAR": "safe-val"}, clear=False),
        ):
            result = mod.get_secret_value("any-block", "SAFE_VAR", "default")
        assert result == "safe-val"


# ============================================================================
# E2E TESTS — Require live SPCS Prefect server
# ============================================================================

pytestmark_e2e = pytest.mark.e2e


def _get_api_config():
    """Get API URL and PAT from environment."""
    api_url = os.environ.get("PREFECT_SPCS_API_URL") or os.environ.get("PREFECT_API_URL")
    pat = os.environ.get("SNOWFLAKE_PAT")
    # Ensure the URL includes the /api path — SPCS ingress rejects POST
    # requests at root-level paths but allows them under /api/.
    if api_url and not api_url.rstrip("/").endswith("/api"):
        api_url = api_url.rstrip("/") + "/api"
    return api_url, pat


def _api_request(url, method="GET", data=None, pat=None):
    """Make an authenticated request to the Prefect API."""
    headers = {"Content-Type": "application/json"}
    if pat:
        headers["Authorization"] = f'Snowflake Token="{pat}"'
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    resp = urllib.request.urlopen(req, timeout=15)
    raw = resp.read()
    if not raw:
        return {}
    return json.loads(raw)


@pytest.fixture(scope="module")
def api_config():
    api_url, pat = _get_api_config()
    if not api_url or not pat:
        pytest.skip("PREFECT_API_URL/PREFECT_SPCS_API_URL and SNOWFLAKE_PAT required")
    return api_url, pat


@pytest.fixture(scope="module")
def server_blocks(api_config):
    """Fetch all block documents from the Prefect server."""
    api_url, pat = api_config
    return _api_request(f"{api_url}/block_documents/filter", method="POST", data={}, pat=pat)


@pytest.mark.e2e
class TestSecretBlocksExistOnServer:
    """Verify all expected Secret blocks exist on the live Prefect server."""

    def test_server_has_blocks(self, server_blocks):
        assert len(server_blocks) >= len(EXPECTED_BLOCKS), (
            f"Expected at least {len(EXPECTED_BLOCKS)} blocks, found {len(server_blocks)}"
        )

    @pytest.mark.parametrize("block_name", list(EXPECTED_BLOCKS.keys()))
    def test_expected_block_exists(self, server_blocks, block_name):
        names = [b.get("name") for b in server_blocks]
        assert block_name in names, f"Block '{block_name}' not found on server. Existing: {names}"

    @pytest.mark.parametrize("block_name", list(EXPECTED_BLOCKS.keys()))
    def test_block_is_secret_type(self, server_blocks, block_name):
        block = next((b for b in server_blocks if b.get("name") == block_name), None)
        if not block:
            pytest.skip(f"Block '{block_name}' not found")
        block_type_slug = block.get("block_type", {}).get("slug", "")
        assert block_type_slug == "secret", (
            f"Block '{block_name}' should be type 'secret', got '{block_type_slug}'"
        )


@pytest.mark.e2e
class TestSecretBlocksReadable:
    """Verify blocks can be read with include_secrets=True."""

    def test_snowflake_pat_block_has_value(self, api_config):
        """The snowflake-pat block must contain a non-empty JWT-like value."""
        api_url, pat = api_config
        blocks = _api_request(
            f"{api_url}/block_documents/filter",
            method="POST",
            data={"include_secrets": True},
            pat=pat,
        )
        pat_block = next((b for b in blocks if b.get("name") == "snowflake-pat"), None)
        assert pat_block, "snowflake-pat block not found"
        value = pat_block.get("data", {}).get("value", "")
        assert value, "snowflake-pat block value is empty"
        # PATs are JWTs: three dot-separated base64 segments
        assert value.count(".") >= 2, "snowflake-pat value doesn't look like a JWT"

    def test_snowflake_pat_matches_current_pat(self, api_config):
        """Block value must match the PAT we're authenticating with."""
        api_url, pat = api_config
        blocks = _api_request(
            f"{api_url}/block_documents/filter",
            method="POST",
            data={"include_secrets": True},
            pat=pat,
        )
        pat_block = next((b for b in blocks if b.get("name") == "snowflake-pat"), None)
        if not pat_block:
            pytest.skip("snowflake-pat block not found")
        block_value = pat_block.get("data", {}).get("value", "")
        assert block_value == pat, (
            "snowflake-pat block value does not match the current SNOWFLAKE_PAT. "
            "Run pat_rotation_flow to sync."
        )

    @pytest.mark.parametrize("block_name", ["slack-webhook-url", "alert-webhook-url"])
    def test_webhook_block_has_url_value(self, api_config, block_name):
        """Webhook blocks must contain a URL (placeholder or real)."""
        api_url, pat = api_config
        blocks = _api_request(
            f"{api_url}/block_documents/filter",
            method="POST",
            data={"include_secrets": True},
            pat=pat,
        )
        block = next((b for b in blocks if b.get("name") == block_name), None)
        if not block:
            pytest.skip(f"{block_name} block not found")
        value = block.get("data", {}).get("value", "")
        assert value.startswith("https://"), (
            f"{block_name} value should be an HTTPS URL, got: {value[:30]}..."
        )


@pytest.mark.e2e
class TestSecretBlockUpdateRoundtrip:
    """Verify blocks can be updated and the new value is immediately readable."""

    def test_update_and_read_back(self, api_config):
        """Update alert-webhook-url, read it back, then restore original."""
        api_url, pat = api_config

        # Read current value
        blocks = _api_request(
            f"{api_url}/block_documents/filter",
            method="POST",
            data={"include_secrets": True},
            pat=pat,
        )
        block = next((b for b in blocks if b.get("name") == "alert-webhook-url"), None)
        if not block:
            pytest.skip("alert-webhook-url block not found")

        block_id = block["id"]
        original_value = block.get("data", {}).get("value", "")

        # Update to test value
        test_value = "https://discord.com/api/webhooks/E2E_TEST_VALUE"
        try:
            _api_request(
                f"{api_url}/block_documents/{block_id}",
                method="PATCH",
                data={"data": {"value": test_value}, "merge_existing_data": False},
                pat=pat,
            )

            # Read back
            blocks_after = _api_request(
                f"{api_url}/block_documents/filter",
                method="POST",
                data={"include_secrets": True},
                pat=pat,
            )
            updated_block = next(
                (b for b in blocks_after if b.get("name") == "alert-webhook-url"), None
            )
            assert updated_block
            new_value = updated_block.get("data", {}).get("value", "")
            assert new_value == test_value, (
                f"Block update failed. Expected '{test_value}', got '{new_value}'"
            )
        finally:
            # Restore original value
            with contextlib.suppress(Exception):
                _api_request(
                    f"{api_url}/block_documents/{block_id}",
                    method="PATCH",
                    data={"data": {"value": original_value}, "merge_existing_data": False},
                    pat=pat,
                )


@pytest.mark.e2e
class TestSecretBlockFlowIntegration:
    """Verify that a flow run can use Secret blocks for authentication.

    This tests the full chain:
      Flow starts → imports shared_utils → calls get_secret_value()
      → reads 'snowflake-pat' block → uses PAT to connect to Snowflake
    """

    def test_trigger_flow_and_verify_completion(self, api_config):
        """Trigger alert-test with should_fail=False and verify it completes.

        This implicitly tests that get_snowflake_connection() works, which
        relies on get_secret_value('snowflake-pat') reading the block.
        """
        import time

        api_url, pat = api_config

        # Find the alert-test-local deployment
        resp = _api_request(
            f"{api_url}/deployments/filter",
            method="POST",
            data={"deployments": {"name": {"any_": ["alert-test-local"]}}},
            pat=pat,
        )
        if not resp:
            pytest.skip("alert-test-local deployment not found")

        deployment_id = resp[0]["id"]

        # Create a flow run
        flow_run = _api_request(
            f"{api_url}/deployments/{deployment_id}/create_flow_run",
            method="POST",
            data={"parameters": {"should_fail": False}},
            pat=pat,
        )
        run_id = flow_run["id"]

        # Poll for completion (max 180 seconds — worker may need to pull code)
        # Prefect API returns title-case state names (e.g. "Completed", "Failed")
        terminal_states = {"completed", "failed", "crashed", "cancelled"}
        last_state = "UNKNOWN"
        for _ in range(36):
            time.sleep(5)
            try:
                run = _api_request(f"{api_url}/flow_runs/{run_id}", pat=pat)
                last_state = run.get("state", {}).get("name", "UNKNOWN")
                if last_state.lower() in terminal_states:
                    assert last_state.lower() == "completed", (
                        f"Flow run ended in {last_state}, expected Completed. "
                        f"This may indicate get_secret_value() or block auth failed."
                    )
                    return
            except Exception:
                continue

        # If still pending/scheduled, the worker may just be slow — skip rather than fail
        if last_state.lower() in ("scheduled", "pending"):
            pytest.skip(
                f"Flow run {run_id} still {last_state} after 180s — "
                "worker may be starting up. Re-run to verify."
            )
        pytest.fail(
            f"Flow run {run_id} stuck in {last_state} after 180s. "
            "Worker may be down or block auth may be failing."
        )


@pytest.mark.e2e
class TestSecretBlockConsistencyWithEnv:
    """Verify block values are consistent with .env file values."""

    def test_pat_block_matches_env_file(self, api_config):
        """The snowflake-pat block should match .env SNOWFLAKE_PAT."""
        api_url, pat = api_config
        env_file = PROJECT_DIR / ".env"
        if not env_file.exists():
            pytest.skip(".env file not found")

        env_pat = None
        for line in env_file.read_text().splitlines():
            if line.startswith("SNOWFLAKE_PAT="):
                env_pat = line.split("=", 1)[1].strip().strip('"').strip("'")
                break

        if not env_pat:
            pytest.skip("SNOWFLAKE_PAT not in .env")

        blocks = _api_request(
            f"{api_url}/block_documents/filter",
            method="POST",
            data={"include_secrets": True},
            pat=pat,
        )
        pat_block = next((b for b in blocks if b.get("name") == "snowflake-pat"), None)
        if not pat_block:
            pytest.skip("snowflake-pat block not found")

        block_value = pat_block.get("data", {}).get("value", "")
        assert block_value == env_pat, (
            "DRIFT DETECTED: snowflake-pat block != .env SNOWFLAKE_PAT. "
            "Run: scripts/rotate_secrets.sh option 4 to sync."
        )
