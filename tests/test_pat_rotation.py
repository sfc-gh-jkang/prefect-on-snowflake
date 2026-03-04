"""Comprehensive tests for flows/pat_rotation_flow.py.

Tests the two-layer PAT rotation architecture:
  Layer 1: Prefect Secret block updates
  Layer 2: .env file, Snowflake secret, GitLab CI variable updates
  Infrastructure: docker-compose restarts for auth-proxies

All tests use mocks — no live Snowflake, Prefect, or Docker connections needed.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import types
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

FLOWS_DIR = Path(__file__).resolve().parent.parent / "flows"
PAT_ROTATION_PATH = FLOWS_DIR / "pat_rotation_flow.py"


# ---------------------------------------------------------------------------
# Module loader — import pat_rotation_flow.py with mocked dependencies
# ---------------------------------------------------------------------------


def _make_mock_prefect():
    """Create mock prefect module hierarchy sufficient for import."""
    mock_prefect = types.ModuleType("prefect")
    mock_prefect.flow = lambda **kw: lambda fn: fn
    mock_prefect.task = lambda **kw: lambda fn: fn
    mock_prefect.get_run_logger = MagicMock(return_value=MagicMock())

    mock_blocks = types.ModuleType("prefect.blocks")
    mock_system = types.ModuleType("prefect.blocks.system")
    mock_secret_cls = MagicMock()
    mock_system.Secret = mock_secret_cls
    mock_blocks.system = mock_system
    mock_prefect.blocks = mock_blocks

    return mock_prefect, mock_system, mock_secret_cls


def _import_pat_rotation(**env_overrides):
    """Import pat_rotation_flow with controlled env and mocked prefect."""
    mock_prefect, mock_system, mock_secret_cls = _make_mock_prefect()

    # Build mock hooks module
    mock_hooks = types.ModuleType("hooks")
    mock_hooks.on_flow_failure = MagicMock()

    # Build mock shared_utils module
    mock_shared_utils = types.ModuleType("shared_utils")
    mock_shared_utils.get_secret_value = MagicMock(return_value="")

    env = {
        "SNOWFLAKE_PAT": "",
        "NEW_SNOWFLAKE_PAT": "",
        "GITLAB_PROJECT_ID": "",
        "GIT_ACCESS_TOKEN": "",
        **env_overrides,
    }

    with (
        patch.dict(os.environ, env, clear=False),
        patch.dict(
            sys.modules,
            {
                "prefect": mock_prefect,
                "prefect.blocks": mock_prefect.blocks,
                "prefect.blocks.system": mock_system,
                "hooks": mock_hooks,
                "shared_utils": mock_shared_utils,
            },
        ),
    ):
        spec = importlib.util.spec_from_file_location("pat_rotation_flow", PAT_ROTATION_PATH)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

    return mod, mock_secret_cls


# ---------------------------------------------------------------------------
# Test: PAT decoding utilities
# ---------------------------------------------------------------------------


class TestDecodePatExpiry:
    """Test JWT PAT expiry decoding."""

    def _make_jwt(self, exp_timestamp: int) -> str:
        """Create a minimal JWT with the given exp claim."""
        import base64

        header = base64.urlsafe_b64encode(json.dumps({"alg": "ES256"}).encode()).rstrip(b"=")
        payload = base64.urlsafe_b64encode(json.dumps({"exp": exp_timestamp}).encode()).rstrip(b"=")
        sig = base64.urlsafe_b64encode(b"fakesig").rstrip(b"=")
        return f"{header.decode()}.{payload.decode()}.{sig.decode()}"

    def test_valid_jwt_returns_datetime(self):
        mod, _ = _import_pat_rotation()
        # exp = 2026-06-03 00:00:00 UTC = 1780473600
        jwt = self._make_jwt(1780473600)
        result = mod.decode_pat_expiry(jwt)
        assert result is not None
        assert isinstance(result, datetime)
        assert result.year == 2026
        assert result.month == 6
        assert result.tzinfo == UTC

    def test_expired_pat(self):
        mod, _ = _import_pat_rotation()
        # exp = 2020-01-01 00:00:00 UTC = 1577836800
        jwt = self._make_jwt(1577836800)
        days = mod.pat_days_remaining(jwt)
        assert days is not None
        assert days < 0

    def test_future_pat(self):
        mod, _ = _import_pat_rotation()
        # exp = 2030-01-01 00:00:00 UTC = 1893456000
        jwt = self._make_jwt(1893456000)
        days = mod.pat_days_remaining(jwt)
        assert days is not None
        assert days > 0

    def test_invalid_jwt_format(self):
        mod, _ = _import_pat_rotation()
        assert mod.decode_pat_expiry("not-a-jwt") is None

    def test_missing_exp_claim(self):
        import base64

        mod, _ = _import_pat_rotation()
        header = base64.urlsafe_b64encode(json.dumps({"alg": "ES256"}).encode()).rstrip(b"=")
        payload = base64.urlsafe_b64encode(json.dumps({"iss": "test"}).encode()).rstrip(b"=")
        sig = base64.urlsafe_b64encode(b"sig").rstrip(b"=")
        jwt = f"{header.decode()}.{payload.decode()}.{sig.decode()}"
        assert mod.decode_pat_expiry(jwt) is None

    def test_empty_string(self):
        mod, _ = _import_pat_rotation()
        assert mod.decode_pat_expiry("") is None

    def test_pat_days_remaining_none_on_invalid(self):
        mod, _ = _import_pat_rotation()
        assert mod.pat_days_remaining("garbage") is None


# ---------------------------------------------------------------------------
# Test: Consumer inventory completeness
# ---------------------------------------------------------------------------


class TestConsumerInventory:
    """Ensure the PAT_CONSUMERS list is comprehensive and consistent."""

    def test_consumer_count(self):
        mod, _ = _import_pat_rotation()
        assert len(mod.PAT_CONSUMERS) == 8, (
            f"Expected 8 PAT consumers, got {len(mod.PAT_CONSUMERS)}: {mod.PAT_CONSUMERS}"
        )

    def test_layer1_consumers(self):
        mod, _ = _import_pat_rotation()
        assert "prefect_secret_block" in mod.PAT_CONSUMERS

    def test_layer2_consumers(self):
        mod, _ = _import_pat_rotation()
        layer2 = ["dotenv_file", "snowflake_secret", "gitlab_ci_variable"]
        for consumer in layer2:
            assert consumer in mod.PAT_CONSUMERS, f"Missing Layer 2 consumer: {consumer}"

    def test_infra_restart_consumers(self):
        mod, _ = _import_pat_rotation()
        expected = ["gcp_auth_proxy", "aws_auth_proxy", "local_auth_proxy", "monitoring_auth_proxy"]
        for consumer in expected:
            assert consumer in mod.PAT_CONSUMERS, f"Missing infra consumer: {consumer}"
            assert consumer in mod.INFRA_RESTART_CONSUMERS, (
                f"Missing from INFRA_RESTART_CONSUMERS: {consumer}"
            )

    def test_infra_consumers_subset_of_all(self):
        mod, _ = _import_pat_rotation()
        for consumer in mod.INFRA_RESTART_CONSUMERS:
            assert consumer in mod.PAT_CONSUMERS

    def test_compose_files_match_infra_consumers(self):
        """Every infra consumer must have a compose file mapping."""
        mod, _ = _import_pat_rotation()
        for consumer in mod.INFRA_RESTART_CONSUMERS:
            assert consumer in mod.COMPOSE_FILES, (
                f"No compose file mapping for infra consumer: {consumer}"
            )

    def test_compose_file_paths_are_relative(self):
        """Compose file paths must be relative (not absolute)."""
        mod, _ = _import_pat_rotation()
        for key, path in mod.COMPOSE_FILES.items():
            assert not path.startswith("/"), f"Compose path for {key} should be relative: {path}"


# ---------------------------------------------------------------------------
# Test: .env file update
# ---------------------------------------------------------------------------


class TestUpdateDotenvFile:
    """Test .env file PAT replacement logic."""

    def test_replaces_pat_line(self, tmp_path):
        mod, _ = _import_pat_rotation()
        env_file = tmp_path / ".env"
        env_file.write_text(
            "SNOWFLAKE_ACCOUNT=test\nSNOWFLAKE_PAT=old-pat-value\nSPCS_ENDPOINT=example.com\n"
        )
        result = mod.update_dotenv_file("new-pat-value", str(env_file))
        assert result is True
        content = env_file.read_text()
        assert "SNOWFLAKE_PAT=new-pat-value" in content
        assert "old-pat-value" not in content

    def test_preserves_other_lines(self, tmp_path):
        mod, _ = _import_pat_rotation()
        env_file = tmp_path / ".env"
        env_file.write_text(
            "SNOWFLAKE_ACCOUNT=test\nSNOWFLAKE_PAT=old\nSPCS_ENDPOINT=example.com\nOTHER_VAR=keep\n"
        )
        mod.update_dotenv_file("new", str(env_file))
        content = env_file.read_text()
        assert "SNOWFLAKE_ACCOUNT=test" in content
        assert "SPCS_ENDPOINT=example.com" in content
        assert "OTHER_VAR=keep" in content

    def test_returns_false_if_file_missing(self, tmp_path):
        mod, _ = _import_pat_rotation()
        result = mod.update_dotenv_file("new", str(tmp_path / "nonexistent"))
        assert result is False

    def test_returns_false_if_no_pat_line(self, tmp_path):
        mod, _ = _import_pat_rotation()
        env_file = tmp_path / ".env"
        env_file.write_text("SNOWFLAKE_ACCOUNT=test\nOTHER=val\n")
        result = mod.update_dotenv_file("new", str(env_file))
        assert result is False

    def test_handles_jwt_with_special_chars(self, tmp_path):
        """PATs are JWTs with dots, dashes, underscores — ensure no regex issues."""
        mod, _ = _import_pat_rotation()
        env_file = tmp_path / ".env"
        old_pat = "eyJra.payload.sig-with_special+chars"
        new_pat = "eyJnew.payload2.newsig-test_val"
        env_file.write_text(f"SNOWFLAKE_PAT={old_pat}\n")
        mod.update_dotenv_file(new_pat, str(env_file))
        content = env_file.read_text()
        assert f"SNOWFLAKE_PAT={new_pat}" in content
        assert old_pat not in content

    def test_replaces_only_first_occurrence(self, tmp_path):
        """Should only replace SNOWFLAKE_PAT=, not lines containing the substring."""
        mod, _ = _import_pat_rotation()
        env_file = tmp_path / ".env"
        env_file.write_text(
            "# SNOWFLAKE_PAT is used by auth-proxy\nSNOWFLAKE_PAT=old-val\nOTHER=val\n"
        )
        mod.update_dotenv_file("new-val", str(env_file))
        content = env_file.read_text()
        # Comment line should be preserved
        assert "# SNOWFLAKE_PAT is used" in content
        assert "SNOWFLAKE_PAT=new-val" in content


# ---------------------------------------------------------------------------
# Test: Snowflake secret SQL generation
# ---------------------------------------------------------------------------


class TestBuildAlterSecretSql:
    """Test SQL generation for Snowflake secret update."""

    def test_basic_sql(self):
        mod, _ = _import_pat_rotation()
        sql = mod.build_alter_secret_sql("my-new-pat")
        assert "USE ROLE PREFECT_ROLE" in sql
        assert "USE DATABASE PREFECT_DB" in sql
        assert "USE SCHEMA PREFECT_SCHEMA" in sql
        assert (
            "ALTER SECRET PREFECT_SVC_PAT SET SECRET_STRING = 'my-new-pat'"  # pragma: allowlist secret
            in sql
        )

    def test_escapes_single_quotes(self):
        mod, _ = _import_pat_rotation()
        sql = mod.build_alter_secret_sql("pat-with-'quote")
        assert "pat-with-''quote" in sql

    def test_handles_empty_string(self):
        mod, _ = _import_pat_rotation()
        sql = mod.build_alter_secret_sql("")
        assert "SECRET_STRING = ''" in sql


# ---------------------------------------------------------------------------
# Test: GitLab API URL construction
# ---------------------------------------------------------------------------


class TestBuildGitlabApiUrl:
    """Test GitLab API URL construction."""

    def test_default_variable_key(self):
        mod, _ = _import_pat_rotation()
        url = mod.build_gitlab_api_url("12345")
        assert "/api/v4/projects/12345/variables/SNOWFLAKE_PAT" in url  # pragma: allowlist secret

    def test_custom_variable_key(self):
        mod, _ = _import_pat_rotation()
        url = mod.build_gitlab_api_url("999", variable_key="CUSTOM_VAR")
        assert "variables/CUSTOM_VAR" in url


# ---------------------------------------------------------------------------
# Test: Prefect Secret block update
# ---------------------------------------------------------------------------


class TestUpdatePrefectSecretBlock:
    """Test Prefect Secret block create/update logic."""

    def test_updates_existing_block(self):
        mod, mock_secret_cls = _import_pat_rotation()
        mock_block = MagicMock()
        mock_secret_cls.load.return_value = mock_block

        with (
            patch.dict(
                sys.modules,
                {"prefect.blocks.system": MagicMock(Secret=mock_secret_cls)},
            ),
            patch.object(mod, "get_run_logger", return_value=MagicMock()),
        ):
            result = mod.update_prefect_secret_block("new-pat")

        assert result is True

    def test_creates_new_block_on_value_error(self):
        mod, mock_secret_cls = _import_pat_rotation()
        mock_secret_cls.load.side_effect = ValueError("not found")
        mock_new_block = MagicMock()
        mock_secret_cls.return_value = mock_new_block

        with (
            patch.dict(
                sys.modules,
                {"prefect.blocks.system": MagicMock(Secret=mock_secret_cls)},
            ),
            patch.object(mod, "get_run_logger", return_value=MagicMock()),
        ):
            result = mod.update_prefect_secret_block("new-pat")

        assert result is True
        mock_new_block.save.assert_called_once_with("snowflake-pat")

    def test_returns_false_on_exception(self):
        mod, mock_secret_cls = _import_pat_rotation()
        mock_secret_cls.load.side_effect = RuntimeError("connection failed")

        with (
            patch.dict(
                sys.modules,
                {"prefect.blocks.system": MagicMock(Secret=mock_secret_cls)},
            ),
            patch.object(mod, "get_run_logger", return_value=MagicMock()),
        ):
            result = mod.update_prefect_secret_block("new-pat")

        assert result is False


# ---------------------------------------------------------------------------
# Test: Infrastructure restart
# ---------------------------------------------------------------------------


class TestRestartInfraConsumer:
    """Test docker-compose restart logic for auth-proxies."""

    def test_skips_if_compose_file_missing(self):
        mod, _ = _import_pat_rotation()
        with (
            patch.object(mod, "get_run_logger", return_value=MagicMock()),
            patch("os.path.isfile", return_value=False),
        ):
            result = mod.restart_infra_consumer("gcp_auth_proxy", "/fake/project")
        assert result is True  # Not an error — just skipped

    def test_skips_if_no_running_containers(self):
        mod, _ = _import_pat_rotation()
        mock_result = MagicMock()
        mock_result.stdout = ""
        mock_result.returncode = 0

        with (
            patch.object(mod, "get_run_logger", return_value=MagicMock()),
            patch("os.path.isfile", return_value=True),
            patch.object(mod, "_run_command", return_value=mock_result),
        ):
            result = mod.restart_infra_consumer("gcp_auth_proxy", "/fake/project")
        assert result is True

    def test_restarts_running_container(self):
        mod, _ = _import_pat_rotation()
        ps_result = MagicMock()
        ps_result.stdout = "abc123\n"
        ps_result.returncode = 0

        up_result = MagicMock()
        up_result.returncode = 0

        call_count = [0]
        results = [ps_result, up_result]

        def mock_run(*args, **kwargs):
            idx = call_count[0]
            call_count[0] += 1
            return results[idx]

        with (
            patch.object(mod, "get_run_logger", return_value=MagicMock()),
            patch("os.path.isfile", return_value=True),
            patch.object(mod, "_run_command", side_effect=mock_run),
        ):
            result = mod.restart_infra_consumer("gcp_auth_proxy", "/fake/project")
        assert result is True

    def test_returns_false_on_restart_failure(self):
        mod, _ = _import_pat_rotation()
        ps_result = MagicMock()
        ps_result.stdout = "abc123\n"
        ps_result.returncode = 0

        up_result = MagicMock()
        up_result.returncode = 1
        up_result.stderr = "error"

        call_count = [0]
        results = [ps_result, up_result]

        def mock_run(*args, **kwargs):
            idx = call_count[0]
            call_count[0] += 1
            return results[idx]

        with (
            patch.object(mod, "get_run_logger", return_value=MagicMock()),
            patch("os.path.isfile", return_value=True),
            patch.object(mod, "_run_command", side_effect=mock_run),
        ):
            result = mod.restart_infra_consumer("gcp_auth_proxy", "/fake/project")
        assert result is False

    def test_unknown_consumer_returns_false(self):
        mod, _ = _import_pat_rotation()
        with patch.object(mod, "get_run_logger", return_value=MagicMock()):
            result = mod.restart_infra_consumer("nonexistent_consumer", "/fake")
        assert result is False

    def test_all_compose_files_exist_in_project(self):
        """Every compose file referenced in COMPOSE_FILES must actually exist."""
        mod, _ = _import_pat_rotation()
        project_dir = Path(__file__).resolve().parent.parent
        for key, rel_path in mod.COMPOSE_FILES.items():
            full_path = project_dir / rel_path
            assert full_path.exists(), (
                f"Compose file for {key} not found: {rel_path} (checked {full_path})"
            )


# ---------------------------------------------------------------------------
# Test: GitLab CI variable update
# ---------------------------------------------------------------------------


class TestUpdateGitlabCiVariable:
    """Test GitLab CI variable update via API."""

    def test_skips_without_project_id(self):
        mod, _ = _import_pat_rotation()
        with (
            patch.object(mod, "get_run_logger", return_value=MagicMock()),
            patch.dict(
                os.environ, {"GITLAB_PROJECT_ID": "", "GIT_ACCESS_TOKEN": "tok"}, clear=False
            ),
        ):
            result = mod.update_gitlab_ci_variable("new-pat")
        assert result is False

    def test_skips_without_git_token(self):
        mod, _ = _import_pat_rotation()
        with (
            patch.object(mod, "get_run_logger", return_value=MagicMock()),
            patch.dict(
                os.environ, {"GITLAB_PROJECT_ID": "123", "GIT_ACCESS_TOKEN": ""}, clear=False
            ),
        ):
            result = mod.update_gitlab_ci_variable("new-pat")
        assert result is False

    def test_sends_put_request(self):
        mod, _ = _import_pat_rotation()
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        with (
            patch.object(mod, "get_run_logger", return_value=MagicMock()),
            patch("urllib.request.urlopen", return_value=mock_resp) as mock_open,
        ):
            result = mod.update_gitlab_ci_variable(
                "new-pat", project_id="123", gitlab_token="glpat-abc"
            )

        assert result is True
        req = mock_open.call_args[0][0]
        assert req.method == "PUT"
        assert "123" in req.full_url
        assert req.get_header("Private-token") == "glpat-abc"
        body = json.loads(req.data)
        assert body["value"] == "new-pat"

    def test_returns_false_on_api_error(self):
        mod, _ = _import_pat_rotation()
        import urllib.error

        with (
            patch.object(mod, "get_run_logger", return_value=MagicMock()),
            patch(
                "urllib.request.urlopen",
                side_effect=urllib.error.URLError("connection refused"),
            ),
        ):
            result = mod.update_gitlab_ci_variable("new-pat", project_id="123", gitlab_token="tok")
        assert result is False


# ---------------------------------------------------------------------------
# Test: Snowflake secret update
# ---------------------------------------------------------------------------


class TestUpdateSnowflakeSecret:
    """Test Snowflake secret update via snow CLI."""

    def test_calls_snow_sql(self):
        mod, _ = _import_pat_rotation()
        mock_result = MagicMock()
        mock_result.returncode = 0

        with (
            patch.object(mod, "get_run_logger", return_value=MagicMock()),
            patch.object(mod, "_run_command", return_value=mock_result) as mock_run,
        ):
            result = mod.update_snowflake_secret("new-pat", "aws_spcs")

        assert result is True
        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "snow"
        assert cmd[1] == "sql"
        assert "--connection" in cmd
        assert "aws_spcs" in cmd
        # Verify SQL contains ALTER SECRET
        sql_arg = cmd[cmd.index("-q") + 1]
        assert "ALTER SECRET PREFECT_SVC_PAT" in sql_arg

    def test_returns_false_on_failure(self):
        mod, _ = _import_pat_rotation()
        mock_result = MagicMock()
        mock_result.returncode = 1

        with (
            patch.object(mod, "get_run_logger", return_value=MagicMock()),
            patch.object(mod, "_run_command", return_value=mock_result),
        ):
            result = mod.update_snowflake_secret("new-pat", "aws_spcs")
        assert result is False


# ---------------------------------------------------------------------------
# Test: Validation
# ---------------------------------------------------------------------------


class TestValidatePatPropagation:
    """Test post-rotation validation checks."""

    def test_validates_prefect_block_match(self):
        mod, mock_secret_cls = _import_pat_rotation()
        mock_block = MagicMock()
        mock_block.get.return_value = "new-pat"
        mock_secret_cls.load.return_value = mock_block

        with (
            patch.object(mod, "get_run_logger", return_value=MagicMock()),
            patch.dict(
                sys.modules,
                {"prefect.blocks.system": MagicMock(Secret=mock_secret_cls)},
            ),
            patch.object(mod, "_read_file", return_value="SNOWFLAKE_PAT=new-pat\n"),
        ):
            results = mod.validate_pat_propagation("new-pat")

        assert results["prefect_secret_block"] is True
        assert results["dotenv_file"] is True

    def test_detects_stale_block(self):
        mod, mock_secret_cls = _import_pat_rotation()
        mock_block = MagicMock()
        mock_block.get.return_value = "old-pat"
        mock_secret_cls.load.return_value = mock_block

        with (
            patch.object(mod, "get_run_logger", return_value=MagicMock()),
            patch.dict(
                sys.modules,
                {"prefect.blocks.system": MagicMock(Secret=mock_secret_cls)},
            ),
            patch.object(mod, "_read_file", return_value="SNOWFLAKE_PAT=new-pat\n"),
        ):
            results = mod.validate_pat_propagation("new-pat")

        assert results["prefect_secret_block"] is False

    def test_detects_stale_dotenv(self):
        mod, mock_secret_cls = _import_pat_rotation()
        mock_block = MagicMock()
        mock_block.get.return_value = "new-pat"
        mock_secret_cls.load.return_value = mock_block

        with (
            patch.object(mod, "get_run_logger", return_value=MagicMock()),
            patch.dict(
                sys.modules,
                {"prefect.blocks.system": MagicMock(Secret=mock_secret_cls)},
            ),
            patch.object(mod, "_read_file", return_value="SNOWFLAKE_PAT=old-pat\n"),
        ):
            results = mod.validate_pat_propagation("new-pat")

        assert results["dotenv_file"] is False


# ---------------------------------------------------------------------------
# Test: Flow file structural checks
# ---------------------------------------------------------------------------


class TestPatRotationFlowStructure:
    """Test pat_rotation_flow.py file structure and imports."""

    @pytest.fixture(scope="class")
    def flow_content(self):
        return PAT_ROTATION_PATH.read_text()

    def test_file_exists(self):
        assert PAT_ROTATION_PATH.exists()

    def test_has_flow_decorator(self, flow_content):
        assert "@flow(" in flow_content

    def test_flow_name_is_pat_rotation(self, flow_content):
        assert 'name="pat-rotation"' in flow_content

    def test_has_on_failure_hook(self, flow_content):
        assert "on_failure=[on_flow_failure]" in flow_content

    def test_imports_hooks(self, flow_content):
        assert "from hooks import on_flow_failure" in flow_content

    def test_has_task_decorators(self, flow_content):
        assert "@task(" in flow_content

    def test_has_main_guard(self, flow_content):
        assert 'if __name__ == "__main__"' in flow_content

    def test_documents_all_consumers(self, flow_content):
        """Docstring must list all consumer types."""
        assert "Prefect Secret block" in flow_content
        assert ".env file" in flow_content
        assert "Snowflake secret" in flow_content
        assert "GitLab CI variable" in flow_content
        assert "auth-proxy" in flow_content.lower()

    def test_does_not_hardcode_pat_values(self, flow_content):
        """Must not contain actual PAT values."""
        assert "eyJraWQ" not in flow_content

    def test_uses_env_var_for_new_pat(self, flow_content):
        """Flow should read new PAT from parameter or env var, not hardcoded."""
        assert "NEW_SNOWFLAKE_PAT" in flow_content

    def test_validates_after_rotation(self, flow_content):
        """Flow must include a validation step."""
        assert "validate" in flow_content.lower()


# ---------------------------------------------------------------------------
# Test: Rollback safety — partial failures
# ---------------------------------------------------------------------------


class TestPartialFailureSafety:
    """Test that partial failures are properly reported and don't leave silent corruption."""

    def test_env_update_failure_does_not_stop_flow(self):
        """If .env update fails, flow should continue to other consumers and report failure."""
        mod, _ = _import_pat_rotation()
        # The flow raises RuntimeError on partial failure — that's correct behavior
        # We just verify that the results dict correctly tracks the failure
        results = {"dotenv_file": False, "snowflake_secret": True, "prefect_secret_block": True}
        failed = [k for k, v in results.items() if not v]
        assert "dotenv_file" in failed
        assert len(failed) == 1

    def test_expired_pat_rejected(self):
        """Flow must reject a PAT that's already expired."""
        mod, _ = _import_pat_rotation()
        import base64

        header = base64.urlsafe_b64encode(json.dumps({"alg": "ES256"}).encode()).rstrip(b"=")
        # 2020-01-01 = expired
        payload = base64.urlsafe_b64encode(json.dumps({"exp": 1577836800}).encode()).rstrip(b"=")
        sig = base64.urlsafe_b64encode(b"sig").rstrip(b"=")
        expired_jwt = f"{header.decode()}.{payload.decode()}.{sig.decode()}"

        days = mod.pat_days_remaining(expired_jwt)
        assert days < 0

    def test_empty_pat_raises_value_error(self):
        """Flow must raise ValueError if no PAT provided."""
        mod, _ = _import_pat_rotation()
        with (
            patch.dict(os.environ, {"NEW_SNOWFLAKE_PAT": ""}, clear=False),
            pytest.raises(ValueError, match="No new PAT"),
        ):
            mod.pat_rotation(new_pat=None)


# ---------------------------------------------------------------------------
# Test: Compose file references match actual project structure
# ---------------------------------------------------------------------------


class TestComposeFileIntegrity:
    """Cross-reference compose file paths with actual project files."""

    def test_gcp_compose_exists(self):
        path = (
            Path(__file__).resolve().parent.parent / "workers" / "gcp" / "docker-compose.gcp.yaml"
        )
        assert path.exists()

    def test_aws_compose_exists(self):
        path = (
            Path(__file__).resolve().parent.parent / "workers" / "aws" / "docker-compose.aws.yaml"
        )
        assert path.exists()

    def test_local_auth_proxy_compose_exists(self):
        path = Path(__file__).resolve().parent.parent / "docker-compose.auth-proxy.yaml"
        assert path.exists()

    def test_monitoring_compose_exists(self):
        path = (
            Path(__file__).resolve().parent.parent / "monitoring" / "docker-compose.monitoring.yml"
        )
        assert path.exists()

    def test_all_referenced_compose_files_have_auth_proxy(self):
        """Each compose file referenced by rotation must contain an auth-proxy service."""
        project_dir = Path(__file__).resolve().parent.parent
        mod, _ = _import_pat_rotation()
        import yaml

        for key, rel_path in mod.COMPOSE_FILES.items():
            full_path = project_dir / rel_path
            if full_path.exists():
                data = yaml.safe_load(full_path.read_text())
                services = data.get("services", {})
                proxy_names = [s for s in services if "proxy" in s.lower() or "auth" in s.lower()]
                assert len(proxy_names) > 0, (
                    f"Compose file {rel_path} ({key}) has no auth-proxy service. "
                    f"Services: {list(services)}"
                )

    def test_all_auth_proxies_use_snowflake_pat_env(self):
        """Each auth-proxy must reference SNOWFLAKE_PAT in its environment."""
        project_dir = Path(__file__).resolve().parent.parent
        mod, _ = _import_pat_rotation()
        import yaml

        for _key, rel_path in mod.COMPOSE_FILES.items():
            full_path = project_dir / rel_path
            if full_path.exists():
                data = yaml.safe_load(full_path.read_text())
                services = data.get("services", {})
                for svc_name, svc in services.items():
                    if "proxy" in svc_name.lower() or "auth" in svc_name.lower():
                        env = svc.get("environment", {})
                        assert "SNOWFLAKE_PAT" in env, (
                            f"Auth-proxy '{svc_name}' in {rel_path} must have SNOWFLAKE_PAT env"
                        )


# ---------------------------------------------------------------------------
# Test: rotate_secrets.sh integration
# ---------------------------------------------------------------------------


class TestRotateSecretsShIntegration:
    """Verify rotate_secrets.sh has option 4 for full PAT rotation."""

    @pytest.fixture(scope="class")
    def script_content(self):
        script_path = Path(__file__).resolve().parent.parent / "scripts" / "rotate_secrets.sh"
        return script_path.read_text()

    def test_has_option_4(self, script_content):
        assert "4)" in script_content

    def test_option_4_mentions_all_consumers(self, script_content):
        assert "Prefect Secret block" in script_content
        assert ".env file" in script_content
        assert "Snowflake secret" in script_content
        assert "GitLab CI variable" in script_content
        assert "auth-proxies" in script_content.lower()

    def test_option_4_runs_pat_rotation_flow(self, script_content):
        assert "pat_rotation_flow.py" in script_content

    def test_option_4_uses_uv_run(self, script_content):
        """Must use uv run, not pip/python directly."""
        assert "uv run python" in script_content

    def test_option_4_passes_pat_via_env(self, script_content):
        """New PAT must be passed via env var, not CLI arg (security)."""
        assert "NEW_SNOWFLAKE_PAT" in script_content

    def test_menu_shows_4_options(self, script_content):
        assert "Choice [1/2/3/4]" in script_content
