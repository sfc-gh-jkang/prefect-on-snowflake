"""Comprehensive tests for the Prefect Secret block integration.

Tests the get_secret_value() helper in shared_utils.py and its use in:
  - shared_utils.get_snowflake_connection() (PAT via Secret block)
  - hooks.py (webhook URLs via Secret blocks)

Verifies the two-layer secret architecture:
  Layer 1: Prefect Secret block (preferred, no restart needed)
  Layer 2: Environment variable fallback (works outside Prefect context)
"""

from __future__ import annotations

import importlib.util
import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

FLOWS_DIR = Path(__file__).resolve().parent.parent / "flows"
SHARED_UTILS_PATH = FLOWS_DIR / "shared_utils.py"
HOOKS_PATH = FLOWS_DIR / "hooks.py"


# ---------------------------------------------------------------------------
# Module loader helpers
# ---------------------------------------------------------------------------


def _make_mock_prefect_with_secret(secret_values: dict[str, str] | None = None):
    """Create mock prefect modules with configurable Secret.load() behavior.

    Args:
        secret_values: dict mapping block_name -> value.
            If a block_name is not in the dict, Secret.load() raises ValueError.
            If None, all Secret.load() calls raise ValueError (no blocks exist).
    """
    values = secret_values or {}

    mock_prefect = types.ModuleType("prefect")
    mock_prefect.flow = lambda **kw: lambda fn: fn
    mock_prefect.task = lambda **kw: lambda fn: fn
    mock_prefect.get_run_logger = MagicMock(return_value=MagicMock())

    mock_blocks = types.ModuleType("prefect.blocks")
    mock_system = types.ModuleType("prefect.blocks.system")

    class MockSecret:
        def __init__(self, value=""):
            self._value = value

        @classmethod
        def load(cls, name):
            if name in values:
                obj = cls(value=values[name])
                return obj
            raise ValueError(f"Block {name!r} not found")

        def get(self):
            return self._value

        def save(self, name, overwrite=False):
            pass

    mock_system.Secret = MockSecret
    mock_blocks.system = mock_system
    mock_prefect.blocks = mock_blocks

    return mock_prefect, mock_system


def _import_shared_utils_with_blocks(secret_values: dict[str, str] | None = None, **env_overrides):
    """Import shared_utils with configurable Secret blocks and env vars."""
    mock_prefect, mock_system = _make_mock_prefect_with_secret(secret_values)

    env = {
        "SNOWFLAKE_ACCOUNT": "test-account",
        "SNOWFLAKE_WAREHOUSE": "TEST_WH",
        "SNOWFLAKE_DATABASE": "TEST_DB",
        "SNOWFLAKE_SCHEMA": "TEST_SCHEMA",
        "SNOWFLAKE_HOST": "",
        "SNOWFLAKE_USER": "",
        "SNOWFLAKE_PASSWORD": "",
        "SNOWFLAKE_PAT": "",
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
                "prefect.runtime": MagicMock(),
            },
        ),
    ):
        spec = importlib.util.spec_from_file_location("shared_utils_test", SHARED_UTILS_PATH)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

    return mod


# ---------------------------------------------------------------------------
# Test: get_secret_value() — core helper
# ---------------------------------------------------------------------------


class TestGetSecretValue:
    """Test the get_secret_value() function's lookup order and fallback behavior."""

    def test_returns_block_value_when_exists(self):
        """Layer 1: Prefect Secret block should be preferred."""
        mod = _import_shared_utils_with_blocks({"my-secret": "block-value"})
        _, mock_system = _make_mock_prefect_with_secret({"my-secret": "block-value"})
        with patch.dict(
            sys.modules,
            {"prefect.blocks.system": mock_system},
        ):
            result = mod.get_secret_value("my-secret", "MY_ENV_VAR")
        assert result == "block-value"

    def test_falls_back_to_env_when_block_missing(self):
        """Layer 2: Falls back to env var when block doesn't exist."""
        mod = _import_shared_utils_with_blocks(
            secret_values={},  # No blocks
            MY_SECRET="env-value",  # pragma: allowlist secret
        )
        # Need to call with the env patched at call time too
        with patch.dict(os.environ, {"MY_SECRET": "env-value"}, clear=False):
            result = mod.get_secret_value("nonexistent-block", "MY_SECRET")
        assert result == "env-value"

    def test_returns_default_when_both_missing(self):
        """Returns default when neither block nor env var exists."""
        mod = _import_shared_utils_with_blocks(secret_values={})
        with patch.dict(os.environ, {}, clear=False):
            result = mod.get_secret_value("nonexistent", "NONEXISTENT_VAR", "fallback")
        assert result == "fallback"

    def test_returns_empty_string_default(self):
        """Default is empty string when not specified."""
        mod = _import_shared_utils_with_blocks(secret_values={})
        with patch.dict(os.environ, {}, clear=False):
            result = mod.get_secret_value("nonexistent", "NONEXISTENT_VAR")
        assert result == ""

    def test_block_takes_precedence_over_env(self):
        """Block value should win even when env var is also set."""
        mod = _import_shared_utils_with_blocks(
            {"my-secret": "from-block"},
            MY_SECRET="from-env",  # pragma: allowlist secret
        )
        _, mock_system = _make_mock_prefect_with_secret({"my-secret": "from-block"})
        with (
            patch.dict(os.environ, {"MY_SECRET": "from-env"}, clear=False),
            patch.dict(sys.modules, {"prefect.blocks.system": mock_system}),
        ):
            result = mod.get_secret_value("my-secret", "MY_SECRET")
        assert result == "from-block"

    def test_empty_block_value_falls_back_to_env(self):
        """If block exists but value is empty string, fall back to env."""
        mod = _import_shared_utils_with_blocks(
            {"my-secret": ""},
            MY_SECRET="env-val",  # pragma: allowlist secret
        )
        with patch.dict(os.environ, {"MY_SECRET": "env-val"}, clear=False):
            result = mod.get_secret_value("my-secret", "MY_SECRET")
        assert result == "env-val"

    def test_handles_block_load_exception(self):
        """Any exception during block load should fall back to env var."""
        mod = _import_shared_utils_with_blocks(secret_values={})
        # Force exception by making Secret.load raise something unexpected
        with (
            patch.dict(os.environ, {"MY_VAR": "env-fallback"}, clear=False),
        ):
            result = mod.get_secret_value("any-block", "MY_VAR")
        assert result == "env-fallback"


# ---------------------------------------------------------------------------
# Test: shared_utils.get_snowflake_connection() with Secret block PAT
# ---------------------------------------------------------------------------


class TestGetSnowflakeConnectionWithSecretBlock:
    """Test that get_snowflake_connection() uses Secret block for PAT."""

    def _make_mock_connector(self):
        mock_connector = MagicMock()
        mock_snowflake = types.ModuleType("snowflake")
        mock_snowflake.connector = mock_connector
        return mock_snowflake, mock_connector

    def test_uses_pat_from_secret_block(self):
        """When Secret block has PAT and no OAuth token file, use block PAT."""
        mod = _import_shared_utils_with_blocks(
            {"snowflake-pat": "block-pat-token"},
            SNOWFLAKE_PAT="env-pat-token",
            SNOWFLAKE_USER="test-user",
        )
        mock_sf, mock_connector = self._make_mock_connector()
        _, mock_system = _make_mock_prefect_with_secret({"snowflake-pat": "block-pat-token"})

        with (
            patch.dict(
                os.environ,
                {
                    "SNOWFLAKE_ACCOUNT": "test-account",
                    "SNOWFLAKE_PAT": "env-pat-token",
                    "SNOWFLAKE_USER": "test-user",
                    "SNOWFLAKE_WAREHOUSE": "WH",
                    "SNOWFLAKE_DATABASE": "DB",
                    "SNOWFLAKE_SCHEMA": "SCH",
                },
                clear=False,
            ),
            patch.dict(
                sys.modules,
                {
                    "snowflake": mock_sf,
                    "snowflake.connector": mock_connector,
                    "prefect.blocks.system": mock_system,
                },
            ),
            patch("os.path.isfile", return_value=False),
        ):
            mod.get_snowflake_connection()

        kwargs = mock_connector.connect.call_args[1]
        # Should use block PAT, not env PAT
        assert kwargs["password"] == "block-pat-token"  # pragma: allowlist secret

    def test_falls_back_to_env_pat_when_no_block(self):
        """When no Secret block, use SNOWFLAKE_PAT env var."""
        mod = _import_shared_utils_with_blocks(
            secret_values={},  # No blocks
            SNOWFLAKE_PAT="env-pat-only",
            SNOWFLAKE_USER="test-user",
        )
        mock_sf, mock_connector = self._make_mock_connector()

        with (
            patch.dict(
                os.environ,
                {
                    "SNOWFLAKE_ACCOUNT": "test-account",
                    "SNOWFLAKE_PAT": "env-pat-only",
                    "SNOWFLAKE_USER": "test-user",
                    "SNOWFLAKE_WAREHOUSE": "WH",
                    "SNOWFLAKE_DATABASE": "DB",
                    "SNOWFLAKE_SCHEMA": "SCH",
                },
                clear=False,
            ),
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            mod.get_snowflake_connection()

        kwargs = mock_connector.connect.call_args[1]
        assert kwargs["password"] == "env-pat-only"  # pragma: allowlist secret

    def test_oauth_still_takes_precedence(self):
        """OAuth token file should still win over both block and env PAT."""
        mod = _import_shared_utils_with_blocks(
            {"snowflake-pat": "block-pat"},
            SNOWFLAKE_PAT="env-pat",
        )
        mock_sf, mock_connector = self._make_mock_connector()

        with (
            patch.dict(
                os.environ,
                {
                    "SNOWFLAKE_ACCOUNT": "test-account",
                    "SNOWFLAKE_PAT": "env-pat",
                    "SNOWFLAKE_HOST": "host.com",
                    "SNOWFLAKE_WAREHOUSE": "WH",
                    "SNOWFLAKE_DATABASE": "DB",
                    "SNOWFLAKE_SCHEMA": "SCH",
                },
                clear=False,
            ),
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=True),
            patch("builtins.open", mock_open(read_data="oauth-token")),
        ):
            mod.get_snowflake_connection()

        kwargs = mock_connector.connect.call_args[1]
        assert kwargs["authenticator"] == "oauth"
        assert kwargs["token"] == "oauth-token"


# ---------------------------------------------------------------------------
# Test: hooks.py integration with get_secret_value
# ---------------------------------------------------------------------------


class TestHooksSecretBlockIntegration:
    """Test that hooks.py uses get_secret_value for webhook URLs."""

    @pytest.fixture(scope="class")
    def hooks_content(self):
        return HOOKS_PATH.read_text()

    def test_imports_get_secret_value(self, hooks_content):
        assert "from shared_utils import get_secret_value" in hooks_content

    def test_slack_url_uses_get_secret_value(self, hooks_content):
        assert 'get_secret_value("slack-webhook-url", "SLACK_WEBHOOK_URL")' in hooks_content

    def test_alert_url_uses_get_secret_value(self, hooks_content):
        assert 'get_secret_value("alert-webhook-url", "ALERT_WEBHOOK_URL")' in hooks_content

    def test_prefect_api_url_still_uses_env(self, hooks_content):
        """PREFECT_API_URL is not a secret — should still use os.environ."""
        assert 'os.environ.get("PREFECT_API_URL"' in hooks_content


# ---------------------------------------------------------------------------
# Test: shared_utils.py docstring mentions get_secret_value
# ---------------------------------------------------------------------------


class TestSharedUtilsDocumentation:
    """Verify shared_utils.py documents the Secret block helper."""

    @pytest.fixture(scope="class")
    def utils_content(self):
        return SHARED_UTILS_PATH.read_text()

    def test_docstring_mentions_get_secret_value(self, utils_content):
        assert "get_secret_value" in utils_content[:500]

    def test_get_secret_value_has_docstring(self, utils_content):
        # Find the function and check it has a docstring
        idx = utils_content.find("def get_secret_value(")
        assert idx > 0
        after_def = utils_content[idx : idx + 500]
        assert '"""' in after_def

    def test_docstring_explains_lookup_order(self, utils_content):
        idx = utils_content.find("def get_secret_value(")
        after_def = utils_content[idx : idx + 800]
        assert "Prefect Secret block" in after_def
        assert "Environment variable" in after_def
        assert "fallback" in after_def.lower()

    def test_get_snowflake_connection_uses_get_secret_value(self, utils_content):
        """get_snowflake_connection must use get_secret_value for PAT."""
        assert 'get_secret_value("snowflake-pat", "SNOWFLAKE_PAT")' in utils_content


# ---------------------------------------------------------------------------
# Test: Backward compatibility — existing tests must still pass patterns
# ---------------------------------------------------------------------------


class TestBackwardCompatibility:
    """Ensure the Secret block integration doesn't break existing behavior."""

    def test_no_block_no_env_returns_empty(self):
        """When no block and no env var, PAT should be empty string → tier 3 fallback."""
        mod = _import_shared_utils_with_blocks(secret_values={})
        mock_sf = types.ModuleType("snowflake")
        mock_connector = MagicMock()
        mock_sf.connector = mock_connector

        with (
            patch.dict(
                os.environ,
                {
                    "SNOWFLAKE_ACCOUNT": "test",
                    "SNOWFLAKE_PAT": "",
                    "SNOWFLAKE_USER": "user",
                    "SNOWFLAKE_PASSWORD": "pass",  # pragma: allowlist secret
                    "SNOWFLAKE_WAREHOUSE": "WH",
                    "SNOWFLAKE_DATABASE": "DB",
                    "SNOWFLAKE_SCHEMA": "SCH",
                },
                clear=False,
            ),
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            mod.get_snowflake_connection()

        kwargs = mock_connector.connect.call_args[1]
        # Should fall through to tier 3 (user/password)
        assert kwargs["user"] == "user"
        assert kwargs["password"] == "pass"

    def test_env_var_still_works_without_prefect(self):
        """When Prefect SDK not available, env var fallback must work."""
        mod = _import_shared_utils_with_blocks(
            secret_values={},
            SNOWFLAKE_PAT="env-only-pat",
            SNOWFLAKE_USER="user",
        )
        mock_sf = types.ModuleType("snowflake")
        mock_connector = MagicMock()
        mock_sf.connector = mock_connector

        # Simulate Prefect not being importable
        with (
            patch.dict(
                os.environ,
                {
                    "SNOWFLAKE_ACCOUNT": "test",
                    "SNOWFLAKE_PAT": "env-only-pat",
                    "SNOWFLAKE_USER": "user",
                    "SNOWFLAKE_WAREHOUSE": "WH",
                    "SNOWFLAKE_DATABASE": "DB",
                    "SNOWFLAKE_SCHEMA": "SCH",
                },
                clear=False,
            ),
            patch.dict(
                sys.modules,
                {
                    "snowflake": mock_sf,
                    "snowflake.connector": mock_connector,
                    # Make prefect.blocks.system raise on import
                    "prefect.blocks.system": None,
                },
            ),
            patch("os.path.isfile", return_value=False),
        ):
            mod.get_snowflake_connection()

        kwargs = mock_connector.connect.call_args[1]
        assert kwargs["password"] == "env-only-pat"  # pragma: allowlist secret

    def test_module_level_constants_unchanged(self):
        """SCHEMA, APP_VERSION, PREFECT_ENV must still be set at import time."""
        mod = _import_shared_utils_with_blocks(
            SNOWFLAKE_DATABASE="MY_DB", SNOWFLAKE_SCHEMA="MY_SCHEMA"
        )
        assert mod.SCHEMA == "MY_DB.MY_SCHEMA"
        assert hasattr(mod, "APP_VERSION")
        assert hasattr(mod, "PREFECT_ENV")

    def test_execute_query_still_works(self):
        """execute_query must still accept SQL and return (columns, rows)."""
        mod = _import_shared_utils_with_blocks(secret_values={})
        mock_sf = types.ModuleType("snowflake")
        mock_connector = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("COL",)]
        mock_cursor.fetchall.return_value = [("val",)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connector.connect.return_value = mock_conn
        mock_sf.connector = mock_connector

        with (
            patch.dict(
                os.environ,
                {
                    "SNOWFLAKE_ACCOUNT": "test",
                    "SNOWFLAKE_PAT": "",
                    "SNOWFLAKE_WAREHOUSE": "WH",
                    "SNOWFLAKE_DATABASE": "DB",
                    "SNOWFLAKE_SCHEMA": "SCH",
                },
                clear=False,
            ),
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            columns, rows = mod.execute_query("SELECT 1")

        assert columns == ["COL"]
        assert rows == [("val",)]
        mock_conn.close.assert_called_once()
