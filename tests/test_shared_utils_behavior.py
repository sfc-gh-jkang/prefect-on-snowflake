"""Behavioral tests for flows/shared_utils.py.

Tests the 3-tier auth branching in get_snowflake_connection(),
execute_query()/execute_ddl() cursor lifecycle, and _resolve_query_tag()
with Prefect runtime mocking — all without live Snowflake connections.

Note: snowflake-connector-python is NOT installed locally (only in Docker).
We inject a mock snowflake.connector module into sys.modules so the
`import snowflake.connector` inside get_snowflake_connection() succeeds.
"""

import importlib.util
import os
import sys
import types
from unittest.mock import MagicMock, mock_open, patch

import pytest
from conftest import FLOWS_DIR

SHARED_UTILS_PATH = FLOWS_DIR / "shared_utils.py"


def _make_mock_snowflake_connector():
    """Create a mock snowflake + snowflake.connector module hierarchy."""
    mock_connector = MagicMock()
    mock_snowflake = types.ModuleType("snowflake")
    mock_snowflake.connector = mock_connector
    return mock_snowflake, mock_connector


def _import_shared_utils(**env_overrides) -> types.ModuleType:
    """Import shared_utils with controlled env vars, returning a fresh module.

    Note: This controls env vars at import time (for module-level constants).
    get_snowflake_connection() reads env vars at call time, so tests that
    check connection parameters must also patch env during the call.
    """
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
    with patch.dict(os.environ, env, clear=False):
        spec = importlib.util.spec_from_file_location("shared_utils_test", SHARED_UTILS_PATH)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod


# Default env for connection tests — used in both import and call
_CONN_ENV = {
    "SNOWFLAKE_ACCOUNT": "test-account",
    "SNOWFLAKE_WAREHOUSE": "TEST_WH",
    "SNOWFLAKE_DATABASE": "TEST_DB",
    "SNOWFLAKE_SCHEMA": "TEST_SCHEMA",
    "SNOWFLAKE_HOST": "",
    "SNOWFLAKE_USER": "",
    "SNOWFLAKE_PASSWORD": "",
    "SNOWFLAKE_PAT": "",
}


# ===================================================================
# get_snowflake_connection — 3-tier auth
# ===================================================================
class TestGetSnowflakeConnectionAuth:
    """Test the 3-tier auth strategy in get_snowflake_connection()."""

    def test_oauth_when_token_file_exists(self):
        """Tier 1: When /snowflake/session/token exists, use OAuth."""
        mod = _import_shared_utils()
        mock_sf, mock_connector = _make_mock_snowflake_connector()
        token_content = "my-oauth-token-xyz"

        with (
            patch.dict(os.environ, {**_CONN_ENV}, clear=False),
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=True),
            patch("builtins.open", mock_open(read_data=token_content)),
        ):
            mod.get_snowflake_connection()

        mock_connector.connect.assert_called_once()
        kwargs = mock_connector.connect.call_args[1]
        assert kwargs["authenticator"] == "oauth"
        assert kwargs["token"] == token_content
        assert kwargs["account"] == "test-account"

    def test_oauth_reads_host_from_env(self):
        """OAuth path should pass SNOWFLAKE_HOST to connector."""
        mod = _import_shared_utils(SNOWFLAKE_HOST="my-host.snowflakecomputing.com")
        mock_sf, mock_connector = _make_mock_snowflake_connector()

        with (
            patch.dict(
                os.environ,
                {**_CONN_ENV, "SNOWFLAKE_HOST": "my-host.snowflakecomputing.com"},
                clear=False,
            ),
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=True),
            patch("builtins.open", mock_open(read_data="token")),
        ):
            mod.get_snowflake_connection()

        kwargs = mock_connector.connect.call_args[1]
        assert kwargs["host"] == "my-host.snowflakecomputing.com"

    def test_pat_when_env_var_set(self):
        """Tier 2: When SNOWFLAKE_PAT is set and no token file, use PAT."""
        env = {
            **_CONN_ENV,
            "SNOWFLAKE_PAT": "my-pat-token",
            "SNOWFLAKE_USER": "pat-user",
        }
        mod = _import_shared_utils(**env)
        mock_sf, mock_connector = _make_mock_snowflake_connector()

        with (
            patch.dict(os.environ, env, clear=False),
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            mod.get_snowflake_connection()

        mock_connector.connect.assert_called_once()
        kwargs = mock_connector.connect.call_args[1]
        assert kwargs["password"] == "my-pat-token"
        assert kwargs["user"] == "pat-user"
        assert "authenticator" not in kwargs
        assert "token" not in kwargs

    def test_user_password_fallback(self):
        """Tier 3: When no token file and no PAT, use user/password."""
        env = {
            **_CONN_ENV,
            "SNOWFLAKE_USER": "my-user",
            "SNOWFLAKE_PASSWORD": "my-password",
        }
        mod = _import_shared_utils(**env)
        mock_sf, mock_connector = _make_mock_snowflake_connector()

        with (
            patch.dict(os.environ, env, clear=False),
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            mod.get_snowflake_connection()

        mock_connector.connect.assert_called_once()
        kwargs = mock_connector.connect.call_args[1]
        assert kwargs["user"] == "my-user"
        assert kwargs["password"] == "my-password"
        assert "authenticator" not in kwargs
        assert "token" not in kwargs

    def test_oauth_takes_precedence_over_pat(self):
        """Token file should take priority even when PAT is also set."""
        env = {**_CONN_ENV, "SNOWFLAKE_PAT": "my-pat"}
        mod = _import_shared_utils(**env)
        mock_sf, mock_connector = _make_mock_snowflake_connector()

        with (
            patch.dict(os.environ, env, clear=False),
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

    def test_pat_takes_precedence_over_password(self):
        """PAT should take priority over user/password when no token file."""
        env = {
            **_CONN_ENV,
            "SNOWFLAKE_PAT": "my-pat",
            "SNOWFLAKE_USER": "user",
            "SNOWFLAKE_PASSWORD": "password",
        }
        mod = _import_shared_utils(**env)
        mock_sf, mock_connector = _make_mock_snowflake_connector()

        with (
            patch.dict(os.environ, env, clear=False),
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            mod.get_snowflake_connection()

        kwargs = mock_connector.connect.call_args[1]
        assert kwargs["password"] == "my-pat"


class TestGetSnowflakeConnectionCommon:
    """Test common parameters passed to all connection tiers."""

    def test_sets_query_tag_in_session_parameters(self):
        """All tiers should set QUERY_TAG in session_parameters."""
        mod = _import_shared_utils()
        mock_sf, mock_connector = _make_mock_snowflake_connector()

        with (
            patch.dict(os.environ, _CONN_ENV, clear=False),
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            mod.get_snowflake_connection(query_tag="prefect/my-flow")

        kwargs = mock_connector.connect.call_args[1]
        assert kwargs["session_parameters"] == {"QUERY_TAG": "prefect/my-flow"}

    def test_passes_warehouse_database_schema(self):
        """All tiers should pass warehouse, database, and schema."""
        env = {
            **_CONN_ENV,
            "SNOWFLAKE_WAREHOUSE": "MY_WH",
            "SNOWFLAKE_DATABASE": "MY_DB",
            "SNOWFLAKE_SCHEMA": "MY_SCHEMA",
        }
        mod = _import_shared_utils(**env)
        mock_sf, mock_connector = _make_mock_snowflake_connector()

        with (
            patch.dict(os.environ, env, clear=False),
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            mod.get_snowflake_connection()

        kwargs = mock_connector.connect.call_args[1]
        assert kwargs["warehouse"] == "MY_WH"
        assert kwargs["database"] == "MY_DB"
        assert kwargs["schema"] == "MY_SCHEMA"

    def test_default_query_tag_when_none_provided(self):
        """Default query_tag should be 'prefect/unknown' without runtime context."""
        mod = _import_shared_utils()
        mock_sf, mock_connector = _make_mock_snowflake_connector()

        with (
            patch.dict(os.environ, _CONN_ENV, clear=False),
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            mod.get_snowflake_connection()

        kwargs = mock_connector.connect.call_args[1]
        assert kwargs["session_parameters"]["QUERY_TAG"] == "prefect/unknown"


# ===================================================================
# _resolve_query_tag
# ===================================================================
class TestResolveQueryTag:
    """Test _resolve_query_tag with Prefect runtime mocking."""

    def test_custom_tag_passes_through(self):
        mod = _import_shared_utils()
        assert mod._resolve_query_tag("my/custom-tag") == "my/custom-tag"

    def test_empty_string_triggers_auto_resolve(self):
        mod = _import_shared_utils()
        assert mod._resolve_query_tag("") == "prefect/unknown"

    def test_none_triggers_auto_resolve(self):
        mod = _import_shared_utils()
        assert mod._resolve_query_tag(None) == "prefect/unknown"

    def test_no_arg_triggers_auto_resolve(self):
        mod = _import_shared_utils()
        assert mod._resolve_query_tag() == "prefect/unknown"

    def test_uses_prefect_runtime_flow_name(self):
        """When Prefect runtime has a flow name, use it."""
        mod = _import_shared_utils()

        # _resolve_query_tag does: from prefect.runtime import flow_run
        # then accesses flow_run.flow_name. We mock the entire import chain.
        mock_flow_run = MagicMock()
        mock_flow_run.flow_name = "my-great-flow"
        mock_runtime = types.ModuleType("prefect.runtime")
        mock_runtime.flow_run = mock_flow_run

        with patch.dict(sys.modules, {"prefect.runtime": mock_runtime}):
            result = mod._resolve_query_tag()

        assert result == "prefect/my-great-flow"

    def test_runtime_returning_none_falls_back(self):
        """If flow_name is None, fall back to prefect/unknown."""
        mod = _import_shared_utils()
        mock_flow_run = MagicMock()
        mock_flow_run.flow_name = None
        mock_runtime = types.ModuleType("prefect.runtime")
        mock_runtime.flow_run = mock_flow_run

        with patch.dict(sys.modules, {"prefect.runtime": mock_runtime}):
            result = mod._resolve_query_tag()

        assert result == "prefect/unknown"

    def test_handles_runtime_import_error(self):
        """Should fall back gracefully if prefect.runtime raises."""
        mod = _import_shared_utils()
        result = mod._resolve_query_tag()
        assert result == "prefect/unknown"


# ===================================================================
# execute_query
# ===================================================================
class TestExecuteQuery:
    """Test execute_query cursor lifecycle."""

    def _setup_mock_connector(self, cursor_description, fetchall_return):
        """Create mock snowflake connector with configured cursor."""
        mock_sf, mock_connector = _make_mock_snowflake_connector()
        mock_cursor = MagicMock()
        mock_cursor.description = cursor_description
        mock_cursor.fetchall.return_value = fetchall_return
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connector.connect.return_value = mock_conn
        return mock_sf, mock_connector, mock_conn, mock_cursor

    def test_returns_columns_and_rows(self):
        """execute_query should return (columns, rows) tuple."""
        mod = _import_shared_utils()
        mock_sf, mock_connector, _, _ = self._setup_mock_connector(
            [("COL_A",), ("COL_B",)],
            [("val1", "val2"), ("val3", "val4")],
        )

        with (
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            columns, rows = mod.execute_query("SELECT 1")

        assert columns == ["COL_A", "COL_B"]
        assert rows == [("val1", "val2"), ("val3", "val4")]

    def test_executes_provided_sql(self):
        """The exact SQL string should be passed to cursor.execute."""
        mod = _import_shared_utils()
        mock_sf, mock_connector, _, mock_cursor = self._setup_mock_connector(
            [("X",)],
            [],
        )

        with (
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            mod.execute_query("SELECT * FROM my_table WHERE id = 42")

        mock_cursor.execute.assert_called_once_with("SELECT * FROM my_table WHERE id = 42")

    def test_closes_connection_on_success(self):
        """Connection must be closed after successful query."""
        mod = _import_shared_utils()
        mock_sf, mock_connector, mock_conn, _ = self._setup_mock_connector(
            [("X",)],
            [],
        )

        with (
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            mod.execute_query("SELECT 1")

        mock_conn.close.assert_called_once()

    def test_closes_connection_on_error(self):
        """Connection must be closed even when query raises."""
        mod = _import_shared_utils()
        mock_sf, mock_connector, mock_conn, mock_cursor = self._setup_mock_connector(
            [("X",)],
            [],
        )
        mock_cursor.execute.side_effect = RuntimeError("query failed")

        with (
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
            pytest.raises(RuntimeError, match="query failed"),
        ):
            mod.execute_query("BAD SQL")

        mock_conn.close.assert_called_once()

    def test_passes_query_tag(self):
        """execute_query should forward query_tag to get_snowflake_connection."""
        mod = _import_shared_utils()
        mock_sf, mock_connector, _, _ = self._setup_mock_connector(
            [("X",)],
            [],
        )

        with (
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            mod.execute_query("SELECT 1", query_tag="prefect/test-flow")

        kwargs = mock_connector.connect.call_args[1]
        assert kwargs["session_parameters"]["QUERY_TAG"] == "prefect/test-flow"


# ===================================================================
# execute_ddl
# ===================================================================
class TestExecuteDdl:
    """Test execute_ddl cursor lifecycle."""

    def _setup_mock_connector(self, cursor_description, fetchone_return=None):
        """Create mock snowflake connector with configured cursor."""
        mock_sf, mock_connector = _make_mock_snowflake_connector()
        mock_cursor = MagicMock()
        mock_cursor.description = cursor_description
        mock_cursor.fetchone.return_value = fetchone_return
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connector.connect.return_value = mock_conn
        return mock_sf, mock_connector, mock_conn, mock_cursor

    def test_returns_status_message(self):
        """execute_ddl should return the first row's first column."""
        mod = _import_shared_utils()
        mock_sf, mock_connector, _, _ = self._setup_mock_connector(
            [("status",)],
            ("Table MYTABLE successfully created.",),
        )

        with (
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            result = mod.execute_ddl("CREATE TABLE mytable (id INT)")

        assert result == "Table MYTABLE successfully created."

    def test_returns_ok_when_no_description(self):
        """When cursor has no description (e.g. USE ROLE), return 'OK'."""
        mod = _import_shared_utils()
        mock_sf, mock_connector, _, _ = self._setup_mock_connector(None)

        with (
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            result = mod.execute_ddl("USE ROLE MYROLE")

        assert result == "OK"

    def test_closes_connection_on_success(self):
        mod = _import_shared_utils()
        mock_sf, mock_connector, mock_conn, _ = self._setup_mock_connector(
            [("status",)],
            ("OK",),
        )

        with (
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            mod.execute_ddl("DROP TABLE foo")

        mock_conn.close.assert_called_once()

    def test_closes_connection_on_error(self):
        mod = _import_shared_utils()
        mock_sf, mock_connector, mock_conn, mock_cursor = self._setup_mock_connector(
            [("status",)],
            ("OK",),
        )
        mock_cursor.execute.side_effect = RuntimeError("ddl failed")

        with (
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
            pytest.raises(RuntimeError, match="ddl failed"),
        ):
            mod.execute_ddl("BAD DDL")

        mock_conn.close.assert_called_once()

    def test_passes_query_tag(self):
        mod = _import_shared_utils()
        mock_sf, mock_connector, _, _ = self._setup_mock_connector(
            [("status",)],
            ("OK",),
        )

        with (
            patch.dict(
                sys.modules,
                {"snowflake": mock_sf, "snowflake.connector": mock_connector},
            ),
            patch("os.path.isfile", return_value=False),
        ):
            mod.execute_ddl("CREATE TABLE t (id INT)", query_tag="prefect/ddl-flow")

        kwargs = mock_connector.connect.call_args[1]
        assert kwargs["session_parameters"]["QUERY_TAG"] == "prefect/ddl-flow"
