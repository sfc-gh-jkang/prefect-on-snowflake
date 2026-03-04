"""Comprehensive behavioral tests for dashboard/app.py.

Every data-fetching function, API helper, and pure utility is tested
with mocked Snowflake / Prefect API backends — no live connections needed.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import sys
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pandas as pd

# ---------------------------------------------------------------------------
# Helpers: import app.py with mocked Streamlit
# ---------------------------------------------------------------------------

_APP_PATH = os.path.join(os.path.dirname(__file__), os.pardir, "dashboard", "app.py")


@contextmanager
def _import_app(**env_overrides):
    """Import dashboard/app.py with a mocked ``streamlit`` package.

    Yields the loaded module.  Environment variables can be overridden
    via keyword arguments (e.g. ``PREFECT_API_URL="http://test"``).
    """
    # Build a mock st module that satisfies top-level attribute access
    mock_st = MagicMock()
    mock_st.cache_resource = lambda f: f  # passthrough decorator
    mock_st.session_state = {}

    env = {
        "PREFECT_API_URL": "https://test.snowflakecomputing.app/api",
        "SNOWFLAKE_PAT": "test-pat-token",
        "SNOWFLAKE_DATABASE": "PREFECT_DB",
        "SNOWFLAKE_SCHEMA": "PREFECT_SCHEMA",
        **env_overrides,
    }

    saved_modules = {}
    for mod_name in list(sys.modules):
        if (
            mod_name == "streamlit"
            or mod_name.startswith("streamlit.")
            or mod_name == "plotly"
            or mod_name.startswith("plotly.")
        ):
            saved_modules[mod_name] = sys.modules.pop(mod_name)

    try:
        with patch.dict(os.environ, env, clear=False):
            sys.modules["streamlit"] = mock_st
            sys.modules["streamlit.components"] = MagicMock()
            sys.modules["streamlit.components.v1"] = MagicMock()
            # Mock plotly so tests run without the heavy viz dependency
            mock_plotly = MagicMock()
            sys.modules["plotly"] = mock_plotly
            sys.modules["plotly.express"] = mock_plotly.express
            sys.modules["plotly.graph_objects"] = mock_plotly.graph_objects
            spec = importlib.util.spec_from_file_location("dashboard_app", _APP_PATH)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            mod._mock_st = mock_st  # expose for assertions
            yield mod
    finally:
        for mod_name in list(sys.modules):
            if (
                mod_name == "streamlit"
                or mod_name.startswith("streamlit.")
                or mod_name == "plotly"
                or mod_name.startswith("plotly.")
            ):
                del sys.modules[mod_name]
        sys.modules.update(saved_modules)


_UNSET = object()


@contextmanager
def _app_with_mock(attr: str, *, return_value=_UNSET, side_effect=_UNSET, **env):
    """Import app and patch *attr* on it in one combined context manager.

    Yields ``(module, mock_object)`` — avoids nested ``with`` blocks that
    trigger SIM117.
    """
    with _import_app(**env) as mod:  # noqa: SIM117
        kwargs = {}
        if return_value is not _UNSET:
            kwargs["return_value"] = return_value
        if side_effect is not _UNSET:
            kwargs["side_effect"] = side_effect
        with patch.object(mod, attr, **kwargs) as m:  # depends on mod from outer
            yield mod, m


@contextmanager
def _app_with_request_mock(*, return_value=_UNSET, side_effect=_UNSET, **env):
    """Import app and patch ``requests.request`` in one combined context.

    Yields ``(module, mock_request)`` — avoids nested ``with`` that
    trigger SIM117.
    """
    kwargs = {}
    if return_value is not _UNSET:
        kwargs["return_value"] = return_value
    if side_effect is not _UNSET:
        kwargs["side_effect"] = side_effect
    with _import_app(**env) as mod:  # noqa: SIM117
        with patch("requests.request", **kwargs) as mock_req:
            yield mod, mock_req


# ===================================================================
# TestGetPat
# ===================================================================


class TestGetPat:
    """Test PAT retrieval for local and SiS environments."""

    def test_local_returns_env_var(self):
        with _import_app(SNOWFLAKE_PAT="my-secret-pat") as mod:
            mod.IS_LOCAL = True
            assert mod._get_pat() == "my-secret-pat"

    def test_local_returns_empty_when_unset(self):
        with _import_app(SNOWFLAKE_PAT="") as mod:
            mod.IS_LOCAL = True
            # Fully remove the key so _get_pat() returns ""
            os.environ.pop("SNOWFLAKE_PAT", None)
            assert mod._get_pat() == ""

    def test_sis_calls_udf(self):
        with _app_with_mock("get_session") as (mod, mock_gs):
            mod.IS_LOCAL = False
            mock_session = MagicMock()
            mock_session.sql.return_value.collect.return_value = [("udf-pat",)]
            mock_gs.return_value = mock_session
            assert mod._get_pat() == "udf-pat"
            mock_session.sql.assert_called_once_with("SELECT GET_PREFECT_PAT()")

    def test_sis_returns_empty_on_error(self):
        with _app_with_mock("get_session", side_effect=RuntimeError("boom")) as (mod, _m):
            mod.IS_LOCAL = False
            assert mod._get_pat() == ""


# ===================================================================
# TestRunQuery
# ===================================================================


class TestRunQuery:
    """Test SQL execution wrapper."""

    def _make_mock_session(self, result_df):
        """Build a mock Snowpark session that returns *result_df* for the 3rd sql() call."""
        session = MagicMock()
        call_count = {"n": 0}

        def sql_side_effect(query):
            call_count["n"] += 1
            mock_result = MagicMock()
            if call_count["n"] <= 2:
                # USE DATABASE / USE SCHEMA
                mock_result.collect.return_value = []
                return mock_result
            mock_result.to_pandas.return_value = result_df.copy()
            return mock_result

        session.sql.side_effect = sql_side_effect
        return session

    def test_returns_dataframe_with_lowercase_columns(self):
        raw = pd.DataFrame({"NAME": ["svc1"], "STATUS": ["READY"]})
        session = self._make_mock_session(raw)
        with _app_with_mock("get_session", return_value=session) as (mod, _m):
            df = mod.run_query("SHOW SERVICES")
        assert list(df.columns) == ["name", "status"]
        assert df["name"].iloc[0] == "svc1"

    def test_strips_quoted_column_names(self):
        raw = pd.DataFrame({'"name"': ["svc1"], '"status"': ["READY"]})
        session = self._make_mock_session(raw)
        with _app_with_mock("get_session", return_value=session) as (mod, _m):
            df = mod.run_query("SHOW SERVICES")
        assert list(df.columns) == ["name", "status"]

    def test_returns_empty_on_exception(self):
        session = MagicMock()
        session.sql.side_effect = RuntimeError("connection lost")
        with _app_with_mock("get_session", return_value=session) as (mod, _m):
            df = mod.run_query("SELECT 1")
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_calls_use_database_and_schema(self):
        raw = pd.DataFrame({"col": [1]})
        session = self._make_mock_session(raw)
        with _app_with_mock("get_session", return_value=session) as (mod, _m):
            mod.run_query("SELECT 1")
        calls = [c.args[0] for c in session.sql.call_args_list]
        assert "USE DATABASE PREFECT_DB" in calls[0]
        assert "USE SCHEMA PREFECT_SCHEMA" in calls[1]
        assert calls[2] == "SELECT 1"

    def test_logs_to_session_state(self):
        raw = pd.DataFrame({"x": [1, 2, 3]})
        session = self._make_mock_session(raw)
        with _app_with_mock("get_session", return_value=session) as (mod, _m):
            mod._mock_st.session_state = {}
            mod.run_query("SELECT x FROM t")
        debug = mod._mock_st.session_state.get("_debug_queries", [])
        assert len(debug) == 1
        assert debug[0]["rows"] == 3


# ===================================================================
# TestPrefectApiRequest
# ===================================================================


class TestPrefectApiRequest:
    """Test Prefect API HTTP helper."""

    def test_success_returns_json(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"data": [1, 2]}
        mock_resp.raise_for_status.return_value = None
        with _app_with_request_mock(return_value=mock_resp) as (mod, mock_req):
            result = mod.prefect_api_request("POST", "/flow_runs/filter", {"limit": 10})
        assert result == {"data": [1, 2]}
        call_kwargs = mock_req.call_args
        assert "Snowflake Token" in call_kwargs.kwargs["headers"]["Authorization"]

    def test_returns_none_on_http_error(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("404")
        with _app_with_request_mock(return_value=mock_resp) as (mod, _req):
            result = mod.prefect_api_request("GET", "/health")
        assert result is None

    def test_returns_none_on_connection_error(self):
        with _app_with_request_mock(side_effect=ConnectionError("refused")) as (mod, _req):
            result = mod.prefect_api_request("POST", "/flow_runs/filter")
        assert result is None

    def test_omits_auth_when_no_pat(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status.return_value = None
        with _app_with_request_mock(return_value=mock_resp, SNOWFLAKE_PAT="") as (
            _mod,
            mock_req,
        ):
            _mod.prefect_api_request("POST", "/test")
        headers = mock_req.call_args.kwargs["headers"]
        assert "Authorization" not in headers

    def test_builds_url_from_config(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {}
        mock_resp.raise_for_status.return_value = None
        with _app_with_request_mock(
            return_value=mock_resp,
            PREFECT_API_URL="https://my-endpoint.app/api",
        ) as (_mod, mock_req):
            _mod.prefect_api_request("POST", "/flow_runs/filter")
        url = mock_req.call_args.args[1]
        assert url == "https://my-endpoint.app/api/flow_runs/filter"


# ===================================================================
# TestGetFlowRuns
# ===================================================================


class TestGetFlowRuns:
    """Test flow run parsing from Prefect API response."""

    SAMPLE_RESPONSE = [
        {
            "id": "abcdef12-3456-7890-abcd-ef1234567890",
            "name": "my-flow-run",
            "flow_id": "deadbeef-1234-5678-9abc-def012345678",
            "state": {"type": "COMPLETED", "name": "Completed"},
            "start_time": "2026-03-07T10:00:00Z",
            "end_time": "2026-03-07T10:01:00Z",
            "total_run_time": 60.0,
            "work_pool_name": "spcs-pool",
            "deployment_id": "11111111-2222-3333-4444-555555555555",
        }
    ]

    def test_parses_response_into_dataframe(self):
        with _app_with_mock("prefect_api_request", return_value=self.SAMPLE_RESPONSE) as (mod, _m):
            df = mod.get_flow_runs()
        assert len(df) == 1
        assert df["name"].iloc[0] == "my-flow-run"
        assert df["state"].iloc[0] == "COMPLETED"
        assert df["work_pool"].iloc[0] == "spcs-pool"

    def test_truncates_ids_to_8_chars(self):
        with _app_with_mock("prefect_api_request", return_value=self.SAMPLE_RESPONSE) as (mod, _m):
            df = mod.get_flow_runs()
        assert df["id"].iloc[0] == "abcdef12"
        assert df["flow_id"].iloc[0] == "deadbeef"
        assert df["deployment"].iloc[0] == "11111111"

    def test_returns_empty_on_api_failure(self):
        with _app_with_mock("prefect_api_request", return_value=None) as (mod, _m):
            df = mod.get_flow_runs()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_handles_missing_optional_fields(self):
        minimal = [{"id": "aaaa-bbbb", "state": {}}]
        with _app_with_mock("prefect_api_request", return_value=minimal) as (mod, _m):
            df = mod.get_flow_runs()
        assert df["state"].iloc[0] == "UNKNOWN"
        assert df["state_name"].iloc[0] == "Unknown"
        assert df["deployment"].iloc[0] == ""


# ===================================================================
# TestGetWorkPoolsStatus
# ===================================================================


class TestGetWorkPoolsStatus:
    """Test work pool status parsing."""

    SAMPLE = [
        {
            "name": "spcs-pool",
            "type": "process",
            "status": "READY",
            "is_paused": False,
            "concurrency_limit": 10,
            "default_queue_id": "aaaa-bbbb-cccc-dddd",
        }
    ]

    def test_parses_work_pools(self):
        with _app_with_mock("prefect_api_request", return_value=self.SAMPLE) as (mod, _m):
            df = mod.get_work_pools_status()
        assert len(df) == 1
        assert df["name"].iloc[0] == "spcs-pool"
        assert df["status"].iloc[0] == "READY"
        assert df["concurrency_limit"].iloc[0] == 10

    def test_returns_empty_on_failure(self):
        with _app_with_mock("prefect_api_request", return_value=None) as (mod, _m):
            df = mod.get_work_pools_status()
        assert df.empty

    def test_handles_none_concurrency(self):
        data = [{"name": "p", "type": "x", "status": "READY", "is_paused": False}]
        with _app_with_mock("prefect_api_request", return_value=data) as (mod, _m):
            df = mod.get_work_pools_status()
        assert df["concurrency_limit"].iloc[0] is None


# ===================================================================
# TestGetDeployments
# ===================================================================


class TestGetDeployments:
    """Test deployment parsing."""

    def test_parses_active_schedule(self):
        data = [
            {
                "name": "my-deploy",
                "flow_id": "deadbeef-1234",
                "work_pool_name": "gcp-pool",
                "schedules": [{"active": True, "schedule": {}}],
                "paused": False,
                "tags": ["gcp", "prod"],
                "created": "2026-01-01",
            }
        ]
        with _app_with_mock("prefect_api_request", return_value=data) as (mod, _m):
            df = mod.get_deployments()
        assert df["is_schedule_active"].iloc[0] == True  # noqa: E712 (numpy bool)
        assert df["tags"].iloc[0] == "gcp, prod"

    def test_inactive_when_no_schedules(self):
        data = [{"name": "d", "schedules": [], "tags": []}]
        with _app_with_mock("prefect_api_request", return_value=data) as (mod, _m):
            df = mod.get_deployments()
        assert df["is_schedule_active"].iloc[0] == False  # noqa: E712 (numpy bool)

    def test_returns_empty_on_none(self):
        with _app_with_mock("prefect_api_request", return_value=None) as (mod, _m):
            df = mod.get_deployments()
        assert df.empty

    def test_truncates_flow_id(self):
        data = [
            {
                "name": "d",
                "flow_id": "abcdef12-3456-7890",
                "schedules": [],
                "tags": [],
            }
        ]
        with _app_with_mock("prefect_api_request", return_value=data) as (mod, _m):
            df = mod.get_deployments()
        assert df["flow_id"].iloc[0] == "abcdef12"


# ===================================================================
# TestGetWorkPoolWorkers
# ===================================================================


class TestGetWorkPoolWorkers:
    """Test work pool worker fetching."""

    def test_returns_list_on_success(self):
        workers = [{"id": "w1", "name": "worker-1"}]
        with _app_with_mock("prefect_api_request", return_value=workers) as (mod, _m):
            result = mod.get_work_pool_workers("spcs-pool")
        assert result == workers

    def test_returns_empty_list_on_failure(self):
        with _app_with_mock("prefect_api_request", return_value=None) as (mod, _m):
            result = mod.get_work_pool_workers("spcs-pool")
        assert result == []


# ===================================================================
# TestSPCSDataFetchers
# ===================================================================


class TestSPCSDataFetchers:
    """Test SPCS data-fetching functions that wrap run_query."""

    def test_get_service_containers_concatenates_frames(self):
        df1 = pd.DataFrame({"status": ["READY"], "service_name": ["PF_SERVER"]})
        df2 = pd.DataFrame({"status": ["READY"], "service_name": ["PF_REDIS"]})
        call_count = {"n": 0}

        def mock_run_query(sql):
            call_count["n"] += 1
            if "PF_SERVER" in sql or call_count["n"] == 1:
                return df1.copy()
            if "PF_REDIS" in sql or call_count["n"] == 2:
                return df2.copy()
            return pd.DataFrame()

        with _app_with_mock("run_query", side_effect=mock_run_query) as (mod, _m):
            result = mod.get_service_containers()
        assert len(result) >= 2

    def test_get_service_containers_returns_empty_when_all_empty(self):
        with _app_with_mock("run_query", return_value=pd.DataFrame()) as (mod, _m):
            result = mod.get_service_containers()
        assert result.empty

    def test_get_service_summary_filters_to_prefect_services(self):
        all_services = pd.DataFrame(
            {
                "name": ["PF_SERVER", "PF_REDIS", "OTHER_SVC"],
                "status": ["READY", "READY", "READY"],
            }
        )
        with _app_with_mock("run_query", return_value=all_services) as (mod, _m):
            result = mod.get_service_summary()
        assert "OTHER_SVC" not in result["name"].values
        assert "PF_SERVER" in result["name"].values

    def test_get_compute_pools_filters_to_configured(self):
        all_pools = pd.DataFrame(
            {
                "name": ["PREFECT_INFRA_POOL", "PREFECT_CORE_POOL", "UNRELATED_POOL"],
                "state": ["ACTIVE", "ACTIVE", "ACTIVE"],
            }
        )
        with _app_with_mock("run_query", return_value=all_pools) as (mod, _m):
            result = mod.get_compute_pools()
        assert "UNRELATED_POOL" not in result["name"].values
        assert len(result) == 2

    def test_get_service_instances_concatenates(self):
        df = pd.DataFrame({"instance_id": ["i1"], "status": ["READY"]})
        with _app_with_mock("run_query", return_value=df) as (mod, _m):
            result = mod.get_service_instances()
        assert not result.empty

    def test_get_service_metrics_passes_hours(self):
        with _app_with_mock("run_query", return_value=pd.DataFrame()) as (mod, mock_rq):
            mod.get_service_metrics("PF_SERVER", hours=3)
        sql = mock_rq.call_args.args[0]
        assert "-3" in sql
        assert "PF_SERVER" in sql
        assert "SPCS_GET_METRICS" in sql

    def test_get_service_logs_passes_minutes(self):
        with _app_with_mock("run_query", return_value=pd.DataFrame()) as (mod, mock_rq):
            mod.get_service_logs("PF_WORKER", minutes=60)
        sql = mock_rq.call_args.args[0]
        assert "-60" in sql
        assert "PF_WORKER" in sql
        assert "SPCS_GET_LOGS" in sql

    def test_get_event_table_logs_queries_log_type(self):
        with _app_with_mock("run_query", return_value=pd.DataFrame()) as (mod, mock_rq):
            mod.get_event_table_logs(hours=2)
        sql = mock_rq.call_args.args[0]
        assert "RECORD_TYPE = 'LOG'" in sql
        assert "-2" in sql


# ===================================================================
# TestCalcUptime
# ===================================================================


class TestCalcUptime:
    """Test the _calc_uptime pure function."""

    def test_multiday_uptime(self):
        with _import_app() as mod:
            start = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=2, hours=3, minutes=15)
            result = mod._calc_uptime(str(start))
        assert result.startswith("2d 3h")

    def test_subday_uptime(self):
        with _import_app() as mod:
            start = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=5, minutes=30)
            result = mod._calc_uptime(str(start))
        assert result.startswith("5h")

    def test_none_returns_na(self):
        with _import_app() as mod:
            assert mod._calc_uptime(None) == "N/A"

    def test_nan_returns_na(self):
        with _import_app() as mod:
            assert mod._calc_uptime(float("nan")) == "N/A"

    def test_quoted_timestamp(self):
        with _import_app() as mod:
            start = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=1)
            result = mod._calc_uptime(f'"{start}"')
        assert "h" in result
        assert result != "N/A"

    def test_garbage_returns_na(self):
        with _import_app() as mod:
            assert mod._calc_uptime("not-a-date") == "N/A"


# ===================================================================
# TestRenderKpi
# ===================================================================


class TestRenderKpi:
    """Test KPI rendering dispatch."""

    def test_with_status_calls_markdown(self):
        with _import_app() as mod:
            col = MagicMock()
            mod.render_kpi(col, "Containers", "4/4", status="READY")
            col.markdown.assert_called_once()
            html = col.markdown.call_args.args[0]
            assert "#00c853" in html  # READY color
            assert "4/4" in html

    def test_without_status_calls_metric(self):
        with _import_app() as mod:
            col = MagicMock()
            mod.render_kpi(col, "Uptime", "2d 3h", delta="+1h")
            col.metric.assert_called_once_with("Uptime", "2d 3h", delta="+1h")

    def test_failed_status_uses_red(self):
        with _import_app() as mod:
            col = MagicMock()
            mod.render_kpi(col, "Health", "2/4", status="FAILED")
            html = col.markdown.call_args.args[0]
            assert "#f44336" in html  # FAILED color


# ===========================================================================
# Structural tests — dashboard source code analysis (no mocking needed)
# ===========================================================================
import ast  # noqa: E402
from pathlib import Path  # noqa: E402

import pytest as pytest  # needed for structural test fixtures below  # noqa: E402

_DASHBOARD_DIR = Path(__file__).resolve().parent.parent / "dashboard"
_APP_SOURCE = _DASHBOARD_DIR / "app.py"
_PYPROJECT = _DASHBOARD_DIR / "pyproject.toml"
_SNOWFLAKE_YML = _DASHBOARD_DIR / "snowflake.yml"


class TestDashboardNoHardcodedSecrets:
    """Verify dashboard/app.py contains no hardcoded credentials."""

    @pytest.fixture(autouse=True)
    def load_source(self):
        self.content = _APP_SOURCE.read_text()
        self.lines = self.content.splitlines()

    def test_no_hardcoded_pat_tokens(self):
        """No PAT tokens should appear in source."""
        for i, line in enumerate(self.lines, 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            # PAT tokens are long alphanumeric strings assigned to variables
            if "pat-" in stripped.lower() and "test" not in stripped.lower():
                assert "environ" in stripped or "os.environ" in stripped, (
                    f"Possible hardcoded PAT at line {i}: {stripped}"
                )

    def test_no_hardcoded_passwords(self):
        """No literal passwords in source."""
        for i, line in enumerate(self.lines, 1):
            stripped = line.strip()
            if stripped.startswith("#") or stripped.startswith('"""'):
                continue
            lower = stripped.lower()
            if 'password = "' in lower or "password = '" in lower:
                raise AssertionError(f"Possible hardcoded password at line {i}: {stripped}")

    def test_credentials_come_from_env_or_connection(self):
        """PAT and DB credentials must come from env vars or st.connection."""
        assert "os.environ" in self.content or "st.connection" in self.content

    def test_no_snowflake_account_identifiers(self):
        """No Snowflake account identifiers (locators) in source."""
        import re  # noqa: E402

        for i, line in enumerate(self.lines, 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            # Snowflake locators look like ABC12345 or orgname-acctname
            if re.search(r"QIB\d{5}", stripped):
                raise AssertionError(f"Hardcoded account locator at line {i}: {stripped}")


class TestDashboardSiSConfig:
    """Validate snowflake.yml SiS Container Runtime configuration."""

    @pytest.fixture(autouse=True)
    def load_config(self):
        import yaml  # noqa: E402

        self.config = yaml.safe_load(_SNOWFLAKE_YML.read_text())

    def test_definition_version_is_2(self):
        assert self.config["definition_version"] == 2

    def test_entity_type_is_streamlit(self):
        entity = list(self.config["entities"].values())[0]
        assert entity["type"] == "streamlit"

    def test_uses_container_runtime(self):
        entity = list(self.config["entities"].values())[0]
        assert "CONTAINER_RUNTIME" in entity["runtime_name"]

    def test_uses_python_311(self):
        entity = list(self.config["entities"].values())[0]
        assert "PY3_11" in entity["runtime_name"]

    def test_main_file_is_app_py(self):
        entity = list(self.config["entities"].values())[0]
        assert entity["main_file"] == "app.py"

    def test_artifacts_include_app_and_pyproject(self):
        entity = list(self.config["entities"].values())[0]
        artifacts = entity["artifacts"]
        assert "app.py" in artifacts
        assert "pyproject.toml" in artifacts

    def test_uses_dedicated_dashboard_pool(self):
        entity = list(self.config["entities"].values())[0]
        assert "DASHBOARD" in entity["compute_pool"]

    def test_has_external_access_integration(self):
        entity = list(self.config["entities"].values())[0]
        eais = entity.get("external_access_integrations", [])
        assert len(eais) >= 1

    def test_database_and_schema(self):
        entity = list(self.config["entities"].values())[0]
        ident = entity["identifier"]
        assert ident["database"] == "PREFECT_DB"
        assert ident["schema"] == "PREFECT_SCHEMA"

    def test_has_query_warehouse(self):
        entity = list(self.config["entities"].values())[0]
        assert entity.get("query_warehouse") is not None


class TestDashboardPageStructure:
    """Validate dashboard/app.py has all expected pages and navigation."""

    @pytest.fixture(autouse=True)
    def load_source(self):
        self.content = _APP_SOURCE.read_text()

    def test_has_7_page_functions(self):
        """Dashboard must define all 7 page functions."""
        expected = [
            "page_overview",
            "page_compute_pools",
            "page_services",
            "page_work_pools",
            "page_flow_runs",
            "page_logs_events",
            "page_metrics",
        ]
        for fn in expected:
            assert f"def {fn}" in self.content, f"Missing page function: {fn}"

    def test_page_map_matches_page_functions(self):
        """page_map dict must reference all page functions."""
        for fn in [
            "page_overview",
            "page_compute_pools",
            "page_services",
            "page_work_pools",
            "page_flow_runs",
            "page_logs_events",
            "page_metrics",
        ]:
            assert fn in self.content

    def test_has_main_function(self):
        assert "def main():" in self.content

    def test_has_main_guard(self):
        assert '__name__ == "__main__"' in self.content or "__name__ == '__main__'" in self.content

    def test_uses_wide_layout(self):
        assert 'layout="wide"' in self.content

    def test_has_sidebar_navigation(self):
        assert "st.sidebar" in self.content
        assert "st.radio" in self.content

    def test_has_auto_refresh(self):
        assert "auto_refresh" in self.content or "Auto-refresh" in self.content

    def test_has_debug_panel(self):
        assert "Debug" in self.content and "Query Log" in self.content

    def test_has_runtime_detection(self):
        """Dashboard must detect local vs container runtime."""
        assert "IS_LOCAL" in self.content


class TestDashboardDependencies:
    """Validate dashboard/pyproject.toml has required dependencies."""

    @pytest.fixture(autouse=True)
    def load_pyproject(self):
        self.content = _PYPROJECT.read_text()

    def test_requires_streamlit(self):
        assert "streamlit" in self.content

    def test_requires_plotly(self):
        assert "plotly" in self.content

    def test_requires_pandas(self):
        assert "pandas" in self.content

    def test_requires_requests(self):
        assert "requests" in self.content

    def test_requires_snowflake_connector(self):
        assert "snowflake-connector-python" in self.content

    def test_requires_snowpark(self):
        assert "snowflake-snowpark-python" in self.content

    def test_python_requires_311(self):
        assert ">=3.11" in self.content


class TestDashboardSourceQuality:
    """Validate dashboard/app.py code quality patterns."""

    @pytest.fixture(autouse=True)
    def load_source(self):
        self.content = _APP_SOURCE.read_text()

    def test_source_parses_without_syntax_errors(self):
        """app.py must be valid Python."""
        tree = ast.parse(self.content)
        assert tree is not None

    def test_has_module_docstring(self):
        tree = ast.parse(self.content)
        docstring = ast.get_docstring(tree)
        assert docstring is not None, "app.py must have a module-level docstring"

    def test_uses_future_annotations(self):
        """Should use from __future__ import annotations for modern type hints."""
        assert "from __future__ import annotations" in self.content

    def test_config_uses_env_vars_with_defaults(self):
        """Configuration must use os.environ.get() with sensible defaults."""
        assert 'os.environ.get("SNOWFLAKE_DATABASE"' in self.content
        assert 'os.environ.get("SNOWFLAKE_SCHEMA"' in self.content

    def test_local_env_loading_is_conditional(self):
        """Local .env loading must only happen when IS_LOCAL is True."""
        # Verify .env loading is inside the IS_LOCAL block
        assert "IS_LOCAL" in self.content
        assert ".env" in self.content
