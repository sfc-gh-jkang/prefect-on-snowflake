"""Tests for Prefect flow Python files.

Validates syntax, required decorators, function signatures,
and import patterns for all flow modules.
"""

import ast
import re

import pytest
from conftest import FLOWS_DIR

FLOW_FILES = sorted(f.name for f in FLOWS_DIR.glob("*.py") if f.name != "__init__.py")


class TestFlowDiscovery:
    def test_init_exists(self):
        assert (FLOWS_DIR / "__init__.py").exists()

    @pytest.mark.parametrize(
        "filename",
        ["example_flow.py", "snowflake_flow.py", "external_api_flow.py", "deploy.py"],
    )
    def test_expected_flow_exists(self, filename):
        assert (FLOWS_DIR / filename).exists()


class TestFlowSyntax:
    @pytest.mark.parametrize("filename", FLOW_FILES)
    def test_parses_as_valid_python(self, filename):
        source = (FLOWS_DIR / filename).read_text()
        try:
            ast.parse(source)
        except SyntaxError as e:
            pytest.fail(f"{filename}: syntax error at line {e.lineno}: {e.msg}")


class TestFlowDecorators:
    """Verify that flow files use @flow and @task decorators."""

    FLOW_MODULES = ["example_flow.py", "snowflake_flow.py", "external_api_flow.py"]

    @pytest.mark.parametrize("filename", FLOW_MODULES)
    def test_imports_flow(self, filename):
        source = (FLOWS_DIR / filename).read_text()
        assert "from prefect import" in source and "flow" in source

    @pytest.mark.parametrize("filename", FLOW_MODULES)
    def test_has_flow_decorator(self, filename):
        source = (FLOWS_DIR / filename).read_text()
        assert re.search(r"@flow", source), f"{filename}: missing @flow decorator"

    @pytest.mark.parametrize("filename", FLOW_MODULES)
    def test_has_task_decorator(self, filename):
        source = (FLOWS_DIR / filename).read_text()
        assert re.search(r"@task", source), f"{filename}: missing @task decorator"

    @pytest.mark.parametrize("filename", FLOW_MODULES)
    def test_has_main_block(self, filename):
        source = (FLOWS_DIR / filename).read_text()
        assert 'if __name__ == "__main__"' in source

    @pytest.mark.parametrize("filename", FLOW_MODULES)
    def test_flow_has_name_parameter(self, filename):
        source = (FLOWS_DIR / filename).read_text()
        assert re.search(r"@flow\(.*name=", source, re.DOTALL), (
            f"{filename}: @flow should have a name parameter"
        )

    @pytest.mark.parametrize("filename", FLOW_MODULES)
    def test_flow_has_log_prints(self, filename):
        source = (FLOWS_DIR / filename).read_text()
        assert re.search(r"log_prints\s*=\s*True", source), (
            f"{filename}: @flow should have log_prints=True"
        )


class TestFlowSpecifics:
    def test_snowflake_flow_uses_shared_utils(self):
        source = (FLOWS_DIR / "snowflake_flow.py").read_text()
        assert "from shared_utils import" in source
        assert "get_snowflake_connection" in source

    def test_external_api_flow_imports_requests(self):
        source = (FLOWS_DIR / "external_api_flow.py").read_text()
        assert "requests" in source

    def test_external_api_flow_uses_httpbin(self):
        source = (FLOWS_DIR / "external_api_flow.py").read_text()
        assert "httpbin.org" in source

    def test_deploy_script_has_pool_modes(self):
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "deploy_to_pool" in source
        assert "--gcp" in source
        assert "--all" in source

    def test_deploy_script_has_validate_flag(self):
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "--validate" in source
        assert "_validate" in source

    def test_deploy_script_loads_pools_from_yaml(self):
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "pools.yaml" in source
        assert "_load_pools" in source

    def test_deploy_script_has_git_clone_pull_steps(self):
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "git_clone" in source
        assert "GIT_REPO" in source
        assert "GIT_ACCESS_TOKEN" in source

    def test_deploy_script_has_flowspec_dataclass(self):
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "class FlowSpec" in source
        assert "FLOW_REGISTRY" in source

    def test_deploy_script_imports_git_sha(self):
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "from shared_utils import get_git_sha" in source

    def test_deploy_script_uses_git_sha_for_version(self):
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "git_sha" in source
        assert "spec.version or git_sha" in source

    def test_deploy_script_supports_name_filter(self):
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "--name" in source
        assert "name_filter" in source


class TestFlowSpecDataclass:
    """Test the FlowSpec dataclass from deploy.py offline (no server needed)."""

    @pytest.fixture(autouse=True)
    def _import_deploy(self):
        """Import deploy module components for testing."""
        import importlib.util

        spec = importlib.util.spec_from_file_location("deploy", FLOWS_DIR / "deploy.py")
        mod = importlib.util.module_from_spec(spec)
        # Patch sys.modules to avoid import side effects
        import sys

        old = sys.modules.get("deploy")
        sys.modules["deploy"] = mod
        spec.loader.exec_module(mod)
        self.FlowSpec = mod.FlowSpec
        self.FLOW_REGISTRY = mod.FLOW_REGISTRY
        self.POOLS = mod.POOLS
        yield
        if old is not None:
            sys.modules["deploy"] = old
        else:
            sys.modules.pop("deploy", None)

    def test_flowspec_defaults(self):
        fs = self.FlowSpec(path="foo.py", func="bar", name="test")
        assert fs.pools == "all"
        assert fs.tags == []
        assert fs.parameters is None
        assert fs.cron is None
        assert fs.interval is None
        assert fs.rrule is None
        assert fs.schedules is None
        assert fs.concurrency_limit is None
        assert fs.collision_strategy == "ENQUEUE"
        assert fs.description is None
        assert fs.version is None
        assert fs.paused is None
        assert fs.job_variables is None
        assert fs.enforce_parameter_schema is True

    def test_flowspec_build_schedules_none_by_default(self):
        fs = self.FlowSpec(path="foo.py", func="bar", name="test")
        assert fs.build_schedules() is None

    def test_flowspec_build_schedules_cron(self):
        fs = self.FlowSpec(
            path="foo.py",
            func="bar",
            name="test",
            cron="0 6 * * *",
            timezone="America/New_York",
        )
        schedules = fs.build_schedules()
        assert schedules is not None
        assert len(schedules) == 1
        sched = schedules[0]
        assert sched.schedule.cron == "0 6 * * *"
        assert sched.schedule.timezone == "America/New_York"
        assert sched.active is True

    def test_flowspec_build_schedules_interval(self):
        fs = self.FlowSpec(
            path="foo.py",
            func="bar",
            name="test",
            interval=3600,
        )
        schedules = fs.build_schedules()
        assert schedules is not None
        assert len(schedules) == 1
        from datetime import timedelta

        assert schedules[0].schedule.interval == timedelta(seconds=3600)

    def test_flowspec_build_schedules_rrule(self):
        fs = self.FlowSpec(
            path="foo.py",
            func="bar",
            name="test",
            rrule="FREQ=MONTHLY;BYMONTHDAY=1",
        )
        schedules = fs.build_schedules()
        assert schedules is not None
        assert len(schedules) == 1
        assert schedules[0].schedule.rrule == "FREQ=MONTHLY;BYMONTHDAY=1"

    def test_flowspec_build_schedules_paused(self):
        fs = self.FlowSpec(
            path="foo.py",
            func="bar",
            name="test",
            cron="0 6 * * *",
            paused=True,
        )
        schedules = fs.build_schedules()
        assert schedules[0].active is False

    def test_flowspec_build_schedules_raw_passthrough(self):
        """Raw schedules list should be returned as-is, ignoring convenience fields."""
        from prefect.client.schemas.actions import DeploymentScheduleCreate
        from prefect.client.schemas.schedules import CronSchedule

        raw = [
            DeploymentScheduleCreate(
                schedule=CronSchedule(cron="0 0 * * *"),
                parameters={},
            )
        ]
        fs = self.FlowSpec(
            path="foo.py",
            func="bar",
            name="test",
            cron="should-be-ignored",
            schedules=raw,
        )
        assert fs.build_schedules() is raw

    def test_flowspec_build_concurrency_options_none(self):
        fs = self.FlowSpec(path="foo.py", func="bar", name="test")
        limit, opts = fs.build_concurrency_options()
        assert limit is None
        assert opts is None

    def test_flowspec_build_concurrency_enqueue(self):
        fs = self.FlowSpec(
            path="foo.py",
            func="bar",
            name="test",
            concurrency_limit=3,
        )
        limit, opts = fs.build_concurrency_options()
        assert limit == 3
        assert opts.collision_strategy.value == "ENQUEUE"

    def test_flowspec_build_concurrency_cancel_new(self):
        fs = self.FlowSpec(
            path="foo.py",
            func="bar",
            name="test",
            concurrency_limit=1,
            collision_strategy="CANCEL_NEW",
        )
        limit, opts = fs.build_concurrency_options()
        assert limit == 1
        assert opts.collision_strategy.value == "CANCEL_NEW"

    def test_registry_has_all_expected_flows(self):
        names = {fs.name for fs in self.FLOW_REGISTRY}
        expected = {
            "example-flow",
            "snowflake-etl",
            "external-api",
            "e2e-test",
            "analytics-revenue",
            "quarterly-report",
            "alert-test",
            "data-quality",
            "stage-cleanup",
            "health-check",
        }
        assert names == expected

    def test_registry_entries_have_valid_pool_keys(self):
        for fs in self.FLOW_REGISTRY:
            if fs.pools == "all":
                continue  # "all" sentinel means every pool
            for pool in fs.pools:
                assert pool in self.POOLS, f"{fs.name}: pool '{pool}' not in POOLS"

    def test_registry_paths_exist(self):
        for fs in self.FLOW_REGISTRY:
            full_path = FLOWS_DIR / fs.path
            assert full_path.exists(), f"{fs.name}: flow file {fs.path} not found"

    def test_registry_entries_have_descriptions(self):
        for fs in self.FLOW_REGISTRY:
            assert fs.description, f"{fs.name}: FlowSpec should have a description"

    # -- alert-test FlowSpec --

    def test_alert_test_has_test_tag(self):
        fs = next(f for f in self.FLOW_REGISTRY if f.name == "alert-test")
        assert "test" in fs.tags

    def test_alert_test_has_alerting_tag(self):
        fs = next(f for f in self.FLOW_REGISTRY if f.name == "alert-test")
        assert "alerting" in fs.tags

    def test_alert_test_has_should_fail_parameter(self):
        """alert-test must default to should_fail=False (only fails when explicitly triggered)."""
        fs = next(f for f in self.FLOW_REGISTRY if f.name == "alert-test")
        assert fs.parameters is not None
        assert fs.parameters.get("should_fail") is False

    def test_alert_test_has_no_schedule(self):
        """alert-test must NOT have a schedule — it should only run manually."""
        fs = next(f for f in self.FLOW_REGISTRY if f.name == "alert-test")
        assert fs.cron is None, "alert-test should not have a cron schedule"
        assert fs.interval is None, "alert-test should not have an interval schedule"
        assert fs.rrule is None, "alert-test should not have an rrule schedule"
        assert fs.schedules is None, "alert-test should not have raw schedules"
        assert fs.build_schedules() is None, "alert-test build_schedules() must return None"

    def test_alert_test_points_to_existing_file(self):
        fs = next(f for f in self.FLOW_REGISTRY if f.name == "alert-test")
        assert (FLOWS_DIR / fs.path).exists(), f"alert-test flow file missing: {fs.path}"

    def test_alert_test_func_name(self):
        fs = next(f for f in self.FLOW_REGISTRY if f.name == "alert-test")
        assert fs.func == "alert_test_flow"

    def test_alert_test_description_mentions_alert(self):
        fs = next(f for f in self.FLOW_REGISTRY if f.name == "alert-test")
        desc = fs.description.lower()
        assert "alert" in desc or "fail" in desc, (
            "alert-test description should mention alerting or failure"
        )


class TestSharedUtils:
    """Tests for flows/shared_utils.py constants and helpers."""

    SHARED_UTILS = FLOWS_DIR / "shared_utils.py"

    def test_shared_utils_exists(self):
        assert self.SHARED_UTILS.exists()

    def test_shared_utils_schema_constant(self):
        content = self.SHARED_UTILS.read_text()
        assert "SCHEMA" in content
        assert "_DATABASE" in content
        assert "_SCHEMA" in content

    def test_shared_utils_has_table_name(self):
        content = self.SHARED_UTILS.read_text()
        assert "def table_name" in content

    def test_shared_utils_has_get_git_sha(self):
        content = self.SHARED_UTILS.read_text()
        assert "def get_git_sha" in content

    def test_get_git_sha_returns_string(self):
        """get_git_sha() should return a non-empty string."""
        import importlib.util

        spec = importlib.util.spec_from_file_location("shared_utils", self.SHARED_UTILS)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        sha = mod.get_git_sha()
        assert isinstance(sha, str)
        assert len(sha) > 0

    def test_get_git_sha_short_is_7_chars(self):
        """Short SHA should be ~7 characters (or 'unknown' if not in a repo)."""
        import importlib.util

        spec = importlib.util.spec_from_file_location("shared_utils", self.SHARED_UTILS)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        sha = mod.get_git_sha(short=True)
        if sha != "unknown":
            assert 7 <= len(sha) <= 12  # short SHA is typically 7-12 chars

    def test_shared_utils_table_name_output(self):
        """table_name('FOO') should return 'PREFECT_DB.PREFECT_SCHEMA.FOO'."""
        import importlib.util

        spec = importlib.util.spec_from_file_location("shared_utils", self.SHARED_UTILS)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert mod.table_name("FOO") == "PREFECT_DB.PREFECT_SCHEMA.FOO"


class TestFormatters:
    """Tests for flows/analytics/reports/formatters.py."""

    FORMATTERS = FLOWS_DIR / "analytics" / "reports" / "formatters.py"

    def test_formatters_exists(self):
        assert self.FORMATTERS.exists()

    def test_format_currency(self):
        import importlib.util

        spec = importlib.util.spec_from_file_location("formatters", self.FORMATTERS)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert mod.format_currency(1234.5) == "$1,234.50"

    def test_format_pct(self):
        import importlib.util

        spec = importlib.util.spec_from_file_location("formatters", self.FORMATTERS)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert mod.format_pct(98.7) == "98.7%"

    def test_format_row(self):
        import importlib.util

        spec = importlib.util.spec_from_file_location("formatters", self.FORMATTERS)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        result = mod.format_row("UNITED STATES", 1234567.89, 45.2)
        assert "UNITED STATES" in result
        assert "$1,234,567.89" in result
        assert "45.2%" in result


class TestCostAttribution:
    """Tests for QUERY_TAG cost attribution in shared_utils.py."""

    SHARED_UTILS = FLOWS_DIR / "shared_utils.py"

    @pytest.fixture(autouse=True)
    def _import_shared_utils(self):
        """Import shared_utils for testing."""
        import importlib.util

        spec = importlib.util.spec_from_file_location("shared_utils", self.SHARED_UTILS)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        self.mod = mod
        yield

    def test_resolve_query_tag_custom(self):
        """Custom query_tag should pass through unchanged."""
        assert self.mod._resolve_query_tag("my/custom") == "my/custom"

    def test_resolve_query_tag_fallback(self):
        """Without Prefect runtime, should fall back to prefect/unknown."""
        assert self.mod._resolve_query_tag() == "prefect/unknown"

    def test_resolve_query_tag_empty_is_fallback(self):
        """Empty string should trigger auto-resolve, not pass through."""
        assert self.mod._resolve_query_tag("") == "prefect/unknown"

    def test_get_snowflake_connection_exists(self):
        assert callable(self.mod.get_snowflake_connection)

    def test_execute_query_exists(self):
        assert callable(self.mod.execute_query)

    def test_execute_ddl_exists(self):
        assert callable(self.mod.execute_ddl)

    def test_shared_utils_has_session_parameters(self):
        """The connection helper must set QUERY_TAG in session_parameters."""
        source = self.SHARED_UTILS.read_text()
        assert "session_parameters" in source
        assert "QUERY_TAG" in source

    def test_shared_utils_has_prefect_runtime(self):
        """QUERY_TAG auto-resolve should use prefect.runtime."""
        source = self.SHARED_UTILS.read_text()
        assert "prefect.runtime" in source or "flow_run.flow_name" in source


class TestConnectionConsolidation:
    """Ensure flow files delegate to shared_utils instead of inline connections."""

    # Files that previously had their own connection logic
    REFACTORED_FILES = [
        FLOWS_DIR / "snowflake_flow.py",
        FLOWS_DIR / "e2e_test_flow.py",
    ]

    @pytest.mark.parametrize(
        "flow_path",
        [
            FLOWS_DIR / "snowflake_flow.py",
            FLOWS_DIR / "e2e_test_flow.py",
            FLOWS_DIR / "data_quality_flow.py",
            FLOWS_DIR / "health_check_flow.py",
            FLOWS_DIR / "stage_cleanup_flow.py",
        ],
        ids=[
            "snowflake_flow",
            "e2e_test_flow",
            "data_quality_flow",
            "health_check_flow",
            "stage_cleanup_flow",
        ],
    )
    def test_no_inline_snowflake_connector_connect(self, flow_path):
        """Flow files must not call snowflake.connector.connect() directly."""
        source = flow_path.read_text()
        assert "snowflake.connector.connect(" not in source, (
            f"{flow_path.name}: should use shared_utils instead of inline connector"
        )

    @pytest.mark.parametrize(
        "flow_path",
        [
            FLOWS_DIR / "snowflake_flow.py",
            FLOWS_DIR / "e2e_test_flow.py",
            FLOWS_DIR / "data_quality_flow.py",
            FLOWS_DIR / "health_check_flow.py",
            FLOWS_DIR / "stage_cleanup_flow.py",
        ],
        ids=[
            "snowflake_flow",
            "e2e_test_flow",
            "data_quality_flow",
            "health_check_flow",
            "stage_cleanup_flow",
        ],
    )
    def test_imports_from_shared_utils(self, flow_path):
        """Flow files must import connection helpers from shared_utils."""
        source = flow_path.read_text()
        assert "from shared_utils import" in source, (
            f"{flow_path.name}: should import from shared_utils"
        )

    def test_e2e_no_get_connection_helper(self):
        """e2e_test_flow.py must not define its own _get_connection."""
        source = (FLOWS_DIR / "e2e_test_flow.py").read_text()
        assert "def _get_connection" not in source

    def test_e2e_no_private_execute(self):
        """e2e_test_flow.py must not define its own _execute helpers."""
        source = (FLOWS_DIR / "e2e_test_flow.py").read_text()
        assert "def _execute(" not in source
        assert "def _execute_ddl(" not in source


class TestSfHelpersWrapper:
    """Tests for analytics/sf_helpers.py thin wrapper."""

    SF_HELPERS = FLOWS_DIR / "analytics" / "sf_helpers.py"

    def test_sf_helpers_exists(self):
        """sf_helpers.py must exist for import stress tests."""
        assert self.SF_HELPERS.exists()

    def test_sf_helpers_delegates_to_shared_utils(self):
        """sf_helpers.py must import from shared_utils, not inline connector."""
        source = self.SF_HELPERS.read_text()
        assert "from shared_utils import" in source

    def test_sf_helpers_no_inline_connector(self):
        """sf_helpers.py must not call snowflake.connector directly."""
        source = self.SF_HELPERS.read_text()
        assert "snowflake.connector.connect(" not in source

    def test_sf_helpers_exports_get_connection(self):
        """sf_helpers.py must still export get_connection."""
        source = self.SF_HELPERS.read_text()
        assert "def get_connection" in source

    def test_sf_helpers_exports_execute_query(self):
        source = self.SF_HELPERS.read_text()
        assert "def execute_query" in source

    def test_sf_helpers_exports_execute_ddl(self):
        source = self.SF_HELPERS.read_text()
        assert "def execute_ddl" in source


class TestHooks:
    """Tests for flows/hooks.py monitoring hooks."""

    HOOKS = FLOWS_DIR / "hooks.py"

    def test_hooks_exists(self):
        assert self.HOOKS.exists()

    def test_hooks_has_on_flow_failure(self):
        source = self.HOOKS.read_text()
        assert "def on_flow_failure" in source

    def test_hooks_has_on_flow_completion(self):
        source = self.HOOKS.read_text()
        assert "def on_flow_completion" in source

    def test_hooks_logs_flow_name(self):
        """Failure hook must log the flow name for alerting."""
        source = self.HOOKS.read_text()
        assert "flow_name" in source

    def test_hooks_is_importable(self):
        import importlib.util

        spec = importlib.util.spec_from_file_location("hooks", self.HOOKS)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert callable(mod.on_flow_failure)
        assert callable(mod.on_flow_completion)


class TestWebhookAlerting:
    """Tests for webhook/Slack alerting in hooks.py."""

    HOOKS = FLOWS_DIR / "hooks.py"

    def test_reads_slack_url_from_env(self):
        source = self.HOOKS.read_text()
        assert "SLACK_WEBHOOK_URL" in source

    def test_reads_generic_webhook_from_env(self):
        source = self.HOOKS.read_text()
        assert "ALERT_WEBHOOK_URL" in source

    def test_no_hardcoded_urls(self):
        source = self.HOOKS.read_text()
        assert "hooks.slack.com" not in source
        assert "http://localhost" not in source

    def test_uses_env_vars_not_constants(self):
        source = self.HOOKS.read_text()
        # Webhook URLs use get_secret_value() (Prefect block with env fallback)
        assert (
            'get_secret_value("slack-webhook-url", "SLACK_WEBHOOK_URL")' in source
            or "os.environ.get('SLACK_WEBHOOK_URL'" in source
        )
        assert (
            'get_secret_value("alert-webhook-url", "ALERT_WEBHOOK_URL")' in source
            or "os.environ.get('ALERT_WEBHOOK_URL'" in source
        )

    def test_has_post_json_helper(self):
        source = self.HOOKS.read_text()
        assert "def _post_json" in source

    def test_slack_uses_block_kit(self):
        source = self.HOOKS.read_text()
        assert '"blocks"' in source or "'blocks'" in source

    def test_failure_hook_sends_slack_when_configured(self):
        """on_flow_failure should call Slack webhook when SLACK_WEBHOOK_URL is set."""
        source = self.HOOKS.read_text()
        # The failure hook should conditionally check SLACK_WEBHOOK_URL
        assert "if SLACK_WEBHOOK_URL" in source

    def test_failure_hook_sends_generic_when_configured(self):
        source = self.HOOKS.read_text()
        assert "if ALERT_WEBHOOK_URL" in source

    def test_webhook_errors_are_caught(self):
        """Webhook failures should be caught, not crash the flow."""
        source = self.HOOKS.read_text()
        assert "except" in source and ("URLError" in source or "OSError" in source)

    def test_env_example_has_webhook_vars(self):
        env_example = (FLOWS_DIR.parent / ".env.example").read_text()
        assert "SLACK_WEBHOOK_URL" in env_example
        assert "ALERT_WEBHOOK_URL" in env_example


class TestFlowRetries:
    """Ensure production flows have retries and failure hooks configured."""

    PRODUCTION_FLOWS = [
        ("snowflake_flow.py", "snowflake-etl"),
        ("external_api_flow.py", "external-api-flow"),
        ("e2e_test_flow.py", "e2e-pipeline-test"),
    ]

    @pytest.mark.parametrize("filename,_", PRODUCTION_FLOWS, ids=[f[0] for f in PRODUCTION_FLOWS])
    def test_has_retries(self, filename, _):
        source = (FLOWS_DIR / filename).read_text()
        assert "retries=" in source, f"{filename}: @flow should have retries"

    @pytest.mark.parametrize("filename,_", PRODUCTION_FLOWS, ids=[f[0] for f in PRODUCTION_FLOWS])
    def test_has_retry_delay(self, filename, _):
        source = (FLOWS_DIR / filename).read_text()
        assert "retry_delay_seconds=" in source, (
            f"{filename}: @flow should have retry_delay_seconds"
        )

    @pytest.mark.parametrize("filename,_", PRODUCTION_FLOWS, ids=[f[0] for f in PRODUCTION_FLOWS])
    def test_has_failure_hook(self, filename, _):
        source = (FLOWS_DIR / filename).read_text()
        assert "on_failure=" in source, f"{filename}: @flow should have on_failure hook"
        assert "on_flow_failure" in source

    @pytest.mark.parametrize("filename,_", PRODUCTION_FLOWS, ids=[f[0] for f in PRODUCTION_FLOWS])
    def test_imports_hooks(self, filename, _):
        source = (FLOWS_DIR / filename).read_text()
        assert "from hooks import" in source

    def test_analytics_revenue_has_retries(self):
        source = (FLOWS_DIR / "analytics" / "revenue_flow.py").read_text()
        assert "retries=" in source
        assert "on_failure=" in source

    def test_quarterly_report_has_retries(self):
        source = (FLOWS_DIR / "analytics" / "reports" / "quarterly_flow.py").read_text()
        assert "retries=" in source
        assert "on_failure=" in source


class TestDeploymentDiff:
    """Tests for the --diff deployment plan feature in deploy.py."""

    def test_deploy_has_diff_flag(self):
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "--diff" in source

    def test_deploy_parse_args_returns_diff(self):
        """_parse_args return type should include diff boolean."""
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "validate, diff" in source

    def test_deploy_has_diff_pool_function(self):
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "async def _diff_pool(" in source

    def test_diff_compares_version(self):
        """Diff should compare deployment versions."""
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "live_version" in source
        assert "desired_version" in source

    def test_diff_compares_tags(self):
        """Diff should compare deployment tags."""
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "live_tags" in source
        assert "desired_tags" in source

    def test_diff_compares_parameters(self):
        """Diff should compare deployment parameters."""
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "live_params" in source
        assert "desired_params" in source

    def test_diff_compares_entrypoint(self):
        """Diff should compare deployment entrypoints."""
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "live_entrypoint" in source
        assert "desired_entrypoint" in source

    def test_diff_compares_concurrency(self):
        """Diff should compare concurrency limits."""
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "live_conc" in source
        assert "concurrency_limit" in source

    def test_diff_shows_plan_summary(self):
        """Diff output should end with a plan summary line."""
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "to add" in source
        assert "to change" in source
        assert "unchanged" in source

    def test_diff_uses_plan_symbols(self):
        """Diff should use +/~/= symbols for new/changed/unchanged."""
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert '"  + "' in source or "'  + '" in source or "  + {" in source
        assert '"  ~ "' in source or "'  ~ '" in source or "  ~ {" in source
        assert '"  = "' in source or "'  = '" in source or "  = {" in source

    def test_diff_returns_counts(self):
        """_diff_pool should return (added, changed, unchanged) tuple."""
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "return added, changed, unchanged" in source


class TestMultiEnvironment:
    """Tests for PREFECT_ENV multi-environment support."""

    SHARED_UTILS = FLOWS_DIR / "shared_utils.py"

    def test_shared_utils_schema_from_env(self):
        """SCHEMA should be derived from SNOWFLAKE_DATABASE + SNOWFLAKE_SCHEMA env vars."""
        source = self.SHARED_UTILS.read_text()
        assert "SNOWFLAKE_DATABASE" in source
        assert "SNOWFLAKE_SCHEMA" in source

    def test_shared_utils_has_prefect_env(self):
        """shared_utils should export PREFECT_ENV."""
        source = self.SHARED_UTILS.read_text()
        assert "PREFECT_ENV" in source

    def test_shared_utils_schema_default_unchanged(self):
        """Default SCHEMA should still be PREFECT_DB.PREFECT_SCHEMA."""
        import importlib.util

        spec = importlib.util.spec_from_file_location("shared_utils_env_test", self.SHARED_UTILS)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert mod.SCHEMA == "PREFECT_DB.PREFECT_SCHEMA"

    def test_deploy_has_prefect_env(self):
        """deploy.py should read PREFECT_ENV for deployment naming."""
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "PREFECT_ENV" in source

    def test_deploy_env_prefixes_name(self):
        """deploy.py should prefix deployment names with env when set."""
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "PREFECT_ENV}/{deploy_name}" in source or "PREFECT_ENV}/{deploy_name}" in source

    def test_deploy_env_adds_env_tag(self):
        """deploy.py should add env:<name> tag when PREFECT_ENV is set."""
        source = (FLOWS_DIR / "deploy.py").read_text()
        assert "env:{PREFECT_ENV}" in source or "env:" in source

    def test_diff_mirrors_env_naming(self):
        """_diff_pool should use the same PREFECT_ENV naming as deploy_to_pool."""
        source = (FLOWS_DIR / "deploy.py").read_text()
        # Both deploy_to_pool and _diff_pool should reference PREFECT_ENV
        assert source.count("PREFECT_ENV") >= 4  # declaration + deploy + diff + diff tags

    def test_env_example_has_prefect_env(self):
        """The .env.example should document PREFECT_ENV."""
        env_example = FLOWS_DIR.parent / ".env.example"
        source = env_example.read_text()
        assert "PREFECT_ENV" in source

    def test_env_example_has_snowflake_database(self):
        """The .env.example should document SNOWFLAKE_DATABASE."""
        env_example = FLOWS_DIR.parent / ".env.example"
        source = env_example.read_text()
        assert "SNOWFLAKE_DATABASE" in source

    def test_env_example_has_git_vars(self):
        """The .env.example should document GIT_REPO_URL and GIT_BRANCH."""
        env_example = FLOWS_DIR.parent / ".env.example"
        source = env_example.read_text()
        assert "GIT_REPO_URL" in source
        assert "GIT_BRANCH" in source


class TestAutoScaling:
    """Tests for auto-scaling: worker resource limits, MAX_INSTANCES, pools config."""

    SPECS_DIR = FLOWS_DIR.parent / "specs"
    SQL_DIR = FLOWS_DIR.parent / "sql"

    def test_worker_spec_has_resource_limits(self):
        """pf_worker.yaml should define CPU/memory resource limits."""
        source = (self.SPECS_DIR / "pf_worker.yaml").read_text()
        assert "resources:" in source
        assert "limits:" in source
        assert "cpu:" in source
        assert "memory:" in source

    def test_worker_spec_has_resource_requests(self):
        """pf_worker.yaml should define CPU/memory resource requests."""
        source = (self.SPECS_DIR / "pf_worker.yaml").read_text()
        assert "requests:" in source

    def test_create_sql_worker_max_instances(self):
        """07_create_services.sql should set MAX_INSTANCES > 1 for PF_WORKER."""
        source = (self.SQL_DIR / "07_create_services.sql").read_text()
        # Find the PF_WORKER block and check MAX_INSTANCES
        worker_idx = source.index("PF_WORKER")
        worker_block = source[worker_idx : worker_idx + 300]
        assert "MAX_INSTANCES = 3" in worker_block

    def test_update_sql_worker_has_max_instances(self):
        """07b_update_services.sql should set MAX_INSTANCES for PF_WORKER."""
        source = (self.SQL_DIR / "07b_update_services.sql").read_text()
        assert "MAX_INSTANCES = 3" in source

    def test_update_sql_worker_has_min_instances(self):
        """07b_update_services.sql should set MIN_INSTANCES for PF_WORKER."""
        source = (self.SQL_DIR / "07b_update_services.sql").read_text()
        assert "MIN_INSTANCES = 1" in source

    def test_pools_yaml_has_max_instances(self):
        """pools.yaml SPCS pool should document max_instances."""
        import yaml

        pools_file = FLOWS_DIR.parent / "pools.yaml"
        with open(pools_file) as f:
            data = yaml.safe_load(f)
        spcs = data["pools"]["spcs"]
        assert "max_instances" in spcs
        assert spcs["max_instances"] >= 2

    def test_pools_yaml_has_min_instances(self):
        """pools.yaml SPCS pool should document min_instances."""
        import yaml

        pools_file = FLOWS_DIR.parent / "pools.yaml"
        with open(pools_file) as f:
            data = yaml.safe_load(f)
        spcs = data["pools"]["spcs"]
        assert "min_instances" in spcs
        assert spcs["min_instances"] >= 1

    def test_worker_memory_limit_at_least_2gi(self):
        """Worker memory limit should be at least 2Gi for flow execution."""
        source = (self.SPECS_DIR / "pf_worker.yaml").read_text()
        import re

        match = re.search(r"limits:.*?memory:\s*(\d+)Gi", source, re.DOTALL)
        assert match, "memory limit not found in Gi format"
        assert int(match.group(1)) >= 2


class TestAutomationsScript:
    """Tests for scripts/setup_automations.py."""

    SCRIPT = FLOWS_DIR.parent / "scripts" / "setup_automations.py"

    def test_script_exists(self):
        assert self.SCRIPT.exists()

    def test_parses_as_valid_python(self):
        source = self.SCRIPT.read_text()
        ast.parse(source)

    def test_reads_webhook_urls_from_env(self):
        source = self.SCRIPT.read_text()
        assert "SLACK_WEBHOOK_URL" in source
        assert "ALERT_WEBHOOK_URL" in source

    def test_no_hardcoded_webhook_urls(self):
        source = self.SCRIPT.read_text()
        assert "hooks.slack.com" not in source

    def test_has_dry_run_flag(self):
        source = self.SCRIPT.read_text()
        assert "--dry-run" in source

    def test_has_delete_flag(self):
        source = self.SCRIPT.read_text()
        assert "--delete" in source

    def test_uses_prefect_automation_api(self):
        source = self.SCRIPT.read_text()
        assert "create_automation" in source
        assert "AutomationCore" in source

    def test_uses_event_trigger(self):
        source = self.SCRIPT.read_text()
        assert "EventTrigger" in source
        assert "prefect.flow-run.Failed" in source

    def test_is_idempotent(self):
        """Script should check for existing automations before creating."""
        source = self.SCRIPT.read_text()
        assert "read_automations_by_name" in source


# ── New business flow tests ───────────────────────────────────────────────


class TestDataQualityFlow:
    """Tests for flows/data_quality_flow.py."""

    FLOW = FLOWS_DIR / "data_quality_flow.py"

    def test_file_exists(self):
        assert self.FLOW.exists()

    def test_parses_as_valid_python(self):
        ast.parse(self.FLOW.read_text())

    def test_has_flow_decorator(self):
        source = self.FLOW.read_text()
        assert re.search(r"@flow", source)

    def test_has_task_decorators(self):
        source = self.FLOW.read_text()
        assert re.search(r"@task", source)

    def test_flow_name(self):
        source = self.FLOW.read_text()
        assert 'name="data-quality-check"' in source

    def test_uses_on_flow_failure_hook(self):
        source = self.FLOW.read_text()
        assert "on_failure=[on_flow_failure]" in source

    def test_imports_shared_utils(self):
        source = self.FLOW.read_text()
        assert "from shared_utils import" in source

    def test_has_table_checks_config(self):
        source = self.FLOW.read_text()
        assert "TABLE_CHECKS" in source

    def test_checks_row_count(self):
        source = self.FLOW.read_text()
        assert "check_row_count" in source

    def test_checks_freshness(self):
        source = self.FLOW.read_text()
        assert "check_freshness" in source

    def test_has_main_block(self):
        source = self.FLOW.read_text()
        assert 'if __name__ == "__main__"' in source

    def test_uses_table_name_helper(self):
        source = self.FLOW.read_text()
        assert "table_name" in source


class TestStageCleanupFlow:
    """Tests for flows/stage_cleanup_flow.py."""

    FLOW = FLOWS_DIR / "stage_cleanup_flow.py"

    def test_file_exists(self):
        assert self.FLOW.exists()

    def test_parses_as_valid_python(self):
        ast.parse(self.FLOW.read_text())

    def test_has_flow_decorator(self):
        source = self.FLOW.read_text()
        assert re.search(r"@flow", source)

    def test_has_task_decorators(self):
        source = self.FLOW.read_text()
        assert re.search(r"@task", source)

    def test_flow_name(self):
        source = self.FLOW.read_text()
        assert 'name="stage-cleanup"' in source

    def test_uses_on_flow_failure_hook(self):
        source = self.FLOW.read_text()
        assert "on_failure=[on_flow_failure]" in source

    def test_imports_shared_utils(self):
        source = self.FLOW.read_text()
        assert "from shared_utils import" in source

    def test_has_retention_config(self):
        source = self.FLOW.read_text()
        assert "DEFAULT_RETENTION_DAYS" in source

    def test_has_stages_list(self):
        source = self.FLOW.read_text()
        assert "STAGES_TO_CLEAN" in source

    def test_lists_stage_files(self):
        source = self.FLOW.read_text()
        assert "list_stage_files" in source

    def test_removes_stale_files(self):
        source = self.FLOW.read_text()
        assert "remove_files" in source

    def test_has_main_block(self):
        source = self.FLOW.read_text()
        assert 'if __name__ == "__main__"' in source

    def test_retention_from_env(self):
        source = self.FLOW.read_text()
        assert "STAGE_RETENTION_DAYS" in source


class TestHealthCheckFlow:
    """Tests for flows/health_check_flow.py."""

    FLOW = FLOWS_DIR / "health_check_flow.py"

    def test_file_exists(self):
        assert self.FLOW.exists()

    def test_parses_as_valid_python(self):
        ast.parse(self.FLOW.read_text())

    def test_has_flow_decorator(self):
        source = self.FLOW.read_text()
        assert re.search(r"@flow", source)

    def test_has_task_decorators(self):
        source = self.FLOW.read_text()
        assert re.search(r"@task", source)

    def test_flow_name(self):
        source = self.FLOW.read_text()
        assert 'name="health-check"' in source

    def test_uses_on_flow_failure_hook(self):
        source = self.FLOW.read_text()
        assert "on_failure=[on_flow_failure]" in source

    def test_imports_shared_utils(self):
        source = self.FLOW.read_text()
        assert "from shared_utils import" in source

    def test_checks_snowflake_connectivity(self):
        source = self.FLOW.read_text()
        assert "check_snowflake_connectivity" in source

    def test_checks_services(self):
        source = self.FLOW.read_text()
        assert "check_services_running" in source

    def test_checks_compute_pools(self):
        source = self.FLOW.read_text()
        assert "check_compute_pools" in source

    def test_checks_database_objects(self):
        source = self.FLOW.read_text()
        assert "check_database_objects" in source

    def test_expected_services_list(self):
        source = self.FLOW.read_text()
        assert "EXPECTED_SERVICES" in source
        assert "PF_SERVER" in source
        assert "PF_MONITOR" in source

    def test_has_main_block(self):
        source = self.FLOW.read_text()
        assert 'if __name__ == "__main__"' in source

    def test_raises_on_failure(self):
        source = self.FLOW.read_text()
        assert "raise RuntimeError" in source
