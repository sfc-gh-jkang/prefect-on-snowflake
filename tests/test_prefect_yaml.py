"""Tests for prefect.yaml — declarative deployment configuration.

Validates structure, completeness, and correctness of the prefect.yaml file
that replaced the custom deploy.py deployment mechanism.
"""

import ast
import contextlib

import pytest
import yaml
from conftest import FLOWS_DIR, POOL_NAMES, POOLS, PROJECT_DIR

PREFECT_YAML = PROJECT_DIR / "prefect.yaml"


@pytest.fixture(scope="module")
def prefect_config():
    """Parse prefect.yaml once per module."""
    assert PREFECT_YAML.exists(), "prefect.yaml must exist at project root"
    return yaml.safe_load(PREFECT_YAML.read_text())


@pytest.fixture(scope="module")
def deployments(prefect_config):
    """Return the list of deployment dicts."""
    return prefect_config["deployments"]


@pytest.fixture(scope="module")
def deployment_map(deployments):
    """Return dict of deployment name -> deployment config."""
    return {d["name"]: d for d in deployments}


# ---------------------------------------------------------------------------
# FLOW_REGISTRY mirror — the 6 flows that should each have 2 pool variants
#
# This is the single source of truth for what prefect.yaml SHOULD contain.
# Each entry mirrors one FlowSpec from deploy.py's FLOW_REGISTRY plus the
# actual @flow() decorator and function signature from the source file.
# ---------------------------------------------------------------------------
EXPECTED_FLOWS = [
    {
        "base_name": "example-flow",
        "entrypoint_file": "example_flow.py",
        "func": "example_flow",
        "flow_name": "example-flow",
        "tags_contain": ["example"],
        "parameters": {"name": "Prefect-SPCS"},
        "func_params": {"name": str},
        "func_defaults": {"name": "World"},
        "schedule_type": "interval",
        "has_concurrency": False,
    },
    {
        "base_name": "snowflake-etl",
        "entrypoint_file": "snowflake_flow.py",
        "func": "snowflake_etl",
        "flow_name": "snowflake-etl",
        "tags_contain": ["snowflake"],
        "parameters": None,
        "func_params": {},
        "func_defaults": {},
        "schedule_type": "cron",
        "has_concurrency": False,
    },
    {
        "base_name": "external-api",
        "entrypoint_file": "external_api_flow.py",
        "func": "external_api_flow",
        "flow_name": "external-api-flow",
        "tags_contain": ["eai"],
        "parameters": None,
        "func_params": {"url": str},
        "func_defaults": {"url": "https://httpbin.org/get"},
        "schedule_type": None,
        "has_concurrency": False,
    },
    {
        "base_name": "e2e-test",
        "entrypoint_file": "e2e_test_flow.py",
        "func": "e2e_pipeline_test",
        "flow_name": "e2e-pipeline-test",
        "tags_contain": ["e2e"],
        "parameters": None,
        "func_params": {},
        "func_defaults": {},
        "schedule_type": None,
        "has_concurrency": True,
    },
    {
        "base_name": "analytics-revenue",
        "entrypoint_file": "analytics/revenue_flow.py",
        "func": "analytics_revenue",
        "flow_name": "analytics-revenue",
        "tags_contain": ["analytics"],
        "parameters": None,
        "func_params": {},
        "func_defaults": {},
        "schedule_type": None,
        "has_concurrency": False,
    },
    {
        "base_name": "quarterly-report",
        "entrypoint_file": "analytics/reports/quarterly_flow.py",
        "func": "quarterly_report",
        "flow_name": "quarterly-report",
        "tags_contain": ["analytics", "nested"],
        "parameters": None,
        "func_params": {},
        "func_defaults": {},
        "schedule_type": "cron",
        "has_concurrency": True,
    },
    {
        "base_name": "data-quality",
        "entrypoint_file": "data_quality_flow.py",
        "func": "data_quality_check",
        "flow_name": "data-quality-check",
        "tags_contain": ["data-quality", "monitoring"],
        "parameters": None,
        "func_params": {},
        "func_defaults": {},
        "schedule_type": "cron",
        "has_concurrency": False,
    },
    {
        "base_name": "stage-cleanup",
        "entrypoint_file": "stage_cleanup_flow.py",
        "func": "stage_cleanup",
        "flow_name": "stage-cleanup",
        "tags_contain": ["maintenance", "storage"],
        "parameters": {"retention_days": 30},
        "func_params": {"retention_days": int},
        "func_defaults": {"retention_days": 30},
        "schedule_type": "cron",
        "has_concurrency": False,
    },
    {
        "base_name": "health-check",
        "entrypoint_file": "health_check_flow.py",
        "func": "health_check",
        "flow_name": "health-check",
        "tags_contain": ["monitoring", "infra"],
        "parameters": None,
        "func_params": {},
        "func_defaults": {},
        "schedule_type": "interval",
        "has_concurrency": True,
        # SPCS-only: checks SHOW SERVICES / compute pools — meaningless on external workers
        "pools": ["spcs"],
    },
]

POOL_SUFFIXES = {key: cfg["suffix"] for key, cfg in POOLS.items()}
POOL_TAGS = {key: cfg["tag"] for key, cfg in POOLS.items()}

# All pools have deployments in prefect.yaml.
ACTIVE_POOLS = POOLS
ACTIVE_POOL_SUFFIXES = {key: cfg["suffix"] for key, cfg in ACTIVE_POOLS.items()}


def _flow_pool_keys(flow_spec: dict) -> list[str]:
    """Return the ACTIVE_POOLS keys a flow should be deployed to.

    If flow_spec has a ``"pools"`` list, only those pool keys are used.
    Otherwise, all ACTIVE_POOLS keys are used (the common case).
    """
    restricted = flow_spec.get("pools")
    if restricted:
        return [k for k in ACTIVE_POOLS if k in restricted]
    return list(ACTIVE_POOLS.keys())


EXPECTED_TOTAL_DEPLOYMENTS = sum(len(_flow_pool_keys(f)) for f in EXPECTED_FLOWS)


# ===========================================================================
# Top-level structure
# ===========================================================================
class TestTopLevelStructure:
    def test_file_exists(self):
        assert PREFECT_YAML.exists()

    def test_has_pull_section(self, prefect_config):
        assert "pull" in prefect_config

    def test_has_deployments_section(self, prefect_config):
        assert "deployments" in prefect_config

    def test_pull_has_git_clone_step(self, prefect_config):
        pull = prefect_config["pull"]
        step_keys = []
        for step in pull:
            step_keys.extend(step.keys())
        assert any("git_clone" in k for k in step_keys)

    def test_pull_has_set_working_directory(self, prefect_config):
        pull = prefect_config["pull"]
        step_keys = []
        for step in pull:
            step_keys.extend(step.keys())
        assert any("set_working_directory" in k for k in step_keys)

    def test_git_clone_uses_env_vars(self, prefect_config):
        """Git clone should use template variables, not hardcoded values."""
        pull = prefect_config["pull"]
        clone_step = next(s for s in pull if any("git_clone" in k for k in s))
        clone_config = next(v for k, v in clone_step.items() if "git_clone" in k)
        assert "{{ $GIT_REPO_URL }}" in str(clone_config.get("repository", ""))
        assert "{{ $GIT_BRANCH }}" in str(clone_config.get("branch", ""))
        assert "{{ $GIT_ACCESS_TOKEN }}" in str(clone_config.get("access_token", ""))

    def test_git_clone_has_id(self, prefect_config):
        """Clone step needs an id so set_working_directory can reference it."""
        pull = prefect_config["pull"]
        clone_step = next(s for s in pull if any("git_clone" in k for k in s))
        clone_config = next(v for k, v in clone_step.items() if "git_clone" in k)
        assert clone_config.get("id") == "clone"

    def test_working_directory_references_clone(self, prefect_config):
        pull = prefect_config["pull"]
        wd_step = next(s for s in pull if any("set_working_directory" in k for k in s))
        wd_config = next(v for k, v in wd_step.items() if "set_working_directory" in k)
        directory = str(wd_config.get("directory", ""))
        assert "clone.directory" in directory
        assert directory.endswith("/flows"), (
            f"Working directory must end with '/flows', got: {directory}"
        )

    def test_no_hardcoded_secrets(self):
        """prefect.yaml must not contain hardcoded secrets or tokens."""
        content = PREFECT_YAML.read_text()
        assert "glpat-" not in content
        assert "ghp_" not in content
        assert "eyJ" not in content  # JWT tokens


# ===========================================================================
# Deployment completeness
# ===========================================================================
class TestDeploymentCompleteness:
    def test_correct_total_count(self, deployments):
        """Total deployments = sum of per-flow pool counts (some flows are pool-restricted)."""
        assert len(deployments) == EXPECTED_TOTAL_DEPLOYMENTS

    def test_all_deployment_names_unique(self, deployments):
        names = [d["name"] for d in deployments]
        assert len(names) == len(set(names))

    @pytest.mark.parametrize("flow", EXPECTED_FLOWS, ids=[f["base_name"] for f in EXPECTED_FLOWS])
    def test_has_local_variant(self, flow, deployment_map):
        suffix = POOL_SUFFIXES["spcs"]
        name = f"{flow['base_name']}-{suffix}"
        assert name in deployment_map, f"Missing deployment: {name}"

    @pytest.mark.parametrize("flow", EXPECTED_FLOWS, ids=[f["base_name"] for f in EXPECTED_FLOWS])
    def test_has_gcp_variant(self, flow, deployment_map):
        if "gcp" not in _flow_pool_keys(flow):
            pytest.skip(f"{flow['base_name']} is not deployed to gcp pool")
        suffix = POOL_SUFFIXES["gcp"]
        name = f"{flow['base_name']}-{suffix}"
        assert name in deployment_map, f"Missing deployment: {name}"


# ===========================================================================
# Entrypoint correctness
# ===========================================================================
class TestEntrypoints:
    @pytest.mark.parametrize("flow", EXPECTED_FLOWS, ids=[f["base_name"] for f in EXPECTED_FLOWS])
    def test_entrypoint_format(self, flow, deployments):
        """Each entrypoint must be 'path/to/file.py:function_name'."""
        expected_ep = f"{flow['entrypoint_file']}:{flow['func']}"
        matching = [d for d in deployments if d["name"].startswith(flow["base_name"])]
        for d in matching:
            assert d["entrypoint"] == expected_ep, (
                f"{d['name']}: expected entrypoint {expected_ep}, got {d['entrypoint']}"
            )

    @pytest.mark.parametrize("flow", EXPECTED_FLOWS, ids=[f["base_name"] for f in EXPECTED_FLOWS])
    def test_entrypoint_file_exists(self, flow):
        """The Python file referenced in each entrypoint must exist."""
        file_path = FLOWS_DIR / flow["entrypoint_file"]
        assert file_path.exists(), f"Entrypoint file missing: {flow['entrypoint_file']}"

    @pytest.mark.parametrize("flow", EXPECTED_FLOWS, ids=[f["base_name"] for f in EXPECTED_FLOWS])
    def test_entrypoint_function_exists(self, flow):
        """The function referenced in each entrypoint must be defined in the file."""
        file_path = FLOWS_DIR / flow["entrypoint_file"]
        content = file_path.read_text()
        assert f"def {flow['func']}" in content, (
            f"Function {flow['func']} not found in {flow['entrypoint_file']}"
        )

    def test_entrypoints_no_flows_prefix(self, deployments):
        """Entrypoints must NOT start with 'flows/' — set_working_directory
        already points to flows/, so paths are relative to that directory."""
        for d in deployments:
            assert not d["entrypoint"].startswith("flows/"), (
                f"{d['name']}: entrypoint must not start with 'flows/' "
                f"(working directory is already flows/), got {d['entrypoint']}"
            )


# ===========================================================================
# Work pool assignment
# ===========================================================================
class TestWorkPools:
    def test_local_deployments_use_spcs_pool(self, deployments):
        suffix = POOL_SUFFIXES["spcs"]
        local_deps = [d for d in deployments if d["name"].endswith(f"-{suffix}")]
        for d in local_deps:
            assert d["work_pool"]["name"] == POOL_NAMES["spcs"], (
                f"{d['name']} should use {POOL_NAMES['spcs']}"
            )

    def test_gcp_deployments_use_gcp_pool(self, deployments):
        suffix = POOL_SUFFIXES["gcp"]
        gcp_deps = [d for d in deployments if d["name"].endswith(f"-{suffix}")]
        for d in gcp_deps:
            assert d["work_pool"]["name"] == POOL_NAMES["gcp"], (
                f"{d['name']} should use {POOL_NAMES['gcp']}"
            )

    def test_every_deployment_has_work_pool(self, deployments):
        for d in deployments:
            assert "work_pool" in d, f"{d['name']} missing work_pool"
            assert "name" in d["work_pool"], f"{d['name']} work_pool missing name"


# ===========================================================================
# Tags
# ===========================================================================
class TestTags:
    @pytest.mark.parametrize("flow", EXPECTED_FLOWS, ids=[f["base_name"] for f in EXPECTED_FLOWS])
    def test_flow_specific_tags(self, flow, deployments):
        """Each deployment should have the flow-specific tags."""
        matching = [d for d in deployments if d["name"].startswith(flow["base_name"])]
        for d in matching:
            for tag in flow["tags_contain"]:
                assert tag in d.get("tags", []), f"{d['name']} missing expected tag '{tag}'"

    def test_local_deployments_have_spcs_tag(self, deployments):
        suffix = POOL_SUFFIXES["spcs"]
        tag = POOL_TAGS["spcs"]
        local_deps = [d for d in deployments if d["name"].endswith(f"-{suffix}")]
        for d in local_deps:
            assert tag in d.get("tags", []), f"{d['name']} missing pool tag '{tag}'"

    def test_gcp_deployments_have_gcp_tag(self, deployments):
        suffix = POOL_SUFFIXES["gcp"]
        tag = POOL_TAGS["gcp"]
        gcp_deps = [d for d in deployments if d["name"].endswith(f"-{suffix}")]
        for d in gcp_deps:
            assert tag in d.get("tags", []), f"{d['name']} missing pool tag '{tag}'"


# ===========================================================================
# Schedules
# ===========================================================================
class TestSchedules:
    def test_example_flow_has_interval(self, deployment_map):
        for suffix in ACTIVE_POOL_SUFFIXES.values():
            d = deployment_map[f"example-flow-{suffix}"]
            schedules = d.get("schedules", [])
            assert len(schedules) >= 1
            assert schedules[0].get("interval") == 3600

    def test_snowflake_etl_has_daily_cron(self, deployment_map):
        for suffix in ACTIVE_POOL_SUFFIXES.values():
            d = deployment_map[f"snowflake-etl-{suffix}"]
            schedules = d.get("schedules", [])
            assert len(schedules) >= 1
            assert schedules[0].get("cron") == "0 6 * * *"
            assert schedules[0].get("timezone") == "UTC"

    def test_quarterly_report_has_quarterly_cron(self, deployment_map):
        for suffix in ACTIVE_POOL_SUFFIXES.values():
            d = deployment_map[f"quarterly-report-{suffix}"]
            schedules = d.get("schedules", [])
            assert len(schedules) >= 1
            assert schedules[0].get("cron") == "0 0 1 1,4,7,10 *"
            assert schedules[0].get("timezone") == "US/Pacific"

    def test_external_api_has_no_schedule(self, deployment_map):
        for suffix in ACTIVE_POOL_SUFFIXES.values():
            d = deployment_map[f"external-api-{suffix}"]
            assert not d.get("schedules"), f"external-api-{suffix} should have no schedule"

    def test_e2e_test_has_no_schedule(self, deployment_map):
        for suffix in ACTIVE_POOL_SUFFIXES.values():
            d = deployment_map[f"e2e-test-{suffix}"]
            assert not d.get("schedules"), f"e2e-test-{suffix} should have no schedule"


# ===========================================================================
# Concurrency limits
# ===========================================================================
class TestConcurrency:
    def test_e2e_test_has_concurrency_limit(self, deployment_map):
        for suffix in ACTIVE_POOL_SUFFIXES.values():
            d = deployment_map[f"e2e-test-{suffix}"]
            cl = d.get("concurrency_limit", {})
            assert cl.get("limit") == 1
            assert cl.get("collision_strategy") == "CANCEL_NEW"

    def test_quarterly_report_has_concurrency_limit(self, deployment_map):
        for suffix in ACTIVE_POOL_SUFFIXES.values():
            d = deployment_map[f"quarterly-report-{suffix}"]
            cl = d.get("concurrency_limit", {})
            assert cl.get("limit") == 1
            assert cl.get("collision_strategy") == "CANCEL_NEW"

    def test_example_flow_no_concurrency_limit(self, deployment_map):
        for suffix in ACTIVE_POOL_SUFFIXES.values():
            d = deployment_map[f"example-flow-{suffix}"]
            assert "concurrency_limit" not in d


# ===========================================================================
# enforce_parameter_schema
# ===========================================================================
class TestParameterSchema:
    def test_all_deployments_enforce_schema(self, deployments):
        """All deployments should enforce parameter schema."""
        for d in deployments:
            assert d.get("enforce_parameter_schema") is True, (
                f"{d['name']} should have enforce_parameter_schema: true"
            )


# ===========================================================================
# Descriptions
# ===========================================================================
class TestDescriptions:
    def test_all_deployments_have_descriptions(self, deployments):
        for d in deployments:
            assert d.get("description"), f"{d['name']} missing description"

    @pytest.mark.parametrize("flow", EXPECTED_FLOWS, ids=[f["base_name"] for f in EXPECTED_FLOWS])
    def test_paired_deployments_share_description(self, flow, deployments):
        """Both pool variants of the same flow should have matching descriptions."""
        matching = [d for d in deployments if d["name"].startswith(flow["base_name"])]
        descriptions = {d["description"] for d in matching}
        assert len(descriptions) == 1, (
            f"{flow['base_name']} variants have mismatched descriptions: {descriptions}"
        )


# ===========================================================================
# Consistency with deploy.py FLOW_REGISTRY
# ===========================================================================
class TestRegistryConsistency:
    """Ensure prefect.yaml matches the FLOW_REGISTRY in deploy.py."""

    def test_same_number_of_base_flows(self, deployments):
        """Number of unique base flow names should match FLOW_REGISTRY count (6)."""
        base_names = set()
        for d in deployments:
            name = d["name"]
            # Strip pool suffix to get base name
            for suffix in ACTIVE_POOL_SUFFIXES.values():
                if name.endswith(f"-{suffix}"):
                    base_names.add(name[: -(len(suffix) + 1)])
                    break
        assert len(base_names) == len(EXPECTED_FLOWS)

    def test_naming_convention(self, deployments):
        """All deployment names must follow the pattern: {base}-{pool_suffix}."""
        valid_suffixes = set(ACTIVE_POOL_SUFFIXES.values())
        for d in deployments:
            name = d["name"]
            matched = any(name.endswith(f"-{s}") for s in valid_suffixes)
            assert matched, f"{name}: does not end with any valid suffix from {valid_suffixes}"


# ===========================================================================
# YAML validity
# ===========================================================================
class TestYamlValidity:
    def test_is_valid_yaml(self):
        """File should parse without errors."""
        content = PREFECT_YAML.read_text()
        parsed = yaml.safe_load(content)
        assert parsed is not None

    def test_no_tabs(self):
        """YAML should use spaces, not tabs."""
        content = PREFECT_YAML.read_text()
        for i, line in enumerate(content.splitlines(), 1):
            assert "\t" not in line, f"Tab character on line {i}"

    def test_no_duplicate_deployment_names(self):
        """Catch YAML merge issues that could produce duplicate names."""
        content = PREFECT_YAML.read_text()
        parsed = yaml.safe_load(content)
        names = [d["name"] for d in parsed["deployments"]]
        assert len(names) == len(set(names)), (
            f"Duplicate deployment names: {[n for n in names if names.count(n) > 1]}"
        )


# ===========================================================================
# AST helpers — parse flow source files without importing them
# ===========================================================================


def _parse_flow_source(entrypoint_file: str, func_name: str) -> dict:
    """Parse a flow source file using AST and extract function metadata.

    Returns dict with keys:
        - 'params': dict of param_name -> annotation_name (str) or None
        - 'defaults': dict of param_name -> default value (AST literal)
        - 'flow_name': the name= kwarg from @flow() decorator, or None
        - 'found': bool — whether the function was found
    """
    source_path = FLOWS_DIR / entrypoint_file
    tree = ast.parse(source_path.read_text(), filename=str(source_path))

    result = {"params": {}, "defaults": {}, "flow_name": None, "found": False}

    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef) or node.name != func_name:
            continue

        result["found"] = True

        # --- Extract parameters (skip 'self' if present) ---
        args = node.args
        # positional args and their defaults (defaults are right-aligned)
        num_defaults = len(args.defaults)
        num_args = len(args.args)
        for i, arg in enumerate(args.args):
            param_name = arg.arg
            # annotation
            ann = None
            if arg.annotation and isinstance(arg.annotation, ast.Name):
                ann = arg.annotation.id
            result["params"][param_name] = ann
            # default value (defaults list is right-aligned to args list)
            default_idx = i - (num_args - num_defaults)
            if default_idx >= 0:
                default_node = args.defaults[default_idx]
                try:
                    result["defaults"][param_name] = ast.literal_eval(default_node)
                except (ValueError, TypeError):
                    result["defaults"][param_name] = "<complex>"

        # --- Extract @flow(name=...) from decorators ---
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call):
                # decorator is @flow(...) or @flow
                func_node = decorator.func
                dec_name = None
                if isinstance(func_node, ast.Name):
                    dec_name = func_node.id
                elif isinstance(func_node, ast.Attribute):
                    dec_name = func_node.attr
                if dec_name == "flow":
                    for kw in decorator.keywords:
                        if kw.arg == "name":
                            with contextlib.suppress(ValueError, TypeError):
                                result["flow_name"] = ast.literal_eval(kw.value)
        break  # only need the first match

    return result


# ===========================================================================
# Multi-folder entrypoint resolution
# ===========================================================================
class TestMultiFolderEntrypoints:
    """Validate that flows at different directory depths are correctly
    configured in prefect.yaml and that the filesystem supports them."""

    DEPTH_MAP = {
        "root": [
            "example_flow.py",
            "snowflake_flow.py",
            "external_api_flow.py",
            "e2e_test_flow.py",
            "data_quality_flow.py",
            "stage_cleanup_flow.py",
            "health_check_flow.py",
        ],
        "subfolder": ["analytics/revenue_flow.py"],
        "deep_subfolder": ["analytics/reports/quarterly_flow.py"],
    }

    def test_all_entrypoint_files_exist(self, deployments):
        """Every entrypoint referenced in prefect.yaml must point to an
        existing .py file relative to FLOWS_DIR (the working directory)."""
        for d in deployments:
            ep = d["entrypoint"]
            file_part = ep.split(":")[0]
            full_path = FLOWS_DIR / file_part
            assert full_path.is_file(), (
                f"Deployment '{d['name']}': entrypoint file not found: {full_path}"
            )

    def test_all_entrypoints_no_flows_prefix(self, deployments):
        """Entrypoints must NOT start with 'flows/' — set_working_directory
        already points to flows/, so paths are relative to that directory."""
        for d in deployments:
            ep = d["entrypoint"]
            assert not ep.startswith("flows/"), (
                f"Deployment '{d['name']}': entrypoint '{ep}' must not "
                f"start with 'flows/' (working directory is already flows/)"
            )

    def test_subfolder_init_files_exist(self):
        """All intermediate __init__.py files must exist for Python
        to treat subdirectories as packages during import."""
        required_inits = [
            FLOWS_DIR / "__init__.py",
            FLOWS_DIR / "analytics" / "__init__.py",
            FLOWS_DIR / "analytics" / "reports" / "__init__.py",
        ]
        for init_path in required_inits:
            assert init_path.is_file(), (
                f"Missing __init__.py: {init_path} — Python won't recognize this as a package"
            )

    def test_root_level_flows_have_no_subdirectory(self, deployments):
        """Root-level flows should be directly under flows/, not in
        a subdirectory."""
        root_files = set(self.DEPTH_MAP["root"])
        for d in deployments:
            file_part = d["entrypoint"].split(":")[0]
            if "/" not in file_part:
                assert file_part in root_files, f"Unexpected root-level flow file: {file_part}"

    def test_subfolder_flows_correct_depth(self, deployments):
        """Flows in analytics/ should have exactly one directory level."""
        subfolder_files = set(self.DEPTH_MAP["subfolder"])
        for d in deployments:
            file_part = d["entrypoint"].split(":")[0]
            if file_part in subfolder_files:
                parts = file_part.split("/")
                assert len(parts) == 2, (
                    f"Expected 1 subdirectory for {file_part}, got {len(parts) - 1}"
                )

    def test_deep_subfolder_flows_correct_depth(self, deployments):
        """Flows in analytics/reports/ should have exactly two directory levels."""
        deep_files = set(self.DEPTH_MAP["deep_subfolder"])
        for d in deployments:
            file_part = d["entrypoint"].split(":")[0]
            if file_part in deep_files:
                parts = file_part.split("/")
                assert len(parts) == 3, (
                    f"Expected 2 subdirectories for {file_part}, got {len(parts) - 1}"
                )

    def test_entrypoint_function_matches_source(self, deployments):
        """The function name in the entrypoint (after ':') must be a
        real function defined in the source file."""
        for d in deployments:
            ep = d["entrypoint"]
            file_part, func_name = ep.split(":")
            source_path = FLOWS_DIR / file_part
            source = source_path.read_text()
            tree = ast.parse(source, filename=str(source_path))
            func_names = [node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
            assert func_name in func_names, (
                f"Deployment '{d['name']}': function '{func_name}' "
                f"not found in {file_part}. Available: {func_names}"
            )

    @pytest.mark.parametrize("depth,files", list(DEPTH_MAP.items()))
    def test_each_depth_has_at_least_one_flow(self, depth, files, deployments):
        """Ensure we actually test at each directory depth."""
        entrypoint_files = {d["entrypoint"].split(":")[0] for d in deployments}
        found = [f for f in files if f in entrypoint_files]
        assert found, f"No deployments found at depth '{depth}' for files: {files}"


# ===========================================================================
# Parameter pass-through correctness
# ===========================================================================
class TestParameterPassThrough:
    """Validate that parameters declared in prefect.yaml match what
    the actual flow functions accept. Uses AST parsing — no imports needed."""

    @pytest.mark.parametrize(
        "flow_spec", EXPECTED_FLOWS, ids=[f["base_name"] for f in EXPECTED_FLOWS]
    )
    def test_yaml_params_are_valid_function_params(self, flow_spec, deployment_map):
        """If prefect.yaml declares parameters for a deployment, every
        parameter key must be a valid argument of the flow function."""
        parsed = _parse_flow_source(flow_spec["entrypoint_file"], flow_spec["func"])
        assert parsed["found"], (
            f"Function '{flow_spec['func']}' not found in {flow_spec['entrypoint_file']}"
        )

        # Check both pool variants
        for suffix in ACTIVE_POOL_SUFFIXES.values():
            dep_name = f"{flow_spec['base_name']}-{suffix}"
            dep = deployment_map.get(dep_name)
            if dep is None:
                continue
            yaml_params = dep.get("parameters") or {}
            for param_key in yaml_params:
                assert param_key in parsed["params"], (
                    f"Deployment '{dep_name}': parameter '{param_key}' is not "
                    f"accepted by {flow_spec['func']}(). "
                    f"Valid params: {list(parsed['params'].keys())}"
                )

    @pytest.mark.parametrize(
        "flow_spec", EXPECTED_FLOWS, ids=[f["base_name"] for f in EXPECTED_FLOWS]
    )
    def test_flows_without_yaml_params_have_defaults(self, flow_spec, deployment_map):
        """Flows with no parameters in prefect.yaml must have defaults for
        all their function parameters — otherwise they'd fail at runtime."""
        parsed = _parse_flow_source(flow_spec["entrypoint_file"], flow_spec["func"])

        for suffix in ACTIVE_POOL_SUFFIXES.values():
            dep_name = f"{flow_spec['base_name']}-{suffix}"
            dep = deployment_map.get(dep_name)
            if dep is None:
                continue
            yaml_params = dep.get("parameters") or {}
            for param_name in parsed["params"]:
                if param_name not in yaml_params:
                    assert param_name in parsed["defaults"], (
                        f"Deployment '{dep_name}': parameter '{param_name}' has "
                        f"no default in the function and no value in prefect.yaml — "
                        f"this will fail at runtime"
                    )

    @pytest.mark.parametrize(
        "flow_spec", EXPECTED_FLOWS, ids=[f["base_name"] for f in EXPECTED_FLOWS]
    )
    def test_expected_parameters_match_yaml(self, flow_spec, deployment_map):
        """The parameters we expect (from EXPECTED_FLOWS) must match what's
        actually declared in prefect.yaml."""
        expected_params = flow_spec["parameters"]
        for suffix in ACTIVE_POOL_SUFFIXES.values():
            dep_name = f"{flow_spec['base_name']}-{suffix}"
            dep = deployment_map.get(dep_name)
            if dep is None:
                continue
            actual_params = dep.get("parameters")
            if expected_params is None:
                assert actual_params is None or actual_params == {}, (
                    f"Deployment '{dep_name}': expected no parameters but found {actual_params}"
                )
            else:
                assert actual_params == expected_params, (
                    f"Deployment '{dep_name}': parameters mismatch.\n"
                    f"  Expected: {expected_params}\n"
                    f"  Actual:   {actual_params}"
                )

    def test_example_flow_overrides_default(self, deployment_map):
        """example-flow sets name='Prefect-SPCS' which overrides the
        function default of 'World'."""
        parsed = _parse_flow_source("example_flow.py", "example_flow")
        assert parsed["defaults"].get("name") == "World", (
            "example_flow default for 'name' should be 'World'"
        )
        for suffix in ACTIVE_POOL_SUFFIXES.values():
            dep = deployment_map[f"example-flow-{suffix}"]
            assert dep["parameters"]["name"] == "Prefect-SPCS", (
                f"example-flow-{suffix} should override name to 'Prefect-SPCS'"
            )

    def test_no_extra_params_in_yaml(self, deployments):
        """No deployment should declare parameters that the flow function
        doesn't accept — catches typos and stale config."""
        for d in deployments:
            yaml_params = d.get("parameters") or {}
            if not yaml_params:
                continue
            file_part, func_name = d["entrypoint"].split(":")
            parsed = _parse_flow_source(file_part, func_name)
            for key in yaml_params:
                assert key in parsed["params"], (
                    f"Deployment '{d['name']}': parameter '{key}' is not "
                    f"in function signature of {func_name}()"
                )

    def test_parameterless_flows_have_no_yaml_params(self, deployment_map):
        """Flows whose functions take zero arguments should never have
        parameters declared in prefect.yaml."""
        for flow_spec in EXPECTED_FLOWS:
            if flow_spec["func_params"]:
                continue  # has params, skip
            for suffix in ACTIVE_POOL_SUFFIXES.values():
                dep_name = f"{flow_spec['base_name']}-{suffix}"
                dep = deployment_map.get(dep_name)
                if dep is None:
                    continue
                yaml_params = dep.get("parameters")
                assert yaml_params is None or yaml_params == {}, (
                    f"Deployment '{dep_name}': flow takes no arguments but "
                    f"prefect.yaml declares parameters: {yaml_params}"
                )


# ===========================================================================
# @flow(name=...) ↔ deployment name cross-validation
# ===========================================================================
class TestFlowNameConsistency:
    """Validate that @flow(name='...') in source code is consistent
    with the deployment naming in prefect.yaml."""

    @pytest.mark.parametrize(
        "flow_spec", EXPECTED_FLOWS, ids=[f["base_name"] for f in EXPECTED_FLOWS]
    )
    def test_flow_decorator_name_matches_expected(self, flow_spec):
        """The @flow(name='...') in source must match flow_name
        in our expected registry."""
        parsed = _parse_flow_source(flow_spec["entrypoint_file"], flow_spec["func"])
        assert parsed["flow_name"] == flow_spec["flow_name"], (
            f"In {flow_spec['entrypoint_file']}: @flow(name='{parsed['flow_name']}') "
            f"doesn't match expected '{flow_spec['flow_name']}'"
        )

    @pytest.mark.parametrize(
        "flow_spec", EXPECTED_FLOWS, ids=[f["base_name"] for f in EXPECTED_FLOWS]
    )
    def test_deployment_names_derived_from_flow_name(self, flow_spec, deployment_map):
        """Each deployment name should be {base_name}-{pool_suffix} where
        base_name is consistent with the @flow(name=...) value."""
        for pool_key in _flow_pool_keys(flow_spec):
            suffix = ACTIVE_POOL_SUFFIXES[pool_key]
            dep_name = f"{flow_spec['base_name']}-{suffix}"
            assert dep_name in deployment_map, (
                f"Missing deployment '{dep_name}' for flow '{flow_spec['flow_name']}'"
            )

    def test_all_flows_have_name_decorator(self, deployments):
        """Every flow function referenced in prefect.yaml must have an
        explicit @flow(name=...) — relying on auto-generated names is fragile."""
        for d in deployments:
            file_part, func_name = d["entrypoint"].split(":")
            parsed = _parse_flow_source(file_part, func_name)
            assert parsed["flow_name"] is not None, (
                f"Deployment '{d['name']}': {func_name} in {file_part} "
                f"has no explicit @flow(name=...) — add one for stability"
            )


# ===========================================================================
# Paired deployment symmetry (local vs gcp)
# ===========================================================================
class TestPairedDeploymentSymmetry:
    """Each flow deployed to multiple pools must be identical across variants
    except for pool name, pool suffix in deployment name, and pool-specific tag."""

    @pytest.fixture()
    def paired(self, deployment_map):
        """Return list of (local_dep, gcp_dep) tuples for flows on both pools."""
        pairs = []
        for flow_spec in EXPECTED_FLOWS:
            pool_keys = _flow_pool_keys(flow_spec)
            if "spcs" not in pool_keys or "gcp" not in pool_keys:
                continue
            local_name = f"{flow_spec['base_name']}-local"
            gcp_name = f"{flow_spec['base_name']}-gcp"
            assert local_name in deployment_map, f"Missing {local_name}"
            assert gcp_name in deployment_map, f"Missing {gcp_name}"
            pairs.append((deployment_map[local_name], deployment_map[gcp_name]))
        return pairs

    def test_same_entrypoints(self, paired):
        """Both pool variants must point to the same flow file and function."""
        for local, gcp in paired:
            assert local["entrypoint"] == gcp["entrypoint"], (
                f"{local['name']} vs {gcp['name']}: entrypoints differ "
                f"({local['entrypoint']} != {gcp['entrypoint']})"
            )

    def test_same_parameters(self, paired):
        """Both pool variants must declare the same parameters."""
        for local, gcp in paired:
            lp = local.get("parameters") or {}
            gp = gcp.get("parameters") or {}
            assert lp == gp, f"{local['name']} vs {gcp['name']}: parameters differ ({lp} != {gp})"

    def test_same_schedules(self, paired):
        """Both pool variants must have identical schedules."""
        for local, gcp in paired:
            ls = local.get("schedules")
            gs = gcp.get("schedules")
            assert ls == gs, f"{local['name']} vs {gcp['name']}: schedules differ"

    def test_same_concurrency(self, paired):
        """Both pool variants must have identical concurrency limits."""
        for local, gcp in paired:
            lc = local.get("concurrency_limit")
            gc = gcp.get("concurrency_limit")
            assert lc == gc, f"{local['name']} vs {gcp['name']}: concurrency_limit differs"

    def test_same_enforce_parameter_schema(self, paired):
        """Both pool variants must agree on enforce_parameter_schema."""
        for local, gcp in paired:
            le = local.get("enforce_parameter_schema")
            ge = gcp.get("enforce_parameter_schema")
            assert le == ge, (
                f"{local['name']} vs {gcp['name']}: enforce_parameter_schema differs ({le} != {ge})"
            )

    def test_different_pools(self, paired):
        """The two variants MUST target different work pools."""
        for local, gcp in paired:
            assert local["work_pool"]["name"] != gcp["work_pool"]["name"], (
                f"{local['name']} and {gcp['name']} target the same pool"
            )

    def test_different_pool_tags(self, paired):
        """Each variant should have its pool-specific tag."""
        for local, gcp in paired:
            local_tags = set(local.get("tags", []))
            gcp_tags = set(gcp.get("tags", []))
            assert "spcs" in local_tags, f"{local['name']} missing 'spcs' tag"
            assert "gcp" in gcp_tags, f"{gcp['name']} missing 'gcp' tag"

    def test_shared_tags_match(self, paired):
        """Non-pool tags should be identical between variants."""
        pool_tags = {"spcs", "gcp"}
        for local, gcp in paired:
            local_shared = set(local.get("tags", [])) - pool_tags
            gcp_shared = set(gcp.get("tags", [])) - pool_tags
            assert local_shared == gcp_shared, (
                f"{local['name']} vs {gcp['name']}: non-pool tags differ "
                f"({local_shared} != {gcp_shared})"
            )
