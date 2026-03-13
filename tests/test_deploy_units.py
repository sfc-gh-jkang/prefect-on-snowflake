"""Behavioral tests for deploy.py unit functions.

Tests _build_pull_steps (4 code paths), _parse_args (CLI parsing),
and _detect_pool_key (auto-detection) — all without live Prefect connections.
"""

import importlib.util
import os
import sys
import types
from contextlib import contextmanager
from unittest.mock import patch

import pytest
from conftest import FLOWS_DIR


@contextmanager
def _import_deploy(**env_overrides) -> types.ModuleType:
    """Import deploy.py with controlled env vars, returning a fresh module.

    deploy.py reads GIT_REPO_URL and GIT_BRANCH at module level, so we
    need to control those at import time.
    """
    env = {
        "GIT_REPO_URL": "",
        "GIT_BRANCH": "main",
        "PREFECT_ENV": "",
        **env_overrides,
    }
    with patch.dict(os.environ, env, clear=False):
        spec = importlib.util.spec_from_file_location("deploy_test", FLOWS_DIR / "deploy.py")
        mod = importlib.util.module_from_spec(spec)
        old_deploy = sys.modules.get("deploy")
        old_deploy_test = sys.modules.get("deploy_test")
        # Register under both names: "deploy" for internal imports and
        # "deploy_test" so @dataclass can find cls.__module__ in sys.modules.
        sys.modules["deploy"] = mod
        sys.modules["deploy_test"] = mod
        spec.loader.exec_module(mod)
        yield mod
        # Restore previous state
        for key, old_val in [("deploy", old_deploy), ("deploy_test", old_deploy_test)]:
            if old_val is not None:
                sys.modules[key] = old_val
            else:
                sys.modules.pop(key, None)


# ===================================================================
# _build_pull_steps — 4 code paths
# ===================================================================
class TestBuildPullSteps:
    """Test _build_pull_steps for SPCS and external pool types."""

    def test_spcs_with_git_returns_git_clone(self):
        """SPCS pool + GIT_REPO_URL set → git_clone steps."""
        with _import_deploy(GIT_REPO_URL="https://github.com/org/repo.git") as mod:
            steps = mod._build_pull_steps("spcs", "example_flow.py:example_flow")

        assert len(steps) == 2
        first_step = steps[0]
        assert "prefect.deployments.steps.git_clone" in first_step
        clone_cfg = first_step["prefect.deployments.steps.git_clone"]
        assert clone_cfg["repository"] == "https://github.com/org/repo.git"
        assert clone_cfg["branch"] == "main"
        assert clone_cfg["access_token"] == "{{ $GIT_ACCESS_TOKEN }}"

    def test_spcs_with_git_has_set_working_directory(self):
        """SPCS + git should set working directory to clone/flows."""
        with _import_deploy(GIT_REPO_URL="https://github.com/org/repo.git") as mod:
            steps = mod._build_pull_steps("spcs", "example_flow.py:example_flow")

        second_step = steps[1]
        assert "prefect.deployments.steps.set_working_directory" in second_step
        dir_cfg = second_step["prefect.deployments.steps.set_working_directory"]
        assert dir_cfg["directory"] == "{{ clone.directory }}/flows"

    def test_spcs_without_git_uses_stage_volume(self):
        """SPCS pool + no GIT_REPO_URL → stage volume mount fallback."""
        with _import_deploy(GIT_REPO_URL="") as mod:
            steps = mod._build_pull_steps("spcs", "example_flow.py:example_flow")

        assert len(steps) == 1
        step = steps[0]
        assert "prefect.deployments.steps.set_working_directory" in step
        dir_cfg = step["prefect.deployments.steps.set_working_directory"]
        assert dir_cfg["directory"] == "/opt/prefect/flows"

    def test_external_with_git_returns_git_clone(self):
        """External pool + GIT_REPO_URL set → git_clone steps."""
        with _import_deploy(GIT_REPO_URL="https://gitlab.com/org/repo.git") as mod:
            steps = mod._build_pull_steps("gcp", "example_flow.py:example_flow")

        assert len(steps) == 2
        first_step = steps[0]
        assert "prefect.deployments.steps.git_clone" in first_step
        clone_cfg = first_step["prefect.deployments.steps.git_clone"]
        assert clone_cfg["repository"] == "https://gitlab.com/org/repo.git"

    def test_external_without_git_raises_error(self):
        """External pool + no GIT_REPO_URL → ValueError."""
        with (
            _import_deploy(GIT_REPO_URL="") as mod,
            pytest.raises(ValueError, match="GIT_REPO_URL env var is required"),
        ):
            mod._build_pull_steps("gcp", "example_flow.py:example_flow")

    def test_external_error_mentions_pool_name(self):
        """Error message should mention the pool name."""
        with (
            _import_deploy(GIT_REPO_URL="") as mod,
            pytest.raises(ValueError, match="gcp-pool"),
        ):
            mod._build_pull_steps("gcp", "example_flow.py:example_flow")

    def test_git_branch_from_env(self):
        """GIT_BRANCH env var should be used in git_clone step."""
        with _import_deploy(
            GIT_REPO_URL="https://github.com/org/repo.git",
            GIT_BRANCH="feature/my-branch",
        ) as mod:
            steps = mod._build_pull_steps("spcs", "example_flow.py:example_flow")

        clone_cfg = steps[0]["prefect.deployments.steps.git_clone"]
        assert clone_cfg["branch"] == "feature/my-branch"


# ===================================================================
# _detect_pool_key removed (was dead code — always returned "spcs")
# ===================================================================
class TestDetectPoolKeyRemoved:
    """Verify _detect_pool_key dead code was removed."""

    def test_function_does_not_exist(self):
        with _import_deploy() as mod:
            assert not hasattr(mod, "_detect_pool_key"), (
                "_detect_pool_key should be removed — it always returned 'spcs'"
            )

    def test_default_pool_key_is_spcs(self):
        """_parse_args with no args should default pool_keys to ['spcs']."""
        with _import_deploy() as mod, patch("sys.argv", ["deploy.py"]):
            pool_keys, _, _, _, _ = mod._parse_args()
            assert pool_keys == ["spcs"]


# ===================================================================
# _parse_args
# ===================================================================
class TestParseArgs:
    """Test _parse_args CLI argument parsing."""

    def test_no_args_defaults_to_detected_pool(self):
        with _import_deploy() as mod:
            with patch("sys.argv", ["deploy.py"]):
                pool_keys, name_filter, validate, diff, clouds = mod._parse_args()
            assert pool_keys == ["spcs"]
            assert name_filter is None
            assert validate is False
            assert diff is False
            assert clouds == []

    def test_pool_flag(self):
        with _import_deploy() as mod:
            with patch("sys.argv", ["deploy.py", "--pool", "gcp"]):
                pool_keys, name_filter, validate, diff, _ = mod._parse_args()
            assert pool_keys == ["gcp"]

    def test_pool_flag_repeatable(self):
        with _import_deploy() as mod:
            with patch("sys.argv", ["deploy.py", "--pool", "spcs", "--pool", "gcp"]):
                pool_keys, _, _, _, _ = mod._parse_args()
            assert "spcs" in pool_keys
            assert "gcp" in pool_keys

    def test_all_flag_returns_all_pools(self):
        with _import_deploy() as mod:
            with patch("sys.argv", ["deploy.py", "--all"]):
                pool_keys, _, _, _, _ = mod._parse_args()
            assert set(pool_keys) == set(mod.POOLS.keys())

    def test_name_filter(self):
        with _import_deploy() as mod:
            with patch("sys.argv", ["deploy.py", "--name", "example-flow"]):
                _, name_filter, _, _, _ = mod._parse_args()
            assert name_filter == "example-flow"

    def test_validate_flag(self):
        with _import_deploy() as mod:
            with patch("sys.argv", ["deploy.py", "--validate"]):
                pool_keys, _, validate, _, _ = mod._parse_args()
            assert validate is True
            assert pool_keys == []

    def test_diff_flag(self):
        with _import_deploy() as mod:
            with patch("sys.argv", ["deploy.py", "--diff"]):
                _, _, _, diff, _ = mod._parse_args()
            assert diff is True

    def test_gcp_backwards_compat(self):
        """--gcp should be equivalent to --pool gcp."""
        with _import_deploy() as mod:
            with patch("sys.argv", ["deploy.py", "--gcp"]):
                pool_keys, _, _, _, _ = mod._parse_args()
            assert pool_keys == ["gcp"]

    def test_unknown_pool_exits(self):
        with (
            _import_deploy() as mod,
            patch("sys.argv", ["deploy.py", "--pool", "nonexistent"]),
            pytest.raises(SystemExit),
        ):
            mod._parse_args()

    def test_cloud_flag_single(self):
        with _import_deploy() as mod:
            with patch("sys.argv", ["deploy.py", "--cloud", "aws"]):
                _, _, _, _, clouds = mod._parse_args()
            assert clouds == ["aws"]

    def test_cloud_flag_all(self):
        with _import_deploy() as mod:
            with patch("sys.argv", ["deploy.py", "--cloud", "all"]):
                _, _, _, _, clouds = mod._parse_args()
            assert set(clouds) == {"aws", "azure", "gcp"}

    def test_cloud_flag_repeatable(self):
        with _import_deploy() as mod:
            with patch("sys.argv", ["deploy.py", "--cloud", "aws", "--cloud", "gcp"]):
                _, _, _, _, clouds = mod._parse_args()
            assert clouds == ["aws", "gcp"]

    def test_unknown_cloud_exits(self):
        with (
            _import_deploy() as mod,
            patch("sys.argv", ["deploy.py", "--cloud", "nonexistent"]),
            pytest.raises(SystemExit),
        ):
            mod._parse_args()

    def test_combined_flags(self):
        with _import_deploy() as mod:
            with patch(
                "sys.argv",
                ["deploy.py", "--pool", "gcp", "--name", "e2e-test", "--diff", "--cloud", "aws"],
            ):
                pool_keys, name_filter, validate, diff, clouds = mod._parse_args()
            assert pool_keys == ["gcp"]
            assert name_filter == "e2e-test"
            assert validate is False
            assert diff is True
            assert clouds == ["aws"]


# ---------------------------------------------------------------------------
# TriggerSpec + build_automations
# ---------------------------------------------------------------------------
class TestTriggerSpec:
    """Tests for TriggerSpec dataclass and FlowSpec.build_automations()."""

    def test_trigger_spec_defaults(self):
        with _import_deploy() as mod:
            trig = mod.TriggerSpec(expect={"prefect.flow-run.Completed"})
            assert trig.posture == "reactive"
            assert trig.threshold == 1
            assert trig.within == 0
            assert trig.match is None
            assert trig.match_related is None
            assert trig.parameters is None

    def test_flowspec_triggers_default_empty(self):
        with _import_deploy() as mod:
            spec = mod.FlowSpec(path="example_flow.py", func="example_flow", name="test")
            assert spec.triggers == []

    def test_build_automations_empty_when_no_triggers(self):
        with _import_deploy() as mod:
            spec = mod.FlowSpec(path="example_flow.py", func="example_flow", name="test")
            import uuid

            result = spec.build_automations("test-local", uuid.uuid4())
            assert result == []

    def test_build_automations_single_reactive_trigger(self):
        with _import_deploy() as mod:
            trig = mod.TriggerSpec(
                expect={"prefect.flow-run.Completed"},
                match_related={"prefect.resource.name": "upstream-flow"},
            )
            spec = mod.FlowSpec(
                path="example_flow.py",
                func="example_flow",
                name="test",
                triggers=[trig],
            )
            import uuid

            dep_id = uuid.uuid4()
            automations = spec.build_automations("test-local", dep_id)
            assert len(automations) == 1
            auto = automations[0]
            assert auto.name == "trigger/test-local"
            assert auto.enabled is True
            assert auto.trigger.expect == {"prefect.flow-run.Completed"}
            assert len(auto.actions) == 1
            action = auto.actions[0]
            assert action.deployment_id == dep_id

    def test_build_automations_multiple_triggers_get_suffix(self):
        with _import_deploy() as mod:
            trig1 = mod.TriggerSpec(expect={"event.a"})
            trig2 = mod.TriggerSpec(expect={"event.b"}, posture="proactive", within=300)
            spec = mod.FlowSpec(
                path="example_flow.py",
                func="example_flow",
                name="test",
                triggers=[trig1, trig2],
            )
            import uuid

            automations = spec.build_automations("test-local", uuid.uuid4())
            assert len(automations) == 2
            assert automations[0].name == "trigger/test-local-0"
            assert automations[1].name == "trigger/test-local-1"

    def test_build_automations_proactive_posture(self):
        with _import_deploy() as mod:
            trig = mod.TriggerSpec(
                expect={"monitoring.heartbeat"},
                posture="proactive",
                within=600,
                threshold=1,
            )
            spec = mod.FlowSpec(
                path="example_flow.py",
                func="example_flow",
                name="test",
                triggers=[trig],
            )
            import uuid

            automations = spec.build_automations("test-local", uuid.uuid4())
            auto = automations[0]
            # Verify posture is Proactive
            from prefect.events.schemas.automations import Posture

            assert auto.trigger.posture == Posture.Proactive
            assert auto.trigger.within.total_seconds() == 600

    def test_build_automations_passes_parameters(self):
        with _import_deploy() as mod:
            trig = mod.TriggerSpec(
                expect={"custom.event"},
                parameters={"region": "us-east-1"},
            )
            spec = mod.FlowSpec(
                path="example_flow.py",
                func="example_flow",
                name="test",
                triggers=[trig],
            )
            import uuid

            automations = spec.build_automations("test-local", uuid.uuid4())
            action = automations[0].actions[0]
            assert action.parameters == {"region": "us-east-1"}


# ---------------------------------------------------------------------------
# _validate — offline validation of FLOW_REGISTRY
# ---------------------------------------------------------------------------
class TestValidate:
    """Tests for _validate() offline validation."""

    def test_validate_passes_with_default_registry(self, capsys):
        """Default FLOW_REGISTRY should pass validation."""
        with _import_deploy() as mod:
            result = mod._validate()
        assert result is True
        captured = capsys.readouterr()
        assert "0 failed" in captured.out

    def test_validate_catches_missing_file(self, capsys):
        """A FlowSpec pointing to a non-existent file should fail."""
        with _import_deploy() as mod:
            original = mod.FLOW_REGISTRY[:]
            try:
                mod.FLOW_REGISTRY[:] = [mod.FlowSpec(path="nonexistent.py", func="foo", name="bad")]
                result = mod._validate()
            finally:
                mod.FLOW_REGISTRY[:] = original
        assert result is False
        captured = capsys.readouterr()
        assert "file not found" in captured.out

    def test_validate_catches_missing_function(self, capsys):
        """A FlowSpec with wrong function name should fail."""
        with _import_deploy() as mod:
            original = mod.FLOW_REGISTRY[:]
            try:
                mod.FLOW_REGISTRY[:] = [
                    mod.FlowSpec(path="example_flow.py", func="no_such_func", name="bad")
                ]
                result = mod._validate()
            finally:
                mod.FLOW_REGISTRY[:] = original
        assert result is False
        captured = capsys.readouterr()
        assert "not found in" in captured.out

    def test_validate_catches_invalid_cron(self, capsys):
        """A FlowSpec with bad cron expression should fail."""
        with _import_deploy() as mod:
            original = mod.FLOW_REGISTRY[:]
            try:
                mod.FLOW_REGISTRY[:] = [
                    mod.FlowSpec(
                        path="example_flow.py",
                        func="example_flow",
                        name="bad",
                        cron="*/5 * *",  # only 3 fields, need 5
                    )
                ]
                result = mod._validate()
            finally:
                mod.FLOW_REGISTRY[:] = original
        assert result is False
        captured = capsys.readouterr()
        assert "invalid cron" in captured.out

    def test_validate_catches_bad_collision_strategy(self, capsys):
        """A FlowSpec with invalid collision_strategy should fail."""
        with _import_deploy() as mod:
            original = mod.FLOW_REGISTRY[:]
            try:
                mod.FLOW_REGISTRY[:] = [
                    mod.FlowSpec(
                        path="example_flow.py",
                        func="example_flow",
                        name="bad",
                        collision_strategy="DROP",
                    )
                ]
                result = mod._validate()
            finally:
                mod.FLOW_REGISTRY[:] = original
        assert result is False
        captured = capsys.readouterr()
        assert "invalid collision_strategy" in captured.out

    def test_validate_catches_unknown_pool(self, capsys):
        """A FlowSpec targeting a non-existent pool should fail."""
        with _import_deploy() as mod:
            original = mod.FLOW_REGISTRY[:]
            try:
                mod.FLOW_REGISTRY[:] = [
                    mod.FlowSpec(
                        path="example_flow.py",
                        func="example_flow",
                        name="bad",
                        pools=["nonexistent"],
                    )
                ]
                result = mod._validate()
            finally:
                mod.FLOW_REGISTRY[:] = original
        assert result is False
        captured = capsys.readouterr()
        assert "unknown pool key" in captured.out

    def test_validate_catches_invalid_trigger_posture(self, capsys):
        """A TriggerSpec with bad posture should fail validation."""
        with _import_deploy() as mod:
            original = mod.FLOW_REGISTRY[:]
            try:
                mod.FLOW_REGISTRY[:] = [
                    mod.FlowSpec(
                        path="example_flow.py",
                        func="example_flow",
                        name="bad",
                        triggers=[mod.TriggerSpec(expect={"event.a"}, posture="invalid")],
                    )
                ]
                result = mod._validate()
            finally:
                mod.FLOW_REGISTRY[:] = original
        assert result is False
        captured = capsys.readouterr()
        assert "invalid posture" in captured.out

    def test_validate_catches_empty_trigger_expect(self, capsys):
        """A TriggerSpec with empty expect set should fail validation."""
        with _import_deploy() as mod:
            original = mod.FLOW_REGISTRY[:]
            try:
                mod.FLOW_REGISTRY[:] = [
                    mod.FlowSpec(
                        path="example_flow.py",
                        func="example_flow",
                        name="bad",
                        triggers=[mod.TriggerSpec(expect=set())],
                    )
                ]
                result = mod._validate()
            finally:
                mod.FLOW_REGISTRY[:] = original
        assert result is False
        captured = capsys.readouterr()
        assert "'expect' must be a non-empty set" in captured.out

    def test_validate_warns_duplicate_names(self, capsys):
        """Duplicate flow names should produce a warning."""
        with _import_deploy() as mod:
            original = mod.FLOW_REGISTRY[:]
            try:
                mod.FLOW_REGISTRY[:] = [
                    mod.FlowSpec(path="example_flow.py", func="example_flow", name="dup"),
                    mod.FlowSpec(path="example_flow.py", func="example_flow", name="dup"),
                ]
                mod._validate()
            finally:
                mod.FLOW_REGISTRY[:] = original
        captured = capsys.readouterr()
        assert "duplicate name" in captured.out


# ---------------------------------------------------------------------------
# _sync_stage — stage sync behavior
# ---------------------------------------------------------------------------
class TestSyncStage:
    """Tests for _sync_stage() stage sync behavior."""

    def test_sync_stage_skips_when_script_missing(self, capsys):
        """_sync_stage prints skip message when sync_flows.sh doesn't exist."""
        with _import_deploy() as mod, patch.object(mod.Path, "exists", return_value=False):
            # We need to patch the specific Path instance, so use a different approach
            pass

        # Simpler: patch Path at module level
        with _import_deploy() as mod:
            from pathlib import Path as RealPath

            original_exists = RealPath.exists

            def fake_exists(self):
                if "sync_flows.sh" in str(self):
                    return False
                return original_exists(self)

            with patch.object(RealPath, "exists", fake_exists):
                mod._sync_stage()
        captured = capsys.readouterr()
        assert "skipping stage sync" in captured.out

    def test_sync_stage_handles_script_failure(self, capsys):
        """_sync_stage prints warning but doesn't raise when script fails."""
        import subprocess

        with _import_deploy() as mod:
            from pathlib import Path as RealPath

            original_exists = RealPath.exists

            def fake_exists(self):
                if "sync_flows.sh" in str(self):
                    return True
                return original_exists(self)

            with (
                patch.object(RealPath, "exists", fake_exists),
                patch(
                    "subprocess.run",
                    side_effect=subprocess.CalledProcessError(1, "bash"),
                ),
            ):
                mod._sync_stage()
        captured = capsys.readouterr()
        assert "Warning: stage sync failed" in captured.out
