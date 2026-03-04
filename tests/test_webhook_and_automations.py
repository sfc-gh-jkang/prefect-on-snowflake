"""Behavioral tests for hooks.py webhook alerting and setup_automations.py.

These tests actually import and call the functions with mocked HTTP / Prefect
dependencies, verifying payloads, error handling, and conditional dispatch.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from conftest import FLOWS_DIR, PROJECT_DIR

HOOKS_PATH = FLOWS_DIR / "hooks.py"
AUTOMATIONS_PATH = PROJECT_DIR / "scripts" / "setup_automations.py"

FAKE_UUID = "00000000-0000-0000-0000-000000000001"


# ---------------------------------------------------------------------------
# Helpers — import hooks.py with controlled env vars
# ---------------------------------------------------------------------------
def _import_hooks(**env_overrides) -> types.ModuleType:
    """Import hooks.py with specific env vars, returning a fresh module."""
    env = {
        "ALERT_WEBHOOK_URL": "",
        "SLACK_WEBHOOK_URL": "",
        "PREFECT_API_URL": "",
        **env_overrides,
    }
    with patch.dict(os.environ, env, clear=False):
        spec = importlib.util.spec_from_file_location("hooks_test", HOOKS_PATH)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod


def _make_flow_obj(name: str = "test-flow"):
    obj = MagicMock()
    obj.name = name
    return obj


def _make_flow_run_obj(name: str = "test-run", run_id: str = "abc-123"):
    obj = MagicMock()
    obj.name = name
    obj.id = run_id
    return obj


def _make_state_obj(message: str = "Task failed: division by zero"):
    obj = MagicMock()
    obj.message = message
    return obj


# ===================================================================
# hooks.py — _post_json
# ===================================================================
class TestPostJson:
    """Behavioral tests for _post_json helper."""

    def test_sends_json_content_type(self):
        mod = _import_hooks()
        with patch("urllib.request.urlopen") as mock_open:
            mock_open.return_value.__enter__ = MagicMock(return_value=MagicMock(status=200))
            mock_open.return_value.__exit__ = MagicMock(return_value=False)
            mod._post_json("https://example.com/hook", {"key": "val"})
            req = mock_open.call_args[0][0]
            assert req.get_header("Content-type") == "application/json"

    def test_sends_correct_payload(self):
        mod = _import_hooks()
        with patch("urllib.request.urlopen") as mock_open:
            mock_open.return_value.__enter__ = MagicMock(return_value=MagicMock(status=200))
            mock_open.return_value.__exit__ = MagicMock(return_value=False)
            payload = {"event": "test", "data": [1, 2]}
            mod._post_json("https://example.com/hook", payload)
            req = mock_open.call_args[0][0]
            assert json.loads(req.data) == payload

    def test_logs_warning_on_url_error(self):
        mod = _import_hooks()
        import urllib.error

        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("fail")):
            # Should not raise — fire and forget
            mod._post_json("https://bad.example.com", {"x": 1})

    def test_logs_warning_on_os_error(self):
        mod = _import_hooks()
        with patch("urllib.request.urlopen", side_effect=OSError("connection refused")):
            mod._post_json("https://bad.example.com", {"x": 1})

    def test_sets_timeout(self):
        mod = _import_hooks()
        with patch("urllib.request.urlopen") as mock_open:
            mock_open.return_value.__enter__ = MagicMock(return_value=MagicMock(status=200))
            mock_open.return_value.__exit__ = MagicMock(return_value=False)
            mod._post_json("https://example.com", {})
            _, kwargs = mock_open.call_args
            assert kwargs.get("timeout") == 10


# ===================================================================
# hooks.py — _send_slack_failure
# ===================================================================
class TestSendSlackFailure:
    """Behavioral tests for Slack Block Kit payload construction."""

    def test_payload_has_header_block(self):
        mod = _import_hooks(
            SLACK_WEBHOOK_URL="https://hooks.slack.test/x",
            PREFECT_API_URL="https://prefect.test/api",
        )
        with patch.object(mod, "_post_json") as mock_post:
            mod._send_slack_failure("my-flow", "run-1", "id-1", "boom")
            payload = mock_post.call_args[0][1]
            headers = [b for b in payload["blocks"] if b["type"] == "header"]
            assert len(headers) == 1
            assert "my-flow" in headers[0]["text"]["text"]

    def test_payload_has_section_with_fields(self):
        mod = _import_hooks(
            SLACK_WEBHOOK_URL="https://hooks.slack.test/x",
            PREFECT_API_URL="https://prefect.test/api",
        )
        with patch.object(mod, "_post_json") as mock_post:
            mod._send_slack_failure("my-flow", "run-1", "id-1", "boom")
            payload = mock_post.call_args[0][1]
            sections = [b for b in payload["blocks"] if b["type"] == "section"]
            assert any("fields" in s for s in sections)

    def test_payload_includes_error_message(self):
        mod = _import_hooks(
            SLACK_WEBHOOK_URL="https://hooks.slack.test/x",
        )
        with patch.object(mod, "_post_json") as mock_post:
            mod._send_slack_failure("f", "r", "i", "division by zero")
            payload = mock_post.call_args[0][1]
            text_blocks = [
                b
                for b in payload["blocks"]
                if b["type"] == "section" and "text" in b and isinstance(b["text"], dict)
            ]
            assert any("division by zero" in b["text"]["text"] for b in text_blocks)

    def test_error_message_truncated_at_1500(self):
        mod = _import_hooks(
            SLACK_WEBHOOK_URL="https://hooks.slack.test/x",
        )
        with patch.object(mod, "_post_json") as mock_post:
            long_msg = "x" * 3000
            mod._send_slack_failure("f", "r", "i", long_msg)
            payload = mock_post.call_args[0][1]
            raw = json.dumps(payload)
            # The message in the payload should be truncated
            assert "x" * 1501 not in raw

    def test_includes_prefect_link_when_api_url_set(self):
        mod = _import_hooks(
            SLACK_WEBHOOK_URL="https://hooks.slack.test/x",
            PREFECT_API_URL="https://prefect.test/api",
        )
        with patch.object(mod, "_post_json") as mock_post:
            mod._send_slack_failure("f", "r", "run-id-abc", "err")
            payload = mock_post.call_args[0][1]
            action_blocks = [b for b in payload["blocks"] if b["type"] == "actions"]
            assert len(action_blocks) == 1
            btn = action_blocks[0]["elements"][0]
            assert "run-id-abc" in btn["url"]

    def test_no_actions_block_when_api_url_empty(self):
        mod = _import_hooks(
            SLACK_WEBHOOK_URL="https://hooks.slack.test/x",
            PREFECT_API_URL="",
        )
        with patch.object(mod, "_post_json") as mock_post:
            mod._send_slack_failure("f", "r", "i", "err")
            payload = mock_post.call_args[0][1]
            action_blocks = [b for b in payload["blocks"] if b["type"] == "actions"]
            assert len(action_blocks) == 0

    def test_posts_to_slack_url(self):
        mod = _import_hooks(
            SLACK_WEBHOOK_URL="https://hooks.slack.test/secret",
        )
        with patch.object(mod, "_post_json") as mock_post:
            mod._send_slack_failure("f", "r", "i", "err")
            assert mock_post.call_args[0][0] == "https://hooks.slack.test/secret"


# ===================================================================
# hooks.py — _send_generic_webhook
# ===================================================================
class TestSendGenericWebhook:
    """Behavioral tests for the generic webhook payload."""

    def test_payload_has_required_fields(self):
        mod = _import_hooks(ALERT_WEBHOOK_URL="https://webhook.test/alert")
        with patch.object(mod, "_post_json") as mock_post:
            mod._send_generic_webhook("flow_failed", "my-flow", "run-1", "id-1", "boom")
            payload = mock_post.call_args[0][1]
            assert payload["event"] == "flow_failed"
            assert payload["flow_name"] == "my-flow"
            assert payload["flow_run_name"] == "run-1"
            assert payload["flow_run_id"] == "id-1"
            assert payload["message"] == "boom"

    def test_includes_prefect_url_when_set(self):
        mod = _import_hooks(
            ALERT_WEBHOOK_URL="https://webhook.test/alert",
            PREFECT_API_URL="https://prefect.test/api",
        )
        with patch.object(mod, "_post_json") as mock_post:
            mod._send_generic_webhook("flow_failed", "f", "r", "run-id", "err")
            payload = mock_post.call_args[0][1]
            assert "prefect_url" in payload
            assert "run-id" in payload["prefect_url"]

    def test_no_prefect_url_when_api_url_empty(self):
        mod = _import_hooks(
            ALERT_WEBHOOK_URL="https://webhook.test/alert",
            PREFECT_API_URL="",
        )
        with patch.object(mod, "_post_json") as mock_post:
            mod._send_generic_webhook("flow_failed", "f", "r", "i", "err")
            payload = mock_post.call_args[0][1]
            assert "prefect_url" not in payload

    def test_posts_to_alert_url(self):
        mod = _import_hooks(ALERT_WEBHOOK_URL="https://webhook.test/endpoint")
        with patch.object(mod, "_post_json") as mock_post:
            mod._send_generic_webhook("flow_failed", "f", "r", "i", "err")
            assert mock_post.call_args[0][0] == "https://webhook.test/endpoint"


# ===================================================================
# hooks.py — on_flow_failure integration
# ===================================================================
class TestOnFlowFailureIntegration:
    """Test that on_flow_failure dispatches correctly based on env vars."""

    def test_logs_error_always(self):
        mod = _import_hooks()
        with patch.object(mod.logger, "error") as mock_log:
            flow = _make_flow_obj("etl")
            run = _make_flow_run_obj("run-7", "uuid-7")
            state = _make_state_obj("kaboom")
            mod.on_flow_failure(flow, run, state)
            mock_log.assert_called_once()
            log_msg = mock_log.call_args[0][0]
            assert "FLOW FAILED" in log_msg

    def test_calls_slack_when_url_set(self):
        mod = _import_hooks(SLACK_WEBHOOK_URL="https://slack.test")
        with (
            patch.object(mod, "_send_slack_failure") as mock_slack,
            patch.object(mod, "_send_generic_webhook"),
        ):
            mod.on_flow_failure(
                _make_flow_obj("f"), _make_flow_run_obj("r", "i"), _make_state_obj("e")
            )
            mock_slack.assert_called_once()
            assert mock_slack.call_args[0][0] == "f"

    def test_calls_generic_when_url_set(self):
        mod = _import_hooks(ALERT_WEBHOOK_URL="https://alert.test")
        with (
            patch.object(mod, "_send_generic_webhook") as mock_generic,
            patch.object(mod, "_send_slack_failure"),
        ):
            mod.on_flow_failure(
                _make_flow_obj("f"), _make_flow_run_obj("r", "i"), _make_state_obj("e")
            )
            mock_generic.assert_called_once()
            assert mock_generic.call_args[0][0] == "flow_failed"

    def test_calls_both_when_both_urls_set(self):
        mod = _import_hooks(
            SLACK_WEBHOOK_URL="https://slack.test",
            ALERT_WEBHOOK_URL="https://alert.test",
        )
        with (
            patch.object(mod, "_send_slack_failure") as mock_slack,
            patch.object(mod, "_send_generic_webhook") as mock_generic,
        ):
            mod.on_flow_failure(_make_flow_obj(), _make_flow_run_obj(), _make_state_obj())
            mock_slack.assert_called_once()
            mock_generic.assert_called_once()

    def test_calls_neither_when_no_urls(self):
        mod = _import_hooks(SLACK_WEBHOOK_URL="", ALERT_WEBHOOK_URL="")
        with (
            patch.object(mod, "_send_slack_failure") as mock_slack,
            patch.object(mod, "_send_generic_webhook") as mock_generic,
        ):
            mod.on_flow_failure(_make_flow_obj(), _make_flow_run_obj(), _make_state_obj())
            mock_slack.assert_not_called()
            mock_generic.assert_not_called()

    def test_handles_missing_attributes_gracefully(self):
        """Hook should not crash if flow/run/state lack expected attrs."""
        mod = _import_hooks()
        # Plain objects with no .name/.id/.message
        mod.on_flow_failure(object(), object(), object())


# ===================================================================
# hooks.py — on_flow_completion
# ===================================================================
class TestOnFlowCompletion:
    """Test on_flow_completion logging."""

    def test_logs_info(self):
        mod = _import_hooks()
        with patch.object(mod.logger, "info") as mock_log:
            mod.on_flow_completion(
                _make_flow_obj("etl"), _make_flow_run_obj("r"), _make_state_obj()
            )
            mock_log.assert_called_once()
            assert "FLOW COMPLETED" in mock_log.call_args[0][0]

    def test_sends_webhooks_when_configured(self):
        """Completion hook should send webhooks when URLs are set."""
        mod = _import_hooks(
            SLACK_WEBHOOK_URL="https://slack.test",
            ALERT_WEBHOOK_URL="https://alert.test",
        )
        with patch.object(mod, "_post_json") as mock_post:
            mod.on_flow_completion(_make_flow_obj(), _make_flow_run_obj(), _make_state_obj())
            assert mock_post.call_count == 2
            # First call: Slack
            slack_call = mock_post.call_args_list[0]
            assert slack_call[0][0] == "https://slack.test"
            assert "Flow Completed" in slack_call[0][1]["blocks"][0]["text"]["text"]
            # Second call: generic webhook
            generic_call = mock_post.call_args_list[1]
            assert generic_call[0][0] == "https://alert.test"
            assert generic_call[0][1]["event"] == "flow_completed"

    def test_no_webhooks_when_urls_not_set(self):
        """Completion hook should only log when no webhook URLs are configured."""
        mod = _import_hooks(
            SLACK_WEBHOOK_URL="",
            ALERT_WEBHOOK_URL="",
        )
        with patch.object(mod, "_post_json") as mock_post:
            mod.on_flow_completion(_make_flow_obj(), _make_flow_run_obj(), _make_state_obj())
            mock_post.assert_not_called()


# ===================================================================
# setup_automations.py — argument parsing
# ===================================================================
class TestAutomationsCli:
    """Test CLI argument parsing for setup_automations.py."""

    def _import_automations(self) -> types.ModuleType:
        spec = importlib.util.spec_from_file_location("setup_automations_test", AUTOMATIONS_PATH)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_main_exits_without_env_vars(self):
        """Should sys.exit(1) when no webhook URLs are set."""
        mod = self._import_automations()
        with (
            patch.dict(
                os.environ,
                {"SLACK_WEBHOOK_URL": "", "ALERT_WEBHOOK_URL": ""},
                clear=False,
            ),
            patch("sys.argv", ["setup_automations.py"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            import asyncio

            asyncio.run(mod.main())
        assert exc_info.value.code == 1

    def test_main_does_not_exit_with_delete_flag(self):
        """--delete should not require webhook URLs."""
        mod = self._import_automations()

        mock_client = AsyncMock()
        mock_client.read_automations_by_name = AsyncMock(return_value=[])

        async def fake_get_client():
            return mock_client

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        with (
            patch.dict(
                os.environ,
                {"SLACK_WEBHOOK_URL": "", "ALERT_WEBHOOK_URL": ""},
                clear=False,
            ),
            patch("sys.argv", ["setup_automations.py", "--delete"]),
            patch("prefect.client.orchestration.get_client", return_value=mock_ctx),
        ):
            import asyncio

            # Should not raise SystemExit
            asyncio.run(mod.main())

    def test_dry_run_does_not_create(self):
        """--dry-run should not actually call create_automation."""
        mod = self._import_automations()

        mock_client = AsyncMock()
        mock_client.read_automations_by_name = AsyncMock(return_value=[])
        mock_client.create_automation = AsyncMock()

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        # Mock the block loading to return a fake block ID
        with (
            patch.dict(
                os.environ,
                {
                    "SLACK_WEBHOOK_URL": "https://slack.test/x",
                    "ALERT_WEBHOOK_URL": "",
                },
                clear=False,
            ),
            patch("sys.argv", ["setup_automations.py", "--dry-run"]),
            patch("prefect.client.orchestration.get_client", return_value=mock_ctx),
            patch.object(
                mod,
                "_get_or_create_slack_block",
                new_callable=AsyncMock,
                return_value=FAKE_UUID,
            ),
        ):
            import asyncio

            asyncio.run(mod.main())
            mock_client.create_automation.assert_not_called()


# ===================================================================
# setup_automations.py — _create_automation
# ===================================================================
class TestCreateAutomation:
    """Test the _create_automation helper."""

    def _import_automations(self) -> types.ModuleType:
        spec = importlib.util.spec_from_file_location("setup_automations_test2", AUTOMATIONS_PATH)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    @pytest.mark.asyncio
    async def test_skips_when_already_exists(self):
        mod = self._import_automations()
        mock_client = AsyncMock()
        existing = MagicMock()
        existing.id = "existing-id"
        mock_client.read_automations_by_name = AsyncMock(return_value=[existing])
        mock_client.create_automation = AsyncMock()

        await mod._create_automation(mock_client, "test-auto", "desc", FAKE_UUID, dry_run=False)
        mock_client.create_automation.assert_not_called()

    @pytest.mark.asyncio
    async def test_creates_when_not_exists(self):
        mod = self._import_automations()
        mock_client = AsyncMock()
        mock_client.read_automations_by_name = AsyncMock(return_value=[])
        mock_client.create_automation = AsyncMock(return_value="new-id")

        await mod._create_automation(mock_client, "test-auto", "desc", FAKE_UUID, dry_run=False)
        mock_client.create_automation.assert_called_once()
        automation_arg = mock_client.create_automation.call_args[0][0]
        assert automation_arg.name == "test-auto"
        assert automation_arg.enabled is True

    @pytest.mark.asyncio
    async def test_dry_run_does_not_call_api(self):
        mod = self._import_automations()
        mock_client = AsyncMock()
        mock_client.read_automations_by_name = AsyncMock(return_value=[])
        mock_client.create_automation = AsyncMock()

        await mod._create_automation(mock_client, "test-auto", "desc", FAKE_UUID, dry_run=True)
        mock_client.create_automation.assert_not_called()

    @pytest.mark.asyncio
    async def test_automation_trigger_watches_failed_events(self):
        mod = self._import_automations()
        mock_client = AsyncMock()
        mock_client.read_automations_by_name = AsyncMock(return_value=[])
        mock_client.create_automation = AsyncMock(return_value="new-id")

        await mod._create_automation(mock_client, "test-auto", "desc", FAKE_UUID, dry_run=False)
        automation = mock_client.create_automation.call_args[0][0]
        assert "prefect.flow-run.Failed" in automation.trigger.expect


# ===================================================================
# setup_automations.py — _delete_automations
# ===================================================================
class TestDeleteAutomations:
    """Test the _delete_automations helper."""

    def _import_automations(self) -> types.ModuleType:
        spec = importlib.util.spec_from_file_location("setup_automations_test3", AUTOMATIONS_PATH)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    @pytest.mark.asyncio
    async def test_deletes_matching_automations(self):
        mod = self._import_automations()
        mock_client = AsyncMock()
        existing = MagicMock()
        existing.id = "auto-id-1"

        # First call for "flow-failure-slack-alert" returns existing,
        # second for "flow-failure-webhook-alert" returns empty,
        # then final check calls return empty
        mock_client.read_automations_by_name = AsyncMock(side_effect=[[existing], [], [], []])
        mock_client.delete_automation = AsyncMock()

        await mod._delete_automations(mock_client)
        mock_client.delete_automation.assert_called_once_with("auto-id-1")

    @pytest.mark.asyncio
    async def test_handles_no_existing_automations(self):
        mod = self._import_automations()
        mock_client = AsyncMock()
        mock_client.read_automations_by_name = AsyncMock(return_value=[])
        mock_client.delete_automation = AsyncMock()

        await mod._delete_automations(mock_client)
        mock_client.delete_automation.assert_not_called()


# ===================================================================
# .gitlab-ci.yml — deeper behavioral checks
# ===================================================================
class TestCiPipelineBehavior:
    """Deeper behavioral tests for the CI/CD pipeline configuration."""

    @pytest.fixture(scope="class")
    def ci_config(self):
        import yaml

        return yaml.safe_load((PROJECT_DIR / ".gitlab-ci.yml").read_text())

    @pytest.fixture(scope="class")
    def ci_text(self):
        return (PROJECT_DIR / ".gitlab-ci.yml").read_text()

    def test_stage_ordering(self, ci_config):
        """Stages must be ordered: test -> diff -> build -> deploy."""
        stages = ci_config["stages"]
        assert stages.index("test") < stages.index("diff")
        assert stages.index("diff") < stages.index("build")
        assert stages.index("build") < stages.index("deploy")

    def test_prefect_env_documented_in_header(self, ci_text):
        assert "PREFECT_ENV" in ci_text
        # Should be mentioned in the comments as a CI variable
        lines_before_stages = ci_text.split("stages:")[0]
        assert "PREFECT_ENV" in lines_before_stages

    def test_diff_passes_prefect_env(self, ci_config):
        script = " ".join(ci_config["diff"].get("script", []))
        assert "PREFECT_ENV" in script

    def test_deploy_passes_prefect_env(self, ci_config):
        script = " ".join(ci_config["deploy"].get("script", []))
        assert "PREFECT_ENV" in script

    def test_deploy_uploads_specs_before_deploying(self, ci_config):
        """Spec upload must come before deploy.py --all in the script list."""
        scripts = ci_config["deploy"]["script"]
        spec_idx = next((i for i, s in enumerate(scripts) if "PREFECT_SPECS" in s), None)
        deploy_idx = next(
            (i for i, s in enumerate(scripts) if "deploy.py --all" in s and "--diff" not in s),
            None,
        )
        assert spec_idx is not None, "Spec upload step missing"
        assert deploy_idx is not None, "deploy.py --all step missing"
        assert spec_idx < deploy_idx, "Specs must be uploaded before deploying"

    def test_deploy_syncs_flows_before_deploying(self, ci_config):
        """sync_flows must come before deploy.py --all."""
        scripts = ci_config["deploy"]["script"]
        sync_idx = next((i for i, s in enumerate(scripts) if "sync_flows" in s), None)
        deploy_idx = next(
            (i for i, s in enumerate(scripts) if "deploy.py --all" in s and "--diff" not in s),
            None,
        )
        assert sync_idx is not None
        assert deploy_idx is not None
        assert sync_idx < deploy_idx

    def test_deploy_shows_diff_before_final_deploy(self, ci_config):
        """--diff should come before --all (without --diff) in deploy."""
        scripts = ci_config["deploy"]["script"]
        diff_idx = next((i for i, s in enumerate(scripts) if "--diff" in s), None)
        deploy_idx = next(
            (i for i, s in enumerate(scripts) if "deploy.py --all" in s and "--diff" not in s),
            None,
        )
        assert diff_idx is not None
        assert deploy_idx is not None
        assert diff_idx < deploy_idx

    def test_diff_uses_mr_source_branch(self, ci_config):
        script = " ".join(ci_config["diff"].get("script", []))
        assert "CI_MERGE_REQUEST_SOURCE_BRANCH_NAME" in script

    def test_build_uses_snow_spcs_login(self, ci_config):
        before_items = ci_config["build"].get("before_script", [])
        # Flatten nested lists from YAML anchors
        flat = []
        for item in before_items:
            if isinstance(item, list):
                flat.extend(item)
            else:
                flat.append(item)
        before = " ".join(flat)
        assert "spcs image-registry login" in before

    def test_deploy_installs_snow_cli(self, ci_config):
        before_items = ci_config["deploy"].get("before_script", [])
        # Flatten nested lists from YAML anchors
        flat = []
        for item in before_items:
            if isinstance(item, list):
                flat.extend(item)
            else:
                flat.append(item)
        before = " ".join(flat)
        assert "snowflake-cli" in before

    def test_no_create_or_replace_service(self, ci_text):
        """CI should never use CREATE OR REPLACE SERVICE."""
        assert "CREATE OR REPLACE SERVICE" not in ci_text.upper()
