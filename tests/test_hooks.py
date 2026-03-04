"""Unit tests for flows/hooks.py with mocked urllib.

Tests the actual behavior of hook functions — payload construction,
conditional webhook dispatch, and error handling — rather than just
checking source text.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

HOOKS_PATH = Path(__file__).resolve().parent.parent / "flows" / "hooks.py"


@pytest.fixture()
def hooks_module(monkeypatch):
    """Import hooks.py fresh with controlled env vars."""
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/test")
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://alert.example.com/hook")
    monkeypatch.setenv("PREFECT_API_URL", "https://prefect.example.com/api")

    spec = importlib.util.spec_from_file_location("hooks", HOOKS_PATH)
    mod = importlib.util.module_from_spec(spec)
    # Force re-read of env vars by reloading
    old = sys.modules.pop("hooks", None)
    sys.modules["hooks"] = mod
    spec.loader.exec_module(mod)
    yield mod
    sys.modules.pop("hooks", None)
    if old is not None:
        sys.modules["hooks"] = old


@pytest.fixture()
def hooks_no_webhooks(monkeypatch):
    """Import hooks.py with no webhook URLs configured."""
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("PREFECT_API_URL", "")

    spec = importlib.util.spec_from_file_location("hooks", HOOKS_PATH)
    mod = importlib.util.module_from_spec(spec)
    old = sys.modules.pop("hooks", None)
    sys.modules["hooks"] = mod
    spec.loader.exec_module(mod)
    yield mod
    sys.modules.pop("hooks", None)
    if old is not None:
        sys.modules["hooks"] = old


class TestPostJson:
    def test_sends_json_payload(self, hooks_module):
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
            hooks_module._post_json("https://example.com", {"key": "value"})
            mock_open.assert_called_once()
            req = mock_open.call_args[0][0]
            assert req.full_url == "https://example.com"
            assert req.get_header("Content-type") == "application/json"
            assert json.loads(req.data) == {"key": "value"}

    def test_logs_warning_on_failure(self, hooks_module):
        import urllib.error

        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.URLError("connection refused"),
        ):
            # Should not raise — fire-and-forget
            hooks_module._post_json("https://bad.example.com", {})


class TestSendSlackFailure:
    def test_payload_structure(self, hooks_module):
        with patch.object(hooks_module, "_post_json") as mock_post:
            hooks_module._send_slack_failure("my-flow", "run-abc", "id-123", "boom")
            mock_post.assert_called_once()
            url, payload = mock_post.call_args[0]
            assert url == "https://hooks.slack.com/test"
            assert "blocks" in payload
            # Header block
            header = payload["blocks"][0]
            assert header["type"] == "header"
            assert "my-flow" in header["text"]["text"]
            # Error section
            error_block = payload["blocks"][2]
            assert "boom" in error_block["text"]["text"]

    def test_includes_prefect_link_when_api_set(self, hooks_module):
        with patch.object(hooks_module, "_post_json") as mock_post:
            hooks_module._send_slack_failure("f", "r", "id-1", "err")
            payload = mock_post.call_args[0][1]
            actions = [b for b in payload["blocks"] if b["type"] == "actions"]
            assert len(actions) == 1
            btn_url = actions[0]["elements"][0]["url"]
            assert "id-1" in btn_url
            assert "prefect.example.com" in btn_url

    def test_truncates_long_messages(self, hooks_module):
        long_msg = "x" * 3000
        with patch.object(hooks_module, "_post_json") as mock_post:
            hooks_module._send_slack_failure("f", "r", "id-1", long_msg)
            payload = mock_post.call_args[0][1]
            error_text = payload["blocks"][2]["text"]["text"]
            # Message is truncated to 1500 chars inside the code block
            assert len(error_text) < 3000


class TestSendSlackCompletion:
    def test_payload_structure(self, hooks_module):
        with patch.object(hooks_module, "_post_json") as mock_post:
            hooks_module._send_slack_completion("my-flow", "run-abc", "id-123")
            mock_post.assert_called_once()
            url, payload = mock_post.call_args[0]
            assert url == "https://hooks.slack.com/test"
            header = payload["blocks"][0]
            assert "Completed" in header["text"]["text"]
            assert "my-flow" in header["text"]["text"]


class TestSendGenericWebhook:
    def test_payload_fields(self, hooks_module):
        with patch.object(hooks_module, "_post_json") as mock_post:
            hooks_module._send_generic_webhook(
                "flow_failed", "my-flow", "run-1", "id-1", "error msg"
            )
            mock_post.assert_called_once()
            url, payload = mock_post.call_args[0]
            assert url == "https://alert.example.com/hook"
            assert payload["event"] == "flow_failed"
            assert payload["flow_name"] == "my-flow"
            assert payload["flow_run_name"] == "run-1"
            assert payload["flow_run_id"] == "id-1"
            assert payload["message"] == "error msg"
            assert "prefect_url" in payload


class TestOnFlowFailure:
    def _make_mocks(self):
        flow = SimpleNamespace(name="test-flow")
        flow_run = SimpleNamespace(name="run-xyz", id="abc-123")
        state = SimpleNamespace(message="something broke")
        return flow, flow_run, state

    def test_sends_both_webhooks(self, hooks_module):
        flow, flow_run, state = self._make_mocks()
        with (
            patch.object(hooks_module, "_send_slack_failure") as mock_slack,
            patch.object(hooks_module, "_send_generic_webhook") as mock_generic,
        ):
            hooks_module.on_flow_failure(flow, flow_run, state)
            mock_slack.assert_called_once_with("test-flow", "run-xyz", "abc-123", "something broke")
            mock_generic.assert_called_once_with(
                "flow_failed", "test-flow", "run-xyz", "abc-123", "something broke"
            )

    def test_no_webhooks_when_urls_empty(self, hooks_no_webhooks):
        flow = SimpleNamespace(name="f")
        flow_run = SimpleNamespace(name="r", id="i")
        state = SimpleNamespace(message="m")
        with (
            patch.object(hooks_no_webhooks, "_send_slack_failure") as mock_slack,
            patch.object(hooks_no_webhooks, "_send_generic_webhook") as mock_generic,
        ):
            hooks_no_webhooks.on_flow_failure(flow, flow_run, state)
            mock_slack.assert_not_called()
            mock_generic.assert_not_called()

    def test_handles_missing_attributes(self, hooks_module):
        """Hook should not crash if flow/flow_run/state lack expected attrs."""
        with (
            patch.object(hooks_module, "_send_slack_failure"),
            patch.object(hooks_module, "_send_generic_webhook"),
        ):
            hooks_module.on_flow_failure(object(), object(), object())


class TestOnFlowCompletion:
    def test_sends_both_webhooks(self, hooks_module):
        flow = SimpleNamespace(name="done-flow")
        flow_run = SimpleNamespace(name="run-1", id="id-done")
        state = SimpleNamespace(message="ok")
        with (
            patch.object(hooks_module, "_send_slack_completion") as mock_slack,
            patch.object(hooks_module, "_send_generic_webhook") as mock_generic,
        ):
            hooks_module.on_flow_completion(flow, flow_run, state)
            mock_slack.assert_called_once_with("done-flow", "run-1", "id-done")
            mock_generic.assert_called_once()
            args = mock_generic.call_args[0]
            assert args[0] == "flow_completed"
            assert args[1] == "done-flow"

    def test_no_webhooks_when_urls_empty(self, hooks_no_webhooks):
        flow = SimpleNamespace(name="f")
        flow_run = SimpleNamespace(name="r", id="i")
        state = SimpleNamespace(message="ok")
        with (
            patch.object(hooks_no_webhooks, "_send_slack_completion") as mock_slack,
            patch.object(hooks_no_webhooks, "_send_generic_webhook") as mock_generic,
        ):
            hooks_no_webhooks.on_flow_completion(flow, flow_run, state)
            mock_slack.assert_not_called()
            mock_generic.assert_not_called()
