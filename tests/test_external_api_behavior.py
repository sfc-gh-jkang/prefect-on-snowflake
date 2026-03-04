"""Behavioral tests for flows/external_api_flow.py.

Tests call_external_api() with mocked requests.get — no live HTTP needed.
"""

import importlib.util
import sys
import types
from unittest.mock import MagicMock, patch

import pytest
from conftest import FLOWS_DIR

FLOW_PATH = FLOWS_DIR / "external_api_flow.py"


def _import_flow() -> types.ModuleType:
    """Import external_api_flow.py with hooks mock (avoids env var side effects)."""
    # Provide a stub hooks module so the flow import doesn't fail
    mock_hooks = types.ModuleType("hooks")
    mock_hooks.on_flow_failure = MagicMock()
    mock_hooks.on_flow_completion = MagicMock()

    old_hooks = sys.modules.get("hooks")
    sys.modules["hooks"] = mock_hooks
    try:
        spec = importlib.util.spec_from_file_location("external_api_flow_test", FLOW_PATH)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    finally:
        if old_hooks is not None:
            sys.modules["hooks"] = old_hooks
        else:
            sys.modules.pop("hooks", None)


class TestCallExternalApi:
    """Test the call_external_api task function."""

    def test_returns_json_data(self):
        """Should return parsed JSON from the response."""
        mod = _import_flow()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "origin": "1.2.3.4",
            "url": "https://httpbin.org/get",
        }

        with patch("requests.get", return_value=mock_response):
            # call_external_api is a Prefect task; call .fn to bypass decorator
            result = mod.call_external_api.fn("https://httpbin.org/get")

        assert result == {"origin": "1.2.3.4", "url": "https://httpbin.org/get"}

    def test_passes_url_to_requests_get(self):
        """The exact URL should be forwarded to requests.get."""
        mod = _import_flow()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}

        with patch("requests.get", return_value=mock_response) as mock_get:
            mod.call_external_api.fn("https://httpbin.org/ip")

        mock_get.assert_called_once_with("https://httpbin.org/ip", timeout=10)

    def test_sets_timeout(self):
        """requests.get should be called with timeout=10."""
        mod = _import_flow()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}

        with patch("requests.get", return_value=mock_response) as mock_get:
            mod.call_external_api.fn("https://httpbin.org/get")

        _, kwargs = mock_get.call_args
        assert kwargs["timeout"] == 10

    def test_calls_raise_for_status(self):
        """Should call response.raise_for_status() to propagate HTTP errors."""
        mod = _import_flow()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}

        with patch("requests.get", return_value=mock_response):
            mod.call_external_api.fn("https://httpbin.org/get")

        mock_response.raise_for_status.assert_called_once()

    def test_propagates_http_error(self):
        """HTTP errors from raise_for_status should propagate."""
        mod = _import_flow()
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = Exception("404 Not Found")

        with (
            patch("requests.get", return_value=mock_response),
            pytest.raises(Exception, match="404 Not Found"),
        ):
            mod.call_external_api.fn("https://httpbin.org/get")

    def test_returns_dict_type(self):
        """Return type should be a dict."""
        mod = _import_flow()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"key": "value"}

        with patch("requests.get", return_value=mock_response):
            result = mod.call_external_api.fn("https://httpbin.org/get")

        assert isinstance(result, dict)

    def test_rejects_url_not_in_allowlist(self):
        """URLs not in ALLOWED_URLS should raise ValueError."""
        mod = _import_flow()
        with pytest.raises(ValueError, match="URL not in allowlist"):
            mod.call_external_api.fn("https://evil.example.com/steal")
