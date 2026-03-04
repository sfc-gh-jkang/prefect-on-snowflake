"""Prefect flow hooks for monitoring and alerting.

Provides reusable hook functions that can be attached to @flow decorators
via on_failure, on_completion, etc.

Webhook alerting is opt-in via environment variables:
  ALERT_WEBHOOK_URL  — generic webhook endpoint (POST JSON payload)
  SLACK_WEBHOOK_URL  — Slack incoming webhook (uses Slack block format)

If neither is set, hooks only log.  Both can be set simultaneously.

Usage in a flow module::

    from hooks import on_flow_failure

    @flow(name="my-flow", on_failure=[on_flow_failure])
    def my_flow():
        ...
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any

from shared_utils import get_secret_value

logger = logging.getLogger("prefect.hooks")

ALERT_WEBHOOK_URL = get_secret_value("alert-webhook-url", "ALERT_WEBHOOK_URL")
SLACK_WEBHOOK_URL = get_secret_value("slack-webhook-url", "SLACK_WEBHOOK_URL")
PREFECT_API_URL = os.environ.get("PREFECT_API_URL", "")


def _post_json(url: str, payload: dict) -> None:
    """POST a JSON payload to a URL.  Fire-and-forget — errors are logged."""
    try:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.debug("Webhook %s responded %s", url[:60], resp.status)
    except (urllib.error.URLError, OSError) as exc:
        logger.warning("Webhook delivery failed (%s): %s", url[:60], exc)


def _send_slack_failure(
    flow_name: str,
    run_name: str,
    run_id: str,
    message: str,
) -> None:
    """Send a Slack Block Kit message for a flow failure."""
    ui_url = f"{PREFECT_API_URL.rstrip('/api')}/flow-runs/flow-run/{run_id}"
    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f":x: Flow Failed: {flow_name}"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Run:*\n{run_name}"},
                    {"type": "mrkdwn", "text": f"*Flow:*\n{flow_name}"},
                ],
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Error:*\n```{message[:1500]}```"},
            },
        ],
    }
    if PREFECT_API_URL:
        payload["blocks"].append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View in Prefect"},
                        "url": ui_url,
                    }
                ],
            }
        )
    _post_json(SLACK_WEBHOOK_URL, payload)


def _send_slack_completion(
    flow_name: str,
    run_name: str,
    run_id: str,
) -> None:
    """Send a Slack Block Kit message for a flow completion."""
    ui_url = f"{PREFECT_API_URL.rstrip('/api')}/flow-runs/flow-run/{run_id}"
    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"Flow Completed: {flow_name}"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Run:*\n{run_name}"},
                    {"type": "mrkdwn", "text": f"*Flow:*\n{flow_name}"},
                ],
            },
        ],
    }
    if PREFECT_API_URL:
        payload["blocks"].append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View in Prefect"},
                        "url": ui_url,
                    }
                ],
            }
        )
    _post_json(SLACK_WEBHOOK_URL, payload)


def _send_generic_webhook(
    event: str,
    flow_name: str,
    run_name: str,
    run_id: str,
    message: str,
) -> None:
    """Send a generic JSON webhook payload."""
    payload = {
        "event": event,
        "flow_name": flow_name,
        "flow_run_name": run_name,
        "flow_run_id": str(run_id),
        "message": message,
    }
    if PREFECT_API_URL:
        payload["prefect_url"] = f"{PREFECT_API_URL.rstrip('/api')}/flow-runs/flow-run/{run_id}"
    _post_json(ALERT_WEBHOOK_URL, payload)


def on_flow_failure(flow: Any, flow_run: Any, state: Any) -> None:
    """Log detailed failure information and send webhook alerts.

    Attached to @flow(on_failure=[on_flow_failure]).
    """
    flow_name = getattr(flow, "name", "unknown")
    run_name = getattr(flow_run, "name", "unknown")
    run_id = getattr(flow_run, "id", "unknown")
    message = getattr(state, "message", "no message")

    logger.error(
        "FLOW FAILED | flow=%s run=%s id=%s | %s",
        flow_name,
        run_name,
        run_id,
        message,
    )

    if SLACK_WEBHOOK_URL:
        _send_slack_failure(flow_name, run_name, str(run_id), message)

    if ALERT_WEBHOOK_URL:
        _send_generic_webhook("flow_failed", flow_name, run_name, str(run_id), message)


def on_flow_completion(flow: Any, flow_run: Any, state: Any) -> None:
    """Log completion info and optionally send success notifications.

    Attached to @flow(on_completion=[on_flow_completion]).
    Webhook alerts are opt-in: only fires when ALERT_WEBHOOK_URL or
    SLACK_WEBHOOK_URL is set.  Useful for audit trails, SLA tracking,
    and daily digest notifications.
    """
    flow_name = getattr(flow, "name", "unknown")
    run_name = getattr(flow_run, "name", "unknown")
    run_id = getattr(flow_run, "id", "unknown")

    logger.info(
        "FLOW COMPLETED | flow=%s run=%s",
        flow_name,
        run_name,
    )

    if SLACK_WEBHOOK_URL:
        _send_slack_completion(flow_name, run_name, str(run_id))

    if ALERT_WEBHOOK_URL:
        _send_generic_webhook(
            "flow_completed",
            flow_name,
            run_name,
            str(run_id),
            "completed successfully",
        )
