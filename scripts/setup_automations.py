"""Set up Prefect OSS automations for flow failure notifications.

Creates notification blocks and automations that trigger when any flow
run enters a Failed state.  Idempotent — safe to re-run.

Required environment variables:
  PREFECT_API_URL     — Prefect server API URL
  SLACK_WEBHOOK_URL   — (optional) Slack incoming webhook URL
  ALERT_WEBHOOK_URL   — (optional) generic webhook endpoint

Usage:
  uv run python scripts/setup_automations.py
  uv run python scripts/setup_automations.py --dry-run   # show what would be created
  uv run python scripts/setup_automations.py --delete     # remove automations
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import timedelta

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


async def _get_or_create_slack_block(client, webhook_url: str) -> str:
    """Return the block_document_id for a Slack webhook block, creating if needed."""
    from prefect.blocks.webhook import Webhook

    block_name = "prefect-alerts-slack"
    try:
        block = await Webhook.load(block_name)
        logger.info("Found existing Slack webhook block: %s", block_name)
        # Read the block document to get its ID
        doc = await client.read_block_document_by_name(block_name, "webhook")
        return str(doc.id)
    except ValueError:
        logger.info("Creating Slack webhook block: %s", block_name)
        block = Webhook(url=webhook_url, method="POST")
        doc_id = await block.save(block_name, overwrite=True)
        return str(doc_id)


async def _get_or_create_generic_block(client, webhook_url: str) -> str:
    """Return the block_document_id for a generic webhook block, creating if needed."""
    from prefect.blocks.webhook import Webhook

    block_name = "prefect-alerts-webhook"
    try:
        block = await Webhook.load(block_name)
        logger.info("Found existing generic webhook block: %s", block_name)
        doc = await client.read_block_document_by_name(block_name, "webhook")
        return str(doc.id)
    except ValueError:
        logger.info("Creating generic webhook block: %s", block_name)
        block = Webhook(url=webhook_url, method="POST")
        doc_id = await block.save(block_name, overwrite=True)
        return str(doc_id)


async def _create_automation(
    client,
    name: str,
    description: str,
    block_document_id: str,
    dry_run: bool = False,
) -> None:
    """Create a reactive automation that fires on any flow run failure."""
    from prefect.events.actions import CallWebhook
    from prefect.events.schemas.automations import (
        AutomationCore,
        EventTrigger,
        Posture,
    )

    # Check if automation already exists
    existing = await client.read_automations_by_name(name)
    if existing:
        logger.info("Automation already exists: %s (id=%s)", name, existing[0].id)
        return

    if dry_run:
        logger.info("[DRY RUN] Would create automation: %s", name)
        logger.info("  Trigger: on prefect.flow-run.Failed")
        logger.info("  Action: CallWebhook (block=%s)", block_document_id)
        return

    automation = AutomationCore(
        name=name,
        description=description,
        enabled=True,
        trigger=EventTrigger(
            expect={"prefect.flow-run.Failed"},
            match={"prefect.resource.id": "prefect.flow-run.*"},
            posture=Posture.Reactive,
            threshold=1,
            within=timedelta(seconds=0),
        ),
        actions=[
            CallWebhook(
                block_document_id=block_document_id,
                payload='{"event": "flow_failed", "flow_run": "{{ flow_run.name }}", "flow": "{{ flow.name }}", "message": "{{ flow_run.state.message }}"}',
            ),
        ],
    )

    automation_id = await client.create_automation(automation)
    logger.info("Created automation: %s (id=%s)", name, automation_id)


async def _delete_automations(client) -> None:
    """Delete all automations created by this script."""
    names = ["flow-failure-slack-alert", "flow-failure-webhook-alert"]
    for name in names:
        existing = await client.read_automations_by_name(name)
        for a in existing:
            await client.delete_automation(a.id)
            logger.info("Deleted automation: %s (id=%s)", name, a.id)
    if not any([await client.read_automations_by_name(n) for n in names]):
        logger.info("No matching automations found to delete")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Set up Prefect automations")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be created")
    parser.add_argument("--delete", action="store_true", help="Remove automations")
    args = parser.parse_args()

    slack_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    generic_url = os.environ.get("ALERT_WEBHOOK_URL", "")

    if not slack_url and not generic_url and not args.delete:
        logger.error("Set SLACK_WEBHOOK_URL and/or ALERT_WEBHOOK_URL environment variables")
        sys.exit(1)

    from prefect.client.orchestration import get_client

    async with get_client() as client:
        if args.delete:
            await _delete_automations(client)
            return

        if slack_url:
            block_id = await _get_or_create_slack_block(client, slack_url)
            await _create_automation(
                client,
                name="flow-failure-slack-alert",
                description="Send Slack notification when any flow run fails",
                block_document_id=block_id,
                dry_run=args.dry_run,
            )

        if generic_url:
            block_id = await _get_or_create_generic_block(client, generic_url)
            await _create_automation(
                client,
                name="flow-failure-webhook-alert",
                description="POST to webhook when any flow run fails",
                block_document_id=block_id,
                dry_run=args.dry_run,
            )

    logger.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
