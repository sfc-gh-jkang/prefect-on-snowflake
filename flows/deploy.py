"""Deploy flows to the Prefect server.

Pool configuration is loaded from pools.yaml at the project root.
Add new external workers (Azure, AWS, K8s) by adding entries there.

Usage:
  # Deploy to SPCS (default)
  PREFECT_API_URL=https://<spcs-endpoint>/api python flows/deploy.py

  # Deploy to a specific pool
  PREFECT_API_URL=https://<spcs-endpoint>/api python flows/deploy.py --pool gcp

  # Deploy to multiple specific pools
  PREFECT_API_URL=https://<spcs-endpoint>/api python flows/deploy.py --pool gcp --pool azure

  # Deploy to all pools defined in pools.yaml
  PREFECT_API_URL=https://<spcs-endpoint>/api python flows/deploy.py --all

  # Deploy a single flow by name (to default pool)
  PREFECT_API_URL=https://<spcs-endpoint>/api python flows/deploy.py --name quarterly-report

  # Show what would change without deploying (plan-style diff)
  PREFECT_API_URL=https://<spcs-endpoint>/api python flows/deploy.py --diff
  PREFECT_API_URL=https://<spcs-endpoint>/api python flows/deploy.py --diff --pool gcp

  # Validate all registry entries offline (no server connection needed)
  python flows/deploy.py --validate

Environment variables for git-based deployment (all pools when configured):
  GIT_REPO_URL      — HTTPS URL to your git repo (GitHub, GitLab, BitBucket, etc.)
  GIT_BRANCH        — Branch to clone (default: main)
  GIT_ACCESS_TOKEN  — Access token for private repos (set on the worker, not here)

Code delivery:
  When GIT_REPO_URL is set, all pools use git_clone as the primary pull step.
  SPCS falls back to the stage volume mount when GIT_REPO_URL is not configured.
  After SPCS deployments, sync_flows.sh auto-runs to keep the stage fallback fresh.
"""

from __future__ import annotations

import ast
import asyncio
import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml

# Ensure flows directory is on the path regardless of cwd
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import DeploymentScheduleCreate
from prefect.client.schemas.schedules import (
    CronSchedule,
    IntervalSchedule,
    RRuleSchedule,
)
from shared_utils import get_git_sha

# Environment name for multi-environment support.
# When set, deployment names are prefixed: "dev/example-flow-local", "prod/example-flow-gcp"
# and an env tag is added to all deployments (e.g., "env:dev", "env:prod").
PREFECT_ENV = os.environ.get("PREFECT_ENV", "")


# The SPCS worker mounts flows at /opt/prefect/flows via stage volume v2.
WORKER_FLOWS_DIR = "/opt/prefect/flows"

# Git repo for git_clone pull step (external workers clone at runtime).
# Works with any HTTPS git provider: GitHub, GitLab, BitBucket, Azure DevOps, etc.
# Prefect auto-detects the provider from the URL and formats credentials accordingly.
GIT_REPO = os.environ.get("GIT_REPO_URL", "")
GIT_BRANCH = os.environ.get("GIT_BRANCH", "main")

# ---------------------------------------------------------------------------
# Pool configuration — loaded from pools.yaml
#
# Each pool entry has:
#   type:      "spcs" (stage volume fallback) or "external" (git_clone only)
#   pool_name: Prefect work pool name
#   suffix:    Deployment name suffix (e.g., "example-flow-{suffix}")
#   tag:       Tag applied to all deployments on this pool
#   worker_dir: (external only) relative path to worker docker-compose dir
# ---------------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent.parent
FLOWS_DIR = Path(__file__).resolve().parent
# Look for pools.yaml at project root first, then in the same dir as deploy.py
# (stage volume mounts flows at /opt/prefect/flows/ — pools.yaml lives there too)
POOLS_FILE = PROJECT_DIR / "pools.yaml"
if not POOLS_FILE.exists():
    POOLS_FILE = FLOWS_DIR / "pools.yaml"


def _load_pools() -> dict[str, dict]:
    """Load pool configuration from pools.yaml."""
    if not POOLS_FILE.exists():
        raise FileNotFoundError(
            f"pools.yaml not found at {PROJECT_DIR / 'pools.yaml'} or "
            f"{FLOWS_DIR / 'pools.yaml'}. Create it to define your work pools."
        )
    with open(POOLS_FILE) as f:
        data = yaml.safe_load(f)
    pools = data.get("pools", {})
    if not pools:
        raise ValueError("pools.yaml must define at least one pool under 'pools:'")
    return pools


POOLS = _load_pools()


# ---------------------------------------------------------------------------
# FlowSpec — define each flow ONCE with full Prefect deployment features.
#
# To add a new flow:
#   1. Add a FlowSpec entry to FLOW_REGISTRY below
#   2. Deploy:  python flows/deploy.py --all  (auto-syncs stage for fallback)
#   3. Push:    git push  (workers clone at runtime)
# ---------------------------------------------------------------------------


@dataclass
class TriggerSpec:
    """Declarative event trigger for a deployment.

    Creates a Prefect automation that runs the deployment when the specified
    event(s) occur.  Supports reactive triggers (run when event fires) and
    proactive triggers (run when event does NOT fire within a window).

    Examples:
        # Run deployment when another flow completes successfully
        TriggerSpec(
            expect={"prefect.flow-run.Completed"},
            match_related={"prefect.resource.name": "upstream-flow"},
        )

        # Proactive: run if no heartbeat event within 10 minutes
        TriggerSpec(
            expect={"monitoring.heartbeat"},
            posture="proactive",
            within=600,
        )
    """

    expect: set[str]  # event names to match
    match: dict[str, str | list[str]] | None = None  # resource filter
    match_related: dict[str, str | list[str]] | None = None  # related resource filter
    posture: str = "reactive"  # "reactive" or "proactive"
    threshold: int = 1  # number of events before firing
    within: int = 0  # seconds window
    parameters: dict[str, Any] | None = None  # override parameters for triggered run


@dataclass
class FlowSpec:
    """Specification for a deployable flow.

    Mirrors the full Prefect deployment API so developers get feature parity
    with native ``flow.deploy()`` while keeping deployment purely declarative
    (no file parsing needed — flow names are derived from the function name).
    """

    # --- Required ---
    path: str  # file path relative to flows/ (e.g., "analytics/reports/quarterly_flow.py")
    func: str  # entrypoint function name
    name: str  # base deployment name (suffixed per pool: "foo" → "foo-local", "foo-gcp")
    tags: list[str] = field(default_factory=list)
    pools: list[str] | str = "all"  # pool keys from pools.yaml, or "all" for every pool

    # --- Flow identity ---
    flow_name: str | None = None  # Prefect flow name override; defaults to func.replace('_', '-')

    # --- Parameters ---
    parameters: dict[str, Any] | None = None  # default parameter values
    enforce_parameter_schema: bool = True  # reject unknown parameters at runtime

    # --- Scheduling (convenience shortcuts) ---
    cron: str | None = None  # cron expression, e.g. "0 6 * * *"
    interval: int | None = None  # seconds between runs, e.g. 3600
    rrule: str | None = None  # iCal RRULE string
    timezone: str | None = None  # timezone for schedule, e.g. "America/New_York"
    schedules: list[DeploymentScheduleCreate] | None = None  # advanced: raw schedule objects

    # --- Concurrency ---
    concurrency_limit: int | None = None  # max concurrent runs
    collision_strategy: str = "ENQUEUE"  # ENQUEUE or CANCEL_NEW

    # --- Metadata ---
    description: str | None = None
    version: str | None = None
    paused: bool | None = None  # deploy with schedule paused

    # --- Worker overrides ---
    job_variables: dict[str, Any] | None = None  # env vars, working dir overrides, etc.

    # --- Event triggers (automations) ---
    triggers: list[TriggerSpec] = field(default_factory=list)  # reactive/proactive event triggers

    def build_schedules(self) -> list[DeploymentScheduleCreate] | None:
        """Build DeploymentScheduleCreate list from convenience fields or raw schedules."""
        # If raw schedules provided, use them directly
        if self.schedules is not None:
            return self.schedules

        result = []
        if self.cron:
            result.append(
                DeploymentScheduleCreate(
                    schedule=CronSchedule(cron=self.cron, timezone=self.timezone),
                    active=not self.paused if self.paused is not None else True,
                    parameters=self.parameters or {},
                )
            )
        if self.interval:
            result.append(
                DeploymentScheduleCreate(
                    schedule=IntervalSchedule(interval=timedelta(seconds=self.interval)),
                    active=not self.paused if self.paused is not None else True,
                    parameters=self.parameters or {},
                )
            )
        if self.rrule:
            result.append(
                DeploymentScheduleCreate(
                    schedule=RRuleSchedule(rrule=self.rrule, timezone=self.timezone),
                    active=not self.paused if self.paused is not None else True,
                    parameters=self.parameters or {},
                )
            )
        return result or None

    def build_concurrency_options(self):
        """Build ConcurrencyOptions if concurrency_limit is set."""
        if self.concurrency_limit is None:
            return None, None
        from prefect.client.schemas.objects import (
            ConcurrencyLimitStrategy,
            ConcurrencyOptions,
        )

        strategy = ConcurrencyLimitStrategy(self.collision_strategy)
        return self.concurrency_limit, ConcurrencyOptions(collision_strategy=strategy)

    def build_automations(self, deploy_name: str, deployment_id) -> list:
        """Build AutomationCore objects for each TriggerSpec.

        Creates one automation per trigger.  Each automation fires a
        RunDeployment action targeting *this* deployment.

        Args:
            deploy_name: Fully-qualified deployment name (e.g., "dev/example-flow-local").
            deployment_id: UUID of the deployment returned by create_deployment.

        Returns:
            List of AutomationCore objects ready for client.create_automation().
        """
        if not self.triggers:
            return []

        from prefect.events.actions import RunDeployment
        from prefect.events.schemas.automations import (
            AutomationCore,
            EventTrigger,
            Posture,
        )

        automations = []
        for i, trig in enumerate(self.triggers):
            posture = Posture.Proactive if trig.posture == "proactive" else Posture.Reactive
            suffix = f"-{i}" if len(self.triggers) > 1 else ""
            auto_name = f"trigger/{deploy_name}{suffix}"

            automations.append(
                AutomationCore(
                    name=auto_name,
                    description=f"Auto-trigger for deployment {deploy_name}",
                    enabled=True,
                    trigger=EventTrigger(
                        expect=trig.expect,
                        match=trig.match or {},
                        match_related=trig.match_related or {},
                        posture=posture,
                        threshold=trig.threshold,
                        within=timedelta(seconds=trig.within),
                    ),
                    actions=[
                        RunDeployment(
                            source="selected",
                            deployment_id=deployment_id,
                            parameters=trig.parameters,
                        ),
                    ],
                )
            )
        return automations


# ---------------------------------------------------------------------------
# Flow Registry — the single source of truth for all deployments.
#
# deploy_name gets a pool-specific suffix appended automatically:
#   "example-flow" → "example-flow-local" (SPCS) / "example-flow-gcp" (GCP)
# ---------------------------------------------------------------------------
FLOW_REGISTRY = [
    FlowSpec(
        path="example_flow.py",
        func="example_flow",
        name="example-flow",
        tags=["example"],
        description="Hello world flow to verify worker connectivity",
        parameters={"name": "Prefect-SPCS"},
        interval=3600,  # every hour
    ),
    FlowSpec(
        path="snowflake_flow.py",
        func="snowflake_etl",
        name="snowflake-etl",
        tags=["snowflake"],
        description="Run a Snowflake query from the Prefect worker",
        cron="0 6 * * *",  # daily at 6am UTC
        timezone="UTC",
    ),
    FlowSpec(
        path="external_api_flow.py",
        func="external_api_flow",
        name="external-api",
        tags=["eai"],
        description="External API call demonstrating EAI egress",
    ),
    FlowSpec(
        path="e2e_test_flow.py",
        func="e2e_pipeline_test",
        name="e2e-test",
        tags=["e2e"],
        description="End-to-end pipeline: sample data -> table -> dynamic table -> verify -> cleanup",
        concurrency_limit=1,
        collision_strategy="CANCEL_NEW",
    ),
    FlowSpec(
        path="analytics/revenue_flow.py",
        func="analytics_revenue",
        name="analytics-revenue",
        tags=["analytics"],
        description="Multi-file subfolder flow demonstrating sibling imports",
    ),
    FlowSpec(
        path="analytics/reports/quarterly_flow.py",
        func="quarterly_report",
        name="quarterly-report",
        tags=["analytics", "nested"],
        description="Deep nested import stress test: sibling, package, and root imports",
        concurrency_limit=1,
        cron="0 0 1 1,4,7,10 *",  # quarterly: 1st day of Jan/Apr/Jul/Oct
        timezone="US/Pacific",
    ),
    FlowSpec(
        path="alert_test_flow.py",
        func="alert_test_flow",
        name="alert-test",
        tags=["test", "alerting"],
        description="Optionally fails to validate alert pipeline end-to-end (pass should_fail=true to trigger)",
        parameters={"should_fail": False},
    ),
    FlowSpec(
        path="data_quality_flow.py",
        func="data_quality_check",
        name="data-quality",
        tags=["data-quality", "monitoring"],
        description="Table freshness, row count, and existence checks",
        cron="0 7 * * *",  # daily at 7am UTC
        timezone="UTC",
    ),
    FlowSpec(
        path="stage_cleanup_flow.py",
        func="stage_cleanup",
        name="stage-cleanup",
        tags=["maintenance", "storage"],
        description="Remove stale files from Snowflake internal stages",
        cron="0 3 * * *",  # daily at 3am UTC
        timezone="UTC",
        parameters={"retention_days": 30},
    ),
    FlowSpec(
        path="health_check_flow.py",
        func="health_check",
        name="health-check",
        tags=["monitoring", "infra"],
        pools=["spcs"],  # SPCS-only: checks services, compute pools, stages via Snowflake SQL
        description="SPCS service, compute pool, and connectivity health checks",
        interval=900,  # every 15 minutes
        concurrency_limit=1,
        collision_strategy="CANCEL_NEW",
    ),
]


def _build_pull_steps(pool_key: str, entrypoint: str) -> list[dict]:
    """Build pool-specific pull steps based on pool type from pools.yaml.

    Pool types:
      - "spcs": git_clone primary, stage volume fallback when GIT_REPO_URL unset
      - "external": git_clone only, requires GIT_REPO_URL

    The git_clone step works with any HTTPS git provider (GitHub, GitLab, BitBucket,
    Azure DevOps). Prefect auto-detects the provider from the repo URL and formats
    the access_token accordingly (e.g., oauth2: prefix for GitLab, plain for GitHub).
    """
    pool_cfg = POOLS[pool_key]
    pool_type = pool_cfg.get("type", "external")

    git_clone_steps = [
        {
            "prefect.deployments.steps.git_clone": {
                "id": "clone",
                "repository": GIT_REPO,
                "branch": GIT_BRANCH,
                "access_token": "{{ $GIT_ACCESS_TOKEN }}",
            }
        },
        {
            "prefect.deployments.steps.set_working_directory": {
                "directory": "{{ clone.directory }}/flows"
            }
        },
    ]

    if pool_type == "spcs":
        if GIT_REPO:
            return git_clone_steps
        else:
            # Fallback: stage volume v2 mount at /opt/prefect/flows
            return [
                {"prefect.deployments.steps.set_working_directory": {"directory": WORKER_FLOWS_DIR}}
            ]
    else:
        # All external pool types use git_clone
        if not GIT_REPO:
            pool_name = pool_cfg.get("pool_name", pool_key)
            raise ValueError(
                f"GIT_REPO_URL env var is required for external pool '{pool_name}'. "
                "Set it to your repo's HTTPS URL (GitHub, GitLab, BitBucket, etc.)."
            )
        return git_clone_steps


async def deploy_to_pool(pool_key: str, name_filter: str | None = None):
    """Deploy flows from FLOW_REGISTRY to a specific pool.

    Registers deployments with pool-specific pull_steps. Flow names are derived
    from FlowSpec.func (no file parsing needed — runs from anywhere).

    Pull step strategy (based on pool type in pools.yaml):
      - type "spcs": git_clone primary, stage volume fallback
      - type "external": git_clone only, requires GIT_REPO_URL

    Args:
        pool_key: Key from pools.yaml (e.g., "spcs", "gcp", "azure")
        name_filter: If set, only deploy the flow with this base name.
    """
    pool_cfg = POOLS[pool_key]
    pool_name = pool_cfg["pool_name"]
    suffix = pool_cfg["suffix"]
    pool_tag = pool_cfg["tag"]
    all_pool_keys = list(POOLS.keys())

    # Resolve version once: use git SHA if no explicit version on any FlowSpec.
    git_sha = get_git_sha()

    async with get_client() as client:
        for spec in FLOW_REGISTRY:
            # Resolve "all" to list of all pool keys
            target_pools = all_pool_keys if spec.pools == "all" else spec.pools

            # Skip flows not targeting this pool
            if pool_key not in target_pools:
                continue

            # Skip if name filter is set and doesn't match
            if name_filter and spec.name != name_filter:
                continue

            deploy_name = f"{spec.name}-{suffix}"
            merged_tags = [pool_tag, *spec.tags]

            # Multi-environment: prefix deployment name and add env tag
            if PREFECT_ENV:
                deploy_name = f"{PREFECT_ENV}/{deploy_name}"
                merged_tags.append(f"env:{PREFECT_ENV}")

            # Derive flow name from function name (Prefect convention: _ → -)
            # or use explicit flow_name override if provided.
            derived_name = spec.flow_name or spec.func.replace("_", "-")
            flow_id = await client.create_flow_from_name(derived_name)

            # Build pool-specific pull steps and schedule/concurrency options
            local_entrypoint = f"{spec.path}:{spec.func}"
            pull_steps = _build_pull_steps(pool_key, local_entrypoint)
            schedules = spec.build_schedules()
            conc_limit, conc_options = spec.build_concurrency_options()

            deployment_id = await client.create_deployment(
                flow_id=flow_id,
                name=deploy_name,
                work_pool_name=pool_name,
                tags=merged_tags,
                entrypoint=local_entrypoint,
                pull_steps=pull_steps,
                # Full Prefect deployment features
                parameters=spec.parameters,
                schedules=schedules,
                concurrency_limit=conc_limit,
                concurrency_options=conc_options,
                description=spec.description,
                version=spec.version or git_sha,
                paused=spec.paused,
                enforce_parameter_schema=spec.enforce_parameter_schema,
                job_variables=spec.job_variables,
            )

            # Create event-trigger automations (idempotent: replaces by name)
            for automation in spec.build_automations(deploy_name, deployment_id):
                existing = await client.read_automations_by_name(automation.name)
                if existing:
                    await client.delete_automation(existing[0].id)
                await client.create_automation(automation)
                print(f"    Trigger: {automation.name} (events={automation.trigger.expect})")

            sched_str = ""
            if spec.cron:
                sched_str = f" cron={spec.cron}"
            elif spec.interval:
                sched_str = f" interval={spec.interval}s"
            elif spec.rrule:
                sched_str = " rrule=..."
            version_str = spec.version or git_sha
            print(f"  Deployed: {deploy_name} -> pool={pool_name} version={version_str}{sched_str}")


def _parse_args() -> tuple[list[str], str | None, bool, bool]:
    """Parse CLI arguments. Returns (pool_keys, name_filter, validate, diff).

    --pool <key>  Deploy to a specific pool (repeatable)
    --all         Deploy to all pools in pools.yaml
    --name <name> Filter to a single flow by base name
    --validate    Check all registry entries offline (no server needed)
    --diff        Show what would change without deploying (plan-style diff)
    (no flags)    Deploy to SPCS only (default)
    """
    pool_keys: list[str] = []
    name_filter = None
    validate = False
    diff = False
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--pool" and i + 1 < len(args):
            key = args[i + 1]
            if key not in POOLS:
                available = ", ".join(POOLS.keys())
                print(f"Error: unknown pool '{key}'. Available: {available}")
                sys.exit(1)
            pool_keys.append(key)
            i += 2
        elif args[i] == "--all":
            pool_keys = list(POOLS.keys())
            i += 1
        elif args[i] == "--name" and i + 1 < len(args):
            name_filter = args[i + 1]
            i += 2
        elif args[i] == "--gcp":
            # Backwards compatibility: --gcp is equivalent to --pool gcp
            if "gcp" in POOLS:
                pool_keys.append("gcp")
            else:
                print("Error: --gcp flag used but no 'gcp' pool in pools.yaml")
                sys.exit(1)
            i += 1
        elif args[i] == "--validate":
            validate = True
            i += 1
        elif args[i] == "--diff":
            diff = True
            i += 1
        else:
            i += 1

    # Default: SPCS only
    if not pool_keys and not validate:
        pool_keys = ["spcs"]

    return pool_keys, name_filter, validate, diff


def _validate() -> bool:
    """Validate all FLOW_REGISTRY entries offline (no server connection needed).

    Checks:
      1. Flow file exists
      2. File is valid Python (ast.parse)
      3. Entrypoint function exists in the file's AST
      4. Pool keys are valid (exist in POOLS or "all")
      5. Cron expression is valid (5 fields)
      6. Collision strategy is ENQUEUE or CANCEL_NEW
      7. Trigger specs have valid posture, expect, threshold, within
      8. No duplicate flow names
      9. External pools warn if GIT_REPO_URL is unset

    Returns True if all checks pass, False otherwise.
    """
    flows_dir = Path(__file__).resolve().parent
    all_pool_keys = set(POOLS.keys())
    valid_strategies = {"ENQUEUE", "CANCEL_NEW"}
    passed = 0
    failed = 0
    errors: list[str] = []

    print(f"Validating {len(FLOW_REGISTRY)} flow specs against {len(POOLS)} pools...\n")

    # Check for duplicate names
    seen_names: dict[str, int] = {}
    for i, spec in enumerate(FLOW_REGISTRY):
        if spec.name in seen_names:
            errors.append(
                f"  WARN: duplicate name '{spec.name}' at registry index {seen_names[spec.name]} and {i}"
            )
        seen_names[spec.name] = i

    for spec in FLOW_REGISTRY:
        spec_errors: list[str] = []
        flow_path = flows_dir / spec.path

        # 1. File exists
        if not flow_path.exists():
            spec_errors.append(f"file not found: {spec.path}")
        else:
            # 2. Valid Python
            source = flow_path.read_text()
            try:
                tree = ast.parse(source)
            except SyntaxError as e:
                spec_errors.append(f"syntax error at line {e.lineno}: {e.msg}")
                tree = None

            # 3. Function exists in AST
            if tree is not None:
                func_names = {
                    node.name
                    for node in ast.walk(tree)
                    if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
                }
                if spec.func not in func_names:
                    spec_errors.append(f"function '{spec.func}' not found in {spec.path}")

        # 4. Pool keys valid
        if spec.pools != "all":
            for key in spec.pools:
                if key not in all_pool_keys:
                    spec_errors.append(
                        f"unknown pool key '{key}' (available: {', '.join(all_pool_keys)})"
                    )

        # 5. Cron parseable (5 fields)
        if spec.cron:
            fields = spec.cron.strip().split()
            if len(fields) != 5:
                spec_errors.append(
                    f"invalid cron '{spec.cron}' (expected 5 fields, got {len(fields)})"
                )

        # 6. Collision strategy valid
        if spec.collision_strategy not in valid_strategies:
            spec_errors.append(
                f"invalid collision_strategy '{spec.collision_strategy}' (expected: {', '.join(valid_strategies)})"
            )

        # 7. Trigger specs valid
        valid_postures = {"reactive", "proactive"}
        for i, trig in enumerate(spec.triggers):
            prefix = f"trigger[{i}]"
            if not trig.expect:
                spec_errors.append(f"{prefix}: 'expect' must be a non-empty set of event names")
            if trig.posture not in valid_postures:
                spec_errors.append(
                    f"{prefix}: invalid posture '{trig.posture}' (expected: {', '.join(valid_postures)})"
                )
            if trig.within < 0:
                spec_errors.append(f"{prefix}: 'within' must be >= 0 (got {trig.within})")
            if trig.threshold < 1:
                spec_errors.append(f"{prefix}: 'threshold' must be >= 1 (got {trig.threshold})")

        # Print result
        if spec_errors:
            failed += 1
            print(f"  FAIL  {spec.name:<24} file={spec.path}  func={spec.func}")
            for err in spec_errors:
                print(f"        ERROR: {err}")
        else:
            passed += 1
            print(f"  OK    {spec.name:<24} file={spec.path}  func={spec.func}")

    # Global warnings
    has_external = any(cfg.get("type") == "external" for cfg in POOLS.values())
    if has_external and not GIT_REPO:
        errors.append("  WARN: external pools defined but GIT_REPO_URL is not set")

    if errors:
        print()
        for err in errors:
            print(err)

    print(f"\n{passed} passed, {failed} failed")
    return failed == 0


def _sync_stage():
    """Auto-sync flow files to @PREFECT_FLOWS stage (keeps fallback fresh).

    Runs sync_flows.sh if it exists. Non-fatal — deployment succeeds even if
    the stage sync fails (e.g., no snow CLI, no Snowflake connection).
    """
    script = Path(__file__).resolve().parent.parent / "scripts" / "sync_flows.sh"
    if not script.exists():
        print("  (sync_flows.sh not found, skipping stage sync)")
        return
    print("Syncing flow files to @PREFECT_FLOWS stage (fallback)...")
    try:
        subprocess.run(["/bin/bash", str(script)], check=True)
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        print(f"  Warning: stage sync failed ({exc}), fallback may be stale")


async def _diff_pool(pool_key: str, name_filter: str | None = None) -> tuple[int, int, int]:
    """Show plan-style diff of what deploy would change for a pool.

    Compares FLOW_REGISTRY against live Prefect deployments and prints:
      +  new deployment (not yet on server)
      ~  changed deployment (version, tags, schedule, params, concurrency differ)
      =  unchanged deployment

    Returns (added, changed, unchanged) counts.
    """
    pool_cfg = POOLS[pool_key]
    pool_name = pool_cfg["pool_name"]
    suffix = pool_cfg["suffix"]
    pool_tag = pool_cfg["tag"]
    all_pool_keys = list(POOLS.keys())
    git_sha = get_git_sha()

    added = 0
    changed = 0
    unchanged = 0

    async with get_client() as client:
        # Fetch all existing deployments for comparison
        existing: dict[str, Any] = {}
        try:
            from prefect.client.schemas.filters import (
                DeploymentFilter,
                DeploymentFilterWorkPoolName,
            )

            deployments = await client.read_deployments(
                deployment_filter=DeploymentFilter(
                    work_pool_name=DeploymentFilterWorkPoolName(any_=[pool_name]),
                ),
            )
            for dep in deployments:
                existing[dep.name] = dep
        except Exception:
            # If filter API fails, fall back to empty (treat all as new)
            pass

        for spec in FLOW_REGISTRY:
            target_pools = all_pool_keys if spec.pools == "all" else spec.pools
            if pool_key not in target_pools:
                continue
            if name_filter and spec.name != name_filter:
                continue

            deploy_name = f"{spec.name}-{suffix}"
            desired_version = spec.version or git_sha
            desired_tags = sorted([pool_tag, *spec.tags])

            # Multi-environment: mirror the naming from deploy_to_pool
            if PREFECT_ENV:
                deploy_name = f"{PREFECT_ENV}/{deploy_name}"
                desired_tags = sorted([*desired_tags, f"env:{PREFECT_ENV}"])

            if deploy_name not in existing:
                added += 1
                print(f"  + {deploy_name:<32} (new)")
                print(f"      pool:    {pool_name}")
                print(f"      version: {desired_version}")
                print(f"      tags:    {desired_tags}")
                if spec.cron:
                    print(f"      cron:    {spec.cron}")
                elif spec.interval:
                    print(f"      interval: {spec.interval}s")
                if spec.triggers:
                    print(f"      triggers: {len(spec.triggers)}")
                continue

            dep = existing[deploy_name]
            diffs: list[str] = []

            # Compare version
            live_version = dep.version or ""
            if live_version != desired_version:
                diffs.append(f"      version: {live_version!r} -> {desired_version!r}")

            # Compare tags
            live_tags = sorted(dep.tags or [])
            if live_tags != desired_tags:
                diffs.append(f"      tags:    {live_tags} -> {desired_tags}")

            # Compare parameters
            live_params = dep.parameters or {}
            desired_params = spec.parameters or {}
            if live_params != desired_params:
                diffs.append(f"      params:  {live_params!r} -> {desired_params!r}")

            # Compare entrypoint
            desired_entrypoint = f"{spec.path}:{spec.func}"
            live_entrypoint = dep.entrypoint or ""
            if live_entrypoint != desired_entrypoint:
                diffs.append(f"      entry:   {live_entrypoint!r} -> {desired_entrypoint!r}")

            # Compare concurrency
            live_conc = getattr(dep, "concurrency_limit", None)
            if live_conc != spec.concurrency_limit:
                diffs.append(f"      conc:    {live_conc} -> {spec.concurrency_limit}")

            if diffs:
                changed += 1
                print(f"  ~ {deploy_name:<32} (changed)")
                for d in diffs:
                    print(d)
            else:
                unchanged += 1
                print(f"  = {deploy_name:<32} (unchanged)")

    return added, changed, unchanged


if __name__ == "__main__":
    pool_keys, name_filter, validate, diff = _parse_args()

    if validate:
        ok = _validate()
        sys.exit(0 if ok else 1)

    if diff:
        total_added = 0
        total_changed = 0
        total_unchanged = 0
        for key in pool_keys:
            pool_name = POOLS[key]["pool_name"]
            print(f"\n--- Diff for {pool_name} ({key}) ---")
            a, c, u = asyncio.run(_diff_pool(key, name_filter))
            total_added += a
            total_changed += c
            total_unchanged += u
        print(
            f"\nPlan: {total_added} to add, {total_changed} to change, {total_unchanged} unchanged."
        )
        sys.exit(0)

    deployed_spcs = False

    for key in pool_keys:
        pool_name = POOLS[key]["pool_name"]
        print(f"Deploying flows to {pool_name}...")
        asyncio.run(deploy_to_pool(key, name_filter))
        if POOLS[key].get("type") == "spcs":
            deployed_spcs = True

    # Auto-sync stage after SPCS deployments to keep fallback fresh
    if deployed_spcs:
        _sync_stage()

    print("Done!")
