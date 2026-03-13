"""Alert validation flow — fails by default to trigger monitoring alerts.

Used to verify end-to-end alert delivery:
  Prefect failure → prefect-exporter metrics → Prometheus → Grafana alert → Gmail

Usage:
  # Run from deployment (fails, triggers alerts):
  prefect deployment run "alert-test/alert-test-aws"

  # Run without failure (verify green path):
  prefect deployment run "alert-test/alert-test-aws" --param should_fail=false
"""

from __future__ import annotations

from hooks import on_flow_failure
from prefect import flow, task


@task
def do_work(iteration: int) -> str:
    """Simulate real work."""
    return f"completed step {iteration}"


@task
def explode() -> None:
    """Deliberately raise an error to trigger failure hooks and alerts."""
    raise RuntimeError(
        "ALERT TEST — This failure is intentional. "
        "Verifying monitoring pipeline: Prefect → Prometheus → Grafana → Email."
    )


@flow(
    name="alert-test",
    log_prints=True,
    on_failure=[on_flow_failure],
)
def alert_test_flow(should_fail: bool = True) -> str:
    """Flow that fails by default for alert validation.

    Args:
        should_fail: If True (default), the flow raises an error after doing
                     some work to test the alert pipeline. Set to False to
                     verify the flow runs green.
    """
    results = []
    for i in range(3):
        results.append(do_work(i))

    if should_fail:
        print("About to fail intentionally for alert testing...")
        explode()

    print("All steps completed successfully.")
    return f"Completed {len(results)} steps"


if __name__ == "__main__":
    alert_test_flow(should_fail=False)
