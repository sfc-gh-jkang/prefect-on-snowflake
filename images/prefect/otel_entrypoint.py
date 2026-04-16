"""Custom OTel entrypoint for the Prefect worker.

Replaces `opentelemetry-instrument` to work around ConnectionResetError(104)
between SPCS containers.  The standard auto-instrumentation creates a
BatchSpanProcessor whose urllib3 connection pool gets "poisoned" by connection
resets from the observe-agent sidecar.  Once poisoned, every subsequent export
fails and never recovers.

This script configures OTel with SimpleSpanProcessor instead, which creates
a fresh HTTP connection per export.  This is slightly less efficient but
completely avoids the connection pool issue in SPCS.

IMPORTANT: The PYTHONPATH env var must NOT point to the OTel
auto-instrumentation directory when this process starts, or Python's startup
will import sitecustomize.py and create a BatchSpanProcessor before our code
runs.  Instead, pf_worker.yaml sets OTEL_PYTHONPATH (a custom env var) and
this script copies it to PYTHONPATH *after* configuring SimpleSpanProcessor,
so child flow-run processes inherit it and get auto-instrumented.

Usage (in pf_worker.yaml command):
    - /opt/prefect/.venv/bin/python
    - /opt/prefect/otel_entrypoint.py
    - prefect
    - worker
    - start
    - --pool
    - spcs-pool
    - --type
    - process
    - --with-healthcheck
"""

import os
import sys


def configure_otel():
    """Configure OTel SDK with SimpleSpanProcessor before Prefect starts."""
    if not os.environ.get("OTEL_TRACES_EXPORTER"):
        return

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor

    # Build resource from standard env vars
    resource_attrs = {}
    service_name = os.environ.get("OTEL_SERVICE_NAME")
    if service_name:
        resource_attrs["service.name"] = service_name

    raw_attrs = os.environ.get("OTEL_RESOURCE_ATTRIBUTES", "")
    for pair in raw_attrs.split(","):
        pair = pair.strip()
        if "=" in pair:
            k, v = pair.split("=", 1)
            resource_attrs[k] = v

    resource = Resource.create(resource_attrs)

    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
    exporter = OTLPSpanExporter(
        endpoint=f"{endpoint}/v1/traces",
        timeout=10,
    )

    provider = TracerProvider(resource=resource)
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    # Propagate PYTHONPATH for child processes so they pick up our custom
    # sitecustomize.py (which also uses SimpleSpanProcessor).  OTEL_PYTHONPATH
    # is a custom env var set in pf_worker.yaml that points to the directory
    # containing our custom sitecustomize.py.  We intentionally did NOT set
    # PYTHONPATH in the container env to prevent Python from importing any
    # sitecustomize.py during THIS process's startup.
    otel_pythonpath = os.environ.get("OTEL_PYTHONPATH")
    if otel_pythonpath:
        existing = os.environ.get("PYTHONPATH", "")
        if existing:
            os.environ["PYTHONPATH"] = f"{otel_pythonpath}:{existing}"
        else:
            os.environ["PYTHONPATH"] = otel_pythonpath

    print(f"otel_entrypoint: configured SimpleSpanProcessor -> {endpoint}")


def main():
    configure_otel()

    args = sys.argv[1:]
    if not args:
        print("Usage: otel_entrypoint.py <command> [args...]", file=sys.stderr)
        sys.exit(1)

    # Use os.execvp to replace this process with the target command.
    # Since we've already set the TracerProvider and PYTHONPATH is now in
    # os.environ, the exec'd process will:
    # 1. NOT re-import sitecustomize.py (because the parent Python already
    #    processed PYTHONPATH at startup when it was empty)
    # Wait -- execvp starts a NEW Python if args[0] is "prefect" (a Python
    # script).  The new Python WILL see PYTHONPATH and import sitecustomize.
    # But sitecustomize.initialize() checks if a TracerProvider is already
    # set... except after execvp, it's a brand new process with no state.
    #
    # So we CANNOT use execvp.  We must run prefect in-process.
    sys.argv = args

    from prefect.cli import app

    app()


if __name__ == "__main__":
    main()
