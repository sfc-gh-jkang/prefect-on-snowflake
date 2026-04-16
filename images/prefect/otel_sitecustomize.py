"""Custom sitecustomize.py for Prefect child flow-run processes.

Replaces the stock OTel auto-instrumentation sitecustomize.py so that child
processes use SimpleSpanProcessor instead of BatchSpanProcessor.

BatchSpanProcessor's urllib3 persistent connection pool gets "poisoned" by
TCP connection resets between SPCS containers (observe-agent sidecar).  Once
poisoned, every subsequent export fails with ConnectionResetError(104) and
never recovers.  SimpleSpanProcessor creates a fresh HTTP connection per
export, which avoids this entirely.

This file is installed into /opt/prefect/otel_sitecustomize/ and that
directory is set as PYTHONPATH by otel_entrypoint.py.  Python automatically
imports sitecustomize.py from PYTHONPATH at interpreter startup, so child
flow-run processes pick this up before any application code runs.

After configuring tracing, this module loads the OTel library instrumentors
(requests, urllib3, etc.) so that child processes get the same automatic
instrumentation they would from the stock auto-instrumentation.
"""

import os


def _configure():
    """Configure OTel with SimpleSpanProcessor and load instrumentors."""
    # Only configure if tracing is enabled via env vars
    if not os.environ.get("OTEL_TRACES_EXPORTER"):
        return

    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor

        # Build resource from standard OTel env vars
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

        # Force-disable metrics and logs export before loading instrumentors.
        # OTEL_METRICS_EXPORTER=none is set in pf_worker.yaml but some OTel
        # instrumentors still create a BatchMetricExporter because they see
        # OTEL_EXPORTER_OTLP_ENDPOINT and use it for all signals.  Setting
        # the signal-specific endpoint to empty prevents the OTLP metric
        # exporter from connecting anywhere.
        os.environ.setdefault("OTEL_METRICS_EXPORTER", "none")
        os.environ.setdefault("OTEL_LOGS_EXPORTER", "none")
        os.environ["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] = ""

        # Load library instrumentors (requests, urllib3, etc.) without going
        # through the stock configurator that would create BatchSpanProcessor.
        try:
            from opentelemetry.instrumentation.auto_instrumentation._load import (
                _load_distro,
                _load_instrumentors,
            )

            distro = _load_distro()
            _load_instrumentors(distro)
        except Exception:
            # If instrumentors fail to load, tracing still works — just without
            # automatic library instrumentation.
            pass

    except Exception:
        # Never let OTel configuration failures crash the child process.
        pass


_configure()
