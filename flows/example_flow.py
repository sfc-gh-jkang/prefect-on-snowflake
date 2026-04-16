"""Basic hello-world flow to verify Prefect worker connectivity."""

import os

from prefect import flow, task


@task
def say_hello(name: str) -> str:
    greeting = f"Hello, {name}! Running on Prefect SPCS."
    print(greeting)
    return greeting


@task
def otel_diagnostic() -> dict:
    """Print OTel diagnostic info to verify auto-instrumentation in child process."""
    from opentelemetry import trace

    provider = trace.get_tracer_provider()
    provider_type = type(provider).__name__

    info = {
        "PYTHONPATH": os.environ.get("PYTHONPATH", "NOT SET"),
        "OTEL_SERVICE_NAME": os.environ.get("OTEL_SERVICE_NAME", "NOT SET"),
        "OTEL_TRACES_EXPORTER": os.environ.get("OTEL_TRACES_EXPORTER", "NOT SET"),
        "OTEL_EXPORTER_OTLP_ENDPOINT": os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "NOT SET"),
        "OTEL_EXPORTER_OTLP_PROTOCOL": os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL", "NOT SET"),
        "TracerProvider": provider_type,
    }

    # Check if spans are being created
    tracer = trace.get_tracer("diagnostic")
    with tracer.start_as_current_span("diagnostic-span") as span:
        info["span_trace_id"] = hex(span.get_span_context().trace_id)
        info["span_valid"] = span.get_span_context().trace_id != 0

    for k, v in sorted(info.items()):
        print(f"  {k}: {v}")

    return info


@flow(name="example-flow", log_prints=True)
def example_flow(name: str = "World"):
    """Simple flow that greets and returns a message."""
    result = say_hello(name)
    otel_diagnostic()
    return result


if __name__ == "__main__":
    example_flow()
