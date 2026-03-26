FROM python:3.12-slim

# git is required for Prefect's git_clone pull step (shells out to git)
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

# Install uv (pinned version for reproducible builds)
COPY --from=ghcr.io/astral-sh/uv:0.6 /uv /usr/local/bin/uv

# Install flow dependencies via uv sync
WORKDIR /opt/prefect
COPY pyproject.toml .
COPY src/ src/
RUN uv sync --no-dev

# OTel: uv sync resolves deps but doesn't always generate console-script
# entry points (opentelemetry-instrument).  Re-install via uv pip to force
# entry-point generation, then use uv pip for bootstrap (it shells out to
# pip internally, which uv venvs don't have).
RUN uv pip install --python .venv/bin/python \
        opentelemetry-distro \
        opentelemetry-exporter-otlp
RUN OTEL_PKGS=$(uv run opentelemetry-bootstrap --action=requirements 2>/dev/null) && \
    if [ -n "$OTEL_PKGS" ]; then echo "$OTEL_PKGS" | uv pip install --python .venv/bin/python -r -; fi

# uv venv is the runtime environment
ENV PATH="/opt/prefect/.venv/bin:$PATH"

# Run as non-root
RUN useradd -m prefect
USER prefect
