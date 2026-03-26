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

# Install OTel auto-instrumentation packages for all installed libraries
RUN uv run opentelemetry-bootstrap -a install

# uv venv is the runtime environment
ENV PATH="/opt/prefect/.venv/bin:$PATH"

# Run as non-root
RUN useradd -m prefect
USER prefect
